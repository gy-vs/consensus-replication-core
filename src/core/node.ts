/**
 * The Raft node state machine.
 *
 * `step(state, now, msg)` is the only mutation entry point.  It is
 * synchronous and side-effect free apart from mutating `state`; everything
 * that must hit the network or stable storage is returned in a
 * {@link StepResult}.  The host contract per step is:
 *
 *   1. apply every `result.disk` write and *flush it to stable storage*;
 *   2. only then put `result.messages` on the wire;
 *   3. hand `result.completed/read/errors` to clients.
 *
 * That ordering is the durability boundary required for the safety
 * property "a successful client write exists at the same index on every
 * future leader".
 *
 * Implemented features: leader election, log replication, the §5.3 conflict
 * term/index optimization, joint-consensus membership changes (single
 * add/remove), InstallSnapshot for catch-up, read-index linearizable reads.
 */
import type {
  AppendEntriesRequest,
  AppendEntriesResponse,
  ClientCompletion,
  Command,
  ConfigPayload,
  DiskWrite,
  InstallSnapshotRequest,
  LogEntry,
  Message,
  NodeId,
  NodeState,
  OutboundMessage,
  ProposeConfigChange,
  ReadReady,
  RpcRequest,
  RpcResponse,
  StepResult,
} from "./types.js";
import {
  configAt,
  entriesFrom,
  entryAt,
  firstIndex,
  lastIndex,
  lastTerm,
  latestConfig,
  logAtLeastAsGood,
  quorumAt,
  replicationSet,
  termAt,
} from "./log.js";

// --------------------------------------------------------------------------
// Construction
// --------------------------------------------------------------------------

/**
 * Create a fresh node in the given voter set.  The initial membership
 * configuration is recorded as a synthetic committed snapshot at index 0
 * (snapshotConfig), exactly what a node knows after loading its durable
 * state.  The host supplies the election timeout; the kernel never reads a
 * clock itself.
 */
export function initNode(
  id: NodeId,
  initialVoters: ReadonlySet<NodeId>,
  now: number,
): NodeState {
  return {
    id,
    p: {
      currentTerm: 0,
      votedFor: null,
      log: [],
      snapshotIndex: 0,
      snapshotTerm: 0,
      snapshotConfig: new Set(initialVoters),
    },
    v: {
      role: "follower",
      leaderId: null,
      commitIndex: 0,
      lastApplied: 0,
      progress: new Map(),
      pendingWrites: new Map(),
      votes: new Set(),
      pendingReads: new Map(),
      configChangeInFlight: false,
      pendingConfigChange: null,
    },
    lastHeartbeat: now,
  };
}

/**
 * Reconstruct a node from durable state after a crash.  Everything volatile
 * is discarded; the node comes back as a follower with commit/apply
 * positions at the snapshot boundary (the host re-applies its FSM snapshot).
 */
export function restoreNode(
  id: NodeId,
  p: NodeState["p"],
  now: number,
): NodeState {
  return {
    id,
    p,
    v: {
      role: "follower",
      leaderId: null,
      commitIndex: p.snapshotIndex,
      lastApplied: p.snapshotIndex,
      progress: new Map(),
      pendingWrites: new Map(),
      votes: new Set(),
      pendingReads: new Map(),
      configChangeInFlight: false,
      pendingConfigChange: null,
    },
    lastHeartbeat: now,
  };
}

// --------------------------------------------------------------------------
// Step plumbing
// --------------------------------------------------------------------------

interface Acc {
  disk: DiskWrite[];
  messages: OutboundMessage[];
  completed: ClientCompletion[];
  reads: ReadReady[];
  errors: StepResult["errors"];
  resetElectionTimer: boolean;
  configCompleted?: ProposeConfigChange;
}

function acc(): Acc {
  return {
    disk: [],
    messages: [],
    completed: [],
    reads: [],
    errors: [],
    resetElectionTimer: false,
  };
}

function toResult(a: Acc): StepResult {
  return {
    disk: a.disk,
    messages: a.messages,
    completed: a.completed,
    reads: a.reads,
    errors: a.errors,
    resetElectionTimer: a.resetElectionTimer,
    configCompleted: a.configCompleted,
  };
}

function send(s: NodeState, a: Acc, to: NodeId, message: RpcRequest | RpcResponse): void {
  a.messages.push({ to, message });
}

function sendToEach(
  s: NodeState,
  a: Acc,
  peers: Iterable<NodeId>,
  make: (to: NodeId) => RpcRequest,
): void {
  for (const id of [...peers].sort((x, y) => x - y)) {
    if (id !== s.id) send(s, a, id, make(id));
  }
}

/**
 * Persist a term/vote change.  NOTE the ordering: callers append this disk
 * write BEFORE appending the messages whose validity depends on the new term
 * (RequestVote broadcasts, vote grants, AppendEntries after a step-up).
 */
function persistVote(s: NodeState, a: Acc, term: number, votedFor: NodeId | null): void {
  s.p.currentTerm = term;
  s.p.votedFor = votedFor;
  a.disk.push({ kind: "vote", term, votedFor });
}

/** Step down (or merely adopt a newer term).  Drops all volatile leader work. */
function adoptTerm(s: NodeState, a: Acc, term: number, votedFor: NodeId | null): void {
  persistVote(s, a, term, votedFor);
  becomeFollower(s);
}

function becomeFollower(s: NodeState): void {
  s.v.role = "follower";
  s.v.progress = new Map();
  s.v.pendingWrites = new Map();
  s.v.votes = new Set();
  s.v.pendingReads = new Map();
  s.v.configChangeInFlight = false;
  s.v.pendingConfigChange = null;
}

function becomeCandidate(s: NodeState, a: Acc, now: number): void {
  const term = s.p.currentTerm + 1;
  // DURABILITY: term + self-vote on disk before any RequestVote leaves.
  persistVote(s, a, term, s.id);
  s.v.role = "candidate";
  s.v.leaderId = null;
  s.v.votes = new Set([s.id]);
  s.v.pendingReads = new Map();
  s.v.pendingWrites = new Map();
  s.lastHeartbeat = now;
  a.resetElectionTimer = true;
}

function becomeLeader(s: NodeState, a: Acc, now: number): void {
  s.v.role = "leader";
  s.v.leaderId = s.id;
  s.v.votes = new Set();
  s.v.pendingReads = new Map();
  s.v.pendingWrites = new Map();
  s.lastHeartbeat = now;

  // Initialise progress from the union of the configuration the new leader
  // appends by (§6: leader uses latest config; union when joint).
  const cfg = latestConfig(s.p);
  s.v.progress = new Map();
  const li = lastIndex(s.p);
  for (const id of replicationSet(cfg)) {
    if (id !== s.id) s.v.progress.set(id, { nextIndex: li + 1, matchIndex: 0 });
  }
  // Defensive no-op: the new leader commits entries from its own term
  // promptly.  No disk write: the no-op entry goes through the normal
  // durable append path below.
  appendEntry(s, a, {
    term: s.p.currentTerm,
    command: "",
    config: null,
  });
  a.resetElectionTimer = true;
}

/** Append a freshly proposed entry.  Durable BEFORE it is replicated. */
function appendEntry(s: NodeState, a: Acc, entry: LogEntry): number {
  s.p.log.push(entry);
  a.disk.push({ kind: "append", entries: [entry] });
  return lastIndex(s.p);
}

// --------------------------------------------------------------------------
// Leader: replication
// --------------------------------------------------------------------------

function ensureProgress(s: NodeState): void {
  const cfg = latestConfig(s.p);
  for (const id of replicationSet(cfg)) {
    if (id !== s.id && !s.v.progress.has(id)) {
      s.v.progress.set(id, { nextIndex: lastIndex(s.p) + 1, matchIndex: 0 });
    }
  }
}

/** Build the AppendEntries (or InstallSnapshot) for one peer. */
function replicationMessage(
  s: NodeState,
  to: NodeId,
): RpcRequest {
  const pr = s.v.progress.get(to)!;
  const li = lastIndex(s.p);

  if (pr.nextIndex <= s.p.snapshotIndex) {
    // Needed prefix is compacted away: ship the snapshot.  The host carries
    // the FSM payload; Raft itself only needs index/term/config.
    const msg: InstallSnapshotRequest = {
      type: "InstallSnapshot",
      term: s.p.currentTerm,
      leaderId: s.id,
      lastIncludedIndex: s.p.snapshotIndex,
      lastIncludedTerm: s.p.snapshotTerm,
      config: s.p.snapshotConfig ?? new Set<NodeId>(),
    };
    return msg;
  }

  const prevLogIndex = pr.nextIndex - 1;
  const prevLogTerm = termAt(s.p, prevLogIndex);
  const req: AppendEntriesRequest = {
    type: "AppendEntries",
    term: s.p.currentTerm,
    leaderId: s.id,
    prevLogIndex,
    prevLogTerm: prevLogTerm ?? 0,
    entries: entriesFrom(s.p, pr.nextIndex),
    leaderCommit: s.v.commitIndex,
  };
  void li;
  return req;
}

function broadcastHeartbeats(s: NodeState, a: Acc): void {
  ensureProgress(s);
  sendToEach(s, a, s.v.progress.keys(), (to) => replicationMessage(s, to));
}

/**
 * Recalculate commit index under the configuration carried by each candidate
 * index (§6: quorum changes at the entry whose config applies at N).
 */
function advanceCommit(s: NodeState): number[] {
  if (s.v.role !== "leader") return [];
  const newlyCommitted: number[] = [];
  const li = lastIndex(s.p);
  const ack = (peer: NodeId, idx: number): boolean => {
    if (peer === s.id) return idx <= li;
    return (s.v.progress.get(peer)?.matchIndex ?? 0) >= idx;
  };
  for (let n = li; n > s.v.commitIndex; n--) {
    // Raft §5.4.2: a leader only commits entries from *its own term*
    // directly; older-term entries ride along once a newer one commits.
    if (termAt(s.p, n) !== s.p.currentTerm) continue;
    const cfg = configAt(s.p, n);
    if (!cfg) continue;
    if (quorumAt(cfg, n, s.id, ack)) {
      for (let i = s.v.commitIndex + 1; i <= n; i++) newlyCommitted.push(i);
      s.v.commitIndex = n;
      break;
    }
  }
  return newlyCommitted;
}

/** Run after commitIndex may have grown: completions, joint→plain, reads. */
function postCommit(s: NodeState, a: Acc): void {
  // 1. Complete client writes whose entry has just committed.  Durability of
  //    completion follows from the entries having been synced by the quorum
  //    that committed them; the host may now answer the client.
  for (const idx of [...s.v.pendingWrites.keys()].sort((x, y) => x - y)) {
    if (idx > s.v.commitIndex) break;
    const w = s.v.pendingWrites.get(idx)!;
    const e = entryAt(s.p, idx);
    if (e) {
      a.completed.push({
        kind: "write",
        requestId: w.requestId,
        command: w.command,
        index: idx,
        term: e.term,
      });
    }
    s.v.pendingWrites.delete(idx);
  }

  // 2. Joint consensus committed?  Append the plain (Cold,new) entry now.
  //    This is derived purely from the log, so a *different* leader than
  //    the one that started the change still finishes it (Raft §6).
  const lastCfgIndex = findLastConfigIndex(s.p);
  if (lastCfgIndex !== null) {
    const lastCfg = entryAt(s.p, lastCfgIndex)!.config!;
    if (lastCfg.kind === "joint" && lastCfgIndex <= s.v.commitIndex) {
      const voters = new Set<NodeId>([...lastCfg.oldVoters, ...lastCfg.newVoters]);
      appendEntry(s, a, {
        term: s.p.currentTerm,
        command: "",
        config: { kind: "plain", voters },
      });
      // Replicate the plain entry immediately (union progress set).
      ensureProgress(s);
      broadcastHeartbeats(s, a);
    }
  }

  // 3. Plain configuration entry committed: the change is fully done.
  //    The completion event is only delivered to the leader that fielded
  //    the operator request; other observers derive membership from logs.
  if (s.v.pendingConfigChange) {
    if (lastCfgIndex !== null && lastCfgIndex <= s.v.commitIndex) {
      const cfg = entryAt(s.p, lastCfgIndex)?.config;
      if (cfg?.kind === "plain") {
        a.configCompleted = s.v.pendingConfigChange;
        s.v.configChangeInFlight = false;
        s.v.pendingConfigChange = null;
        // If this leader just removed ITSELF from the cluster it must
        // step down once the new plain configuration has committed.
        if (s.v.role === "leader" && !cfg.voters.has(s.id)) {
          becomeFollower(s);
          a.resetElectionTimer = true;
          return;
        }
        // Stop replicating to peers that are no longer members.
        ensureProgress(s);
        for (const id of [...s.v.progress.keys()]) {
          if (!cfg.voters.has(id)) s.v.progress.delete(id);
        }
      }
    }
  }

  // 4. Read-index requests whose quorum/apply conditions now hold.
  releaseReads(s, a);
}

/**
 * Release any read-index request that has (a) collected a FRESH quorum of
 * heartbeat acks for THIS request and (b) been applied up to its read index.
 * The freshness round is per request, so a leader that has been partitioned
 * away from a quorum cannot answer a new read with stale state.
 */
function releaseReads(s: NodeState, a: Acc): void {
  if (s.v.role !== "leader" || s.v.pendingReads.size === 0) return;
  const cfg = latestConfig(s.p);
  const li = lastIndex(s.p);
  for (const [reqId, pr] of s.v.pendingReads) {
    const acked = (peer: NodeId): boolean =>
      peer === s.id || pr.acks.has(peer);
    const quorum = quorumAt(cfg, li, s.id, (id) => acked(id));
    if (quorum && s.v.lastApplied >= pr.readIndex) {
      a.reads.push({
        kind: "read",
        requestId: reqId,
        query: pr.query,
        index: pr.readIndex,
      });
      s.v.pendingReads.delete(reqId);
    }
  }
}

function findLastConfigIndex(p: NodeState["p"]): number | null {
  for (let i = lastIndex(p); i >= p.snapshotIndex + 1; i--) {
    if (entryAt(p, i)?.config) return i;
  }
  return null;
}

/**
 * After the leader appends new entries or gains new match info, drive the
 * whole commit pipeline once, keeping step processing uniform.
 */
function leaderCatchUp(s: NodeState, a: Acc): void {
  if (advanceCommit(s).length > 0) {
    postCommit(s, a);
    // The auto plain-config append needs replication before it can commit;
    // one more pass covers the case where it was already replicated.
    if (advanceCommit(s).length > 0) postCommit(s, a);
  }
}

// --------------------------------------------------------------------------
// Leader-side RPC handlers
// --------------------------------------------------------------------------

function handleAppendEntriesResult(
  s: NodeState,
  a: Acc,
  msg: AppendEntriesResponse,
): void {
  if (s.v.role !== "leader") return;
  if (msg.term > s.p.currentTerm) {
    adoptTerm(s, a, msg.term, null);
    a.resetElectionTimer = true;
    return;
  }
  if (msg.term < s.p.currentTerm) return;
  const pr = s.v.progress.get(msg.followerId);
  if (!pr) return;

  if (msg.success) {
    const idx = msg.successIndex ?? pr.matchIndex;
    // Never move progress backwards (duplicate/delayed responses).
    if (idx > pr.matchIndex) pr.matchIndex = idx;
    if (pr.nextIndex <= idx) pr.nextIndex = idx + 1;
    // This success is a fresh contact for every read barrier in flight.
    recordReadAck(s, msg.followerId);
    leaderCatchUp(s, a);
    releaseReads(s, a);
    return;
  }

  // Rejection: back off using the conflict-term optimization (§5.3).
  let next: number;
  if (msg.conflictTerm !== undefined && msg.conflictIndex !== undefined) {
    let lastOfTerm = -1;
    for (let i = lastIndex(s.p); i >= firstIndex(s.p); i--) {
      if (entryAt(s.p, i)?.term === msg.conflictTerm) {
        lastOfTerm = i;
        break;
      }
    }
    if (lastOfTerm >= 0) next = lastOfTerm + 1;
    else next = msg.conflictIndex;
  } else {
    next = pr.nextIndex - 1;
  }
  if (next < 1) next = 1;
  pr.nextIndex = Math.min(pr.nextIndex, next);
  // Retry immediately; if the required prefix is compacted this becomes an
  // InstallSnapshot, letting the follower catch up while service continues.
  send(s, a, msg.followerId, replicationMessage(s, msg.followerId));
}

function handleInstallSnapshotResult(
  s: NodeState,
  a: Acc,
  msg: RpcResponse & { type: "InstallSnapshotResult" },
): void {
  if (s.v.role !== "leader") return;
  if (msg.term > s.p.currentTerm) {
    adoptTerm(s, a, msg.term, null);
    a.resetElectionTimer = true;
    return;
  }
  const pr = s.v.progress.get(msg.followerId);
  if (!pr) return;
  pr.matchIndex = Math.max(pr.matchIndex, msg.lastIncludedIndex);
  pr.nextIndex = Math.max(pr.nextIndex, msg.lastIncludedIndex + 1);
  recordReadAck(s, msg.followerId);
  // Continue streaming log entries after the snapshot boundary.
  send(s, a, msg.followerId, replicationMessage(s, msg.followerId));
  leaderCatchUp(s, a);
  releaseReads(s, a);
}

/** Add a successful-contact ack to every pending read barrier. */
function recordReadAck(s: NodeState, followerId: NodeId): void {
  for (const pr of s.v.pendingReads.values()) pr.acks.add(followerId);
}

function handleRequestVoteResult(
  s: NodeState,
  a: Acc,
  msg: Extract<RpcResponse, { type: "RequestVoteResult" }>,
): void {
  if (s.v.role !== "candidate") return;
  if (msg.term > s.p.currentTerm) {
    adoptTerm(s, a, msg.term, null);
    a.resetElectionTimer = true;
    return;
  }
  if (msg.term < s.p.currentTerm || !msg.voteGranted) return;
  s.v.votes.add(msg.voterId);

  // Election quorum is computed against the configuration at the end of the
  // candidate's log (§6): both majorities while joint.
  const cfg = latestConfig(s.p);
  const has = (id: NodeId) => s.v.votes.has(id);
  if (quorumAt(cfg, lastIndex(s.p), s.id, (id) => (id === s.id ? true : has(id)))) {
    becomeLeader(s, a, 0);
    broadcastHeartbeats(s, a);
  }
}

// --------------------------------------------------------------------------
// Follower-side RPC handlers
// --------------------------------------------------------------------------

function handleRequestVote(
  s: NodeState,
  a: Acc,
  msg: Extract<RpcRequest, { type: "RequestVote" }>,
): void {
  if (msg.term < s.p.currentTerm) {
    send(s, a, msg.candidateId, {
      type: "RequestVoteResult",
      term: s.p.currentTerm,
      voterId: s.id,
      voteGranted: false,
    });
    return;
  }

  if (msg.term > s.p.currentTerm) {
    // DURABILITY: persist the new term (vote still null) BEFORE granting.
    adoptTerm(s, a, msg.term, null);
  }

  // Membership gate: only a current voter may receive a vote.  This is what
  // keeps a booted-but-not-yet-added spare, or a removed node, from winning
  // an election (Raft §6 non-voter / configuration rules).
  const voterSet = replicationSet(latestConfig(s.p));
  if (!voterSet.has(msg.candidateId)) {
    send(s, a, msg.candidateId, {
      type: "RequestVoteResult",
      term: s.p.currentTerm,
      voterId: s.id,
      voteGranted: false,
    });
    return;
  }

  const upToDate = logAtLeastAsGood(
    msg.lastLogTerm,
    msg.lastLogIndex,
    lastTerm(s.p),
    lastIndex(s.p),
  );
  const canVote =
    (s.p.votedFor === null || s.p.votedFor === msg.candidateId) && upToDate;

  if (canVote) {
    // DURABILITY: the vote is synced before the grant is sent, so a crash
    // cannot cause two votes in one term.
    persistVote(s, a, msg.term, msg.candidateId);
    s.v.leaderId = null;
    a.resetElectionTimer = true;
  }
  send(s, a, msg.candidateId, {
    type: "RequestVoteResult",
    term: s.p.currentTerm,
    voterId: s.id,
    voteGranted: canVote,
  });
}

function handleAppendEntries(
  s: NodeState,
  a: Acc,
  now: number,
  msg: AppendEntriesRequest,
): void {
  if (msg.term < s.p.currentTerm) {
    send(s, a, msg.leaderId, {
      type: "AppendEntriesResult",
      term: s.p.currentTerm,
      followerId: s.id,
      success: false,
    });
    return;
  }

  if (msg.term > s.p.currentTerm) {
    // Persist new term before processing under it / replying in it.
    adoptTerm(s, a, msg.term, null);
  } else if (s.v.role === "candidate") {
    becomeFollower(s);
  }
  s.v.role = "follower";
  s.v.leaderId = msg.leaderId;
  a.resetElectionTimer = true;
  s.lastHeartbeat = now;

  // Entry immediately before the shipped gap is compacted: leader is behind
  // our snapshot (cannot happen for committed entries, but answer honestly).
  if (msg.prevLogIndex < s.p.snapshotIndex) {
    send(s, a, msg.leaderId, {
      type: "AppendEntriesResult",
      term: s.p.currentTerm,
      followerId: s.id,
      success: false,
      conflictIndex: s.p.snapshotIndex,
      conflictTerm: s.p.snapshotTerm,
    });
    return;
  }

  if (msg.prevLogIndex > lastIndex(s.p)) {
    // Gap: hint the first index we're missing (conflict term unknown).
    send(s, a, msg.leaderId, {
      type: "AppendEntriesResult",
      term: s.p.currentTerm,
      followerId: s.id,
      success: false,
      conflictIndex: lastIndex(s.p) + 1,
    });
    return;
  }

  if (msg.prevLogIndex >= firstIndex(s.p)) {
    const have = termAt(s.p, msg.prevLogIndex);
    if (have !== msg.prevLogTerm) {
      // Term conflict at prevLogIndex: report that term and its first index.
      const ct = have ?? 0;
      let ci = msg.prevLogIndex;
      while (ci - 1 >= firstIndex(s.p) && termAt(s.p, ci - 1) === ct) ci--;
      send(s, a, msg.leaderId, {
        type: "AppendEntriesResult",
        term: s.p.currentTerm,
        followerId: s.id,
        success: false,
        conflictTerm: ct,
        conflictIndex: ci,
      });
      return;
    }
  }

  // Merge the shipped suffix: find the first position that actually differs.
  let writeAt = -1;
  let payload: LogEntry[] = [];
  for (let k = 0; k < msg.entries.length; k++) {
    const globalIndex = msg.prevLogIndex + 1 + k;
    const incoming = msg.entries[k]!;
    if (globalIndex <= s.p.snapshotIndex) continue; // covered by snapshot
    const existing = entryAt(s.p, globalIndex);
    if (!existing) {
      writeAt = globalIndex;
      payload = msg.entries.slice(k);
      break;
    }
    if (existing.term !== incoming.term || existing.command !== incoming.command) {
      writeAt = globalIndex;
      payload = msg.entries.slice(k);
      break;
    }
    // identical entry: keep it
  }

  if (writeAt >= 0) {
    const localOffset = writeAt - s.p.snapshotIndex - 1;
    const discarded = s.p.log.length - localOffset;
    // DURABILITY: the replacement suffix is synced before success is acked;
    // a crash before this point leaves the old log and the leader retries.
    if (discarded > 0) {
      s.p.log.length = localOffset;
      a.disk.push({ kind: "truncate", fromIndex: writeAt, entries: payload });
    } else {
      a.disk.push({ kind: "append", entries: payload });
    }
    for (const e of payload) s.p.log.push(e);
  }

  if (msg.leaderCommit > s.v.commitIndex) {
    s.v.commitIndex = Math.min(msg.leaderCommit, lastIndex(s.p));
  }

  send(s, a, msg.leaderId, {
    type: "AppendEntriesResult",
    term: s.p.currentTerm,
    followerId: s.id,
    success: true,
    successIndex: msg.prevLogIndex + msg.entries.length,
  });
}

function handleInstallSnapshot(
  s: NodeState,
  a: Acc,
  now: number,
  msg: InstallSnapshotRequest,
): void {
  if (msg.term < s.p.currentTerm) {
    send(s, a, msg.leaderId, {
      type: "InstallSnapshotResult",
      term: s.p.currentTerm,
      followerId: s.id,
      lastIncludedIndex: s.p.snapshotIndex,
    });
    return;
  }
  if (msg.term > s.p.currentTerm) adoptTerm(s, a, msg.term, null);
  else if (s.v.role === "candidate") becomeFollower(s);
  s.v.role = "follower";
  s.v.leaderId = msg.leaderId;
  a.resetElectionTimer = true;
  s.lastHeartbeat = now;

  if (msg.lastIncludedIndex <= s.p.snapshotIndex) {
    send(s, a, msg.leaderId, {
      type: "InstallSnapshotResult",
      term: s.p.currentTerm,
      followerId: s.id,
      lastIncludedIndex: s.p.snapshotIndex,
    });
    return;
  }

  // DURABILITY: the snapshot is synced BEFORE the ack; entries strictly
  // after the boundary are discarded and re-replicated by the leader, which
  // is the simplest correct behaviour and keeps snapshot/log invariants.
  a.disk.push({
    kind: "snapshot",
    index: msg.lastIncludedIndex,
    term: msg.lastIncludedTerm,
    config: msg.config,
  });
  s.p.snapshotIndex = msg.lastIncludedIndex;
  s.p.snapshotTerm = msg.lastIncludedTerm;
  s.p.snapshotConfig = msg.config;
  s.p.log = [];
  s.v.commitIndex = Math.max(s.v.commitIndex, msg.lastIncludedIndex);

  send(s, a, msg.leaderId, {
    type: "InstallSnapshotResult",
    term: s.p.currentTerm,
    followerId: s.id,
    lastIncludedIndex: msg.lastIncludedIndex,
  });
}

// --------------------------------------------------------------------------
// Local events and client operations
// --------------------------------------------------------------------------

function handleElectionTimeout(s: NodeState, a: Acc, now: number): void {
  if (s.v.role === "leader") return;
  // A node that is no longer a voter never starts an election.
  const voters = replicationSet(latestConfig(s.p));
  if (!voters.has(s.id)) return;
  becomeCandidate(s, a, now);
  // Broadcast RequestVote under the freshly persisted term.
  const cfg = latestConfig(s.p);
  sendToEach(s, a, replicationSet(cfg), (to) => ({
    type: "RequestVote",
    term: s.p.currentTerm,
    candidateId: s.id,
    lastLogIndex: lastIndex(s.p),
    lastLogTerm: lastTerm(s.p),
  }));
}

function handleHeartbeat(s: NodeState, a: Acc): void {
  if (s.v.role !== "leader") return;
  s.lastHeartbeat = 0;
  broadcastHeartbeats(s, a);
}

function handleCompact(s: NodeState, a: Acc, msg: Extract<Message, { type: "Compact" }>): void {
  const idx = msg.index;
  if (idx <= s.p.snapshotIndex || idx > s.v.commitIndex) {
    a.errors.push({ kind: "error", requestId: null, reason: "compact-rejected" });
    return;
  }
  const cfg = configAt(s.p, idx);
  if (!cfg || cfg.kind !== "plain") {
    // We only snapshot on plain-config boundaries so a snapshot carries a
    // single voter set and cannot straddle a membership change.
    a.errors.push({ kind: "error", requestId: null, reason: "compact-rejected" });
    return;
  }
  const term = termAt(s.p, idx);
  if (term === null) {
    a.errors.push({ kind: "error", requestId: null, reason: "compact-rejected" });
    return;
  }
  const suffix = entriesFrom(s.p, idx + 1);
  // DURABILITY: snapshot synced before the trimmed log prefix is discarded.
  a.disk.push({ kind: "snapshot", index: idx, term, config: cfg.voters });
  s.p.snapshotIndex = idx;
  s.p.snapshotTerm = term;
  s.p.snapshotConfig = cfg.voters;
  s.p.log = suffix;
  if (s.v.lastApplied < idx) s.v.lastApplied = idx;
}

function handleApplied(s: NodeState, a: Acc, msg: Extract<Message, { type: "Applied" }>): void {
  s.v.lastApplied = Math.max(s.v.lastApplied, msg.index);
  if (s.v.role === "leader") releaseReads(s, a);
}

function handleClientWrite(
  s: NodeState,
  a: Acc,
  msg: Extract<Message, { type: "ClientWrite" }>,
): void {
  if (s.v.role !== "leader") {
    a.errors.push({
      kind: "error",
      requestId: msg.requestId,
      reason: "not-leader",
      leaderHint: s.v.leaderId ?? undefined,
    });
    return;
  }
  const entry: LogEntry = { term: s.p.currentTerm, command: msg.command, config: null };
  const index = appendEntry(s, a, entry); // durable BEFORE replication
  s.v.pendingWrites.set(index, { requestId: msg.requestId, command: msg.command });
  // Replicate immediately; the snapshot catch-up path is selected per peer.
  ensureProgress(s);
  sendToEach(s, a, s.v.progress.keys(), (to) => replicationMessage(s, to));
}

function handleClientRead(
  s: NodeState,
  a: Acc,
  msg: Extract<Message, { type: "ClientRead" }>,
): void {
  if (s.v.role !== "leader") {
    a.errors.push({
      kind: "error",
      requestId: msg.requestId,
      reason: "not-leader",
      leaderHint: s.v.leaderId ?? undefined,
    });
    return;
  }
  // Read-index (Raft §6.4), a FRESH round per request:
  //   1. remember the leader's current commit index;
  //   2. ask a quorum to confirm we are still leader (heartbeats below);
  //   3. once applied through the read index, return the FSM value.
  // The leader itself counts as one confirming contact.
  s.v.pendingReads.set(msg.requestId, {
    query: msg.query,
    readIndex: s.v.commitIndex,
    acks: new Set<NodeId>(),
  });
  ensureProgress(s);
  broadcastHeartbeats(s, a);
  releaseReads(s, a);
}

function handleProposeConfigChange(
  s: NodeState,
  a: Acc,
  msg: ProposeConfigChange,
): void {
  if (s.v.role !== "leader") {
    a.errors.push({ kind: "error", requestId: null, reason: "not-leader" });
    return;
  }
  const cfg = latestConfig(s.p);
  if (cfg.kind !== "plain") {
    a.errors.push({ kind: "error", requestId: null, reason: "busy" });
    return;
  }
  // One change at a time: refuse while a configuration entry (joint or the
  // plain that settles it) is still uncommitted at the tail of the log.
  const ci = findLastConfigIndex(s.p);
  if (ci !== null && ci > s.v.commitIndex) {
    a.errors.push({ kind: "error", requestId: null, reason: "busy" });
    return;
  }
  const old = cfg.voters;
  if (msg.add && old.has(msg.nodeId)) {
    a.errors.push({ kind: "error", requestId: null, reason: "busy" });
    return;
  }
  if (!msg.add && !old.has(msg.nodeId)) {
    a.errors.push({ kind: "error", requestId: null, reason: "busy" });
    return;
  }
  // Never allow removing a node from a single-node cluster (would lose the
  // ability to form a majority); the operator handles drain sequencing.
  if (!msg.add && old.size <= 1) {
    a.errors.push({ kind: "error", requestId: null, reason: "busy" });
    return;
  }

  const next = new Set<NodeId>(old);
  if (msg.add) next.add(msg.nodeId);
  else next.delete(msg.nodeId);

  const joint: ConfigPayload = {
    kind: "joint",
    oldVoters: new Set(old),
    newVoters: next,
  };
  // DURABILITY via normal append; joint entry replicated to the UNION set,
  // and every decision until it commits needs BOTH majorities, so two
  // disjoint quorums can never coexist during the change.
  appendEntry(s, a, { term: s.p.currentTerm, command: "", config: joint });
  s.v.configChangeInFlight = true;
  s.v.pendingConfigChange = { ...msg };
  ensureProgress(s);
  broadcastHeartbeats(s, a);
}

// --------------------------------------------------------------------------
// Entry point
// --------------------------------------------------------------------------

/**
 * Advance the state machine by one input.
 *
 * @param now logical time of this input (used only for timer bookkeeping)
 * @returns effects the host must perform, in durability order
 */
export function step(s: NodeState, now: number, msg: Message): StepResult {
  const a = acc();
  switch (msg.type) {
    case "ElectionTimeout":
      handleElectionTimeout(s, a, now);
      break;
    case "Heartbeat":
      handleHeartbeat(s, a);
      break;
    case "Compact":
      handleCompact(s, a, msg);
      break;
    case "Applied":
      handleApplied(s, a, msg);
      break;
    case "ClientWrite":
      handleClientWrite(s, a, msg);
      break;
    case "ClientRead":
      handleClientRead(s, a, msg);
      break;
    case "ProposeConfigChange":
      handleProposeConfigChange(s, a, msg);
      break;
    case "RequestVote":
      handleRequestVote(s, a, msg);
      break;
    case "AppendEntries":
      handleAppendEntries(s, a, now, msg);
      break;
    case "InstallSnapshot":
      handleInstallSnapshot(s, a, now, msg);
      break;
    case "RequestVoteResult":
      handleRequestVoteResult(s, a, msg);
      break;
    case "AppendEntriesResult":
      handleAppendEntriesResult(s, a, msg);
      break;
    case "InstallSnapshotResult":
      handleInstallSnapshotResult(s, a, msg);
      break;
  }
  return toResult(a);
}
