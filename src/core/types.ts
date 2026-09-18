/**
 * Core type definitions for the Raft replication kernel.
 *
 * The kernel is a *pure* state machine: {@link NodeState} is mutated only
 * through {@link step} (in `node.ts`), and every mutation of the persistent
 * part ({@link PersistentState}) is paired with a {@link DiskWrite} that
 * describes exactly what must reach stable storage before the messages
 * produced by the same step may leave the node.  No timers, sockets or files
 * appear in this package.
 */

/** Identifier of a cluster member. */
export type NodeId = number;

/** Opaque client payload.  The kernel never inspects it. */
export type Command = string;

/**
 * Membership configuration carried by a log entry (or snapshot).
 * A joint configuration requires majorities of BOTH voter sets.
 */
export type ConfigPayload =
  | { kind: "plain"; voters: ReadonlySet<NodeId> }
  | {
      kind: "joint";
      oldVoters: ReadonlySet<NodeId>;
      newVoters: ReadonlySet<NodeId>;
    };

/** A single entry in the replicated log. */
export interface LogEntry {
  /** Term in which the entry was accepted by its leader. */
  term: number;
  /** Client payload; empty for configuration-only entries. */
  command: Command;
  /** Membership config installed when this entry commits, else null. */
  config: ConfigPayload | null;
}

/**
 * State that MUST survive a crash.  Every field here is written to stable
 * storage *before* any message that depends on it is sent (see
 * {@link DiskWrite} and `node.ts`: the durability boundary is the order in
 * which each step appends to `result.disk` before `result.messages`).
 */
export interface PersistentState {
  currentTerm: number;
  votedFor: NodeId | null;
  /**
   * Log suffix still held after snapshot compaction.  Global index of
   * `log[0]` is snapshotIndex + 1.
   */
  log: LogEntry[];
  /** Highest index covered by the snapshot (0 if none). */
  snapshotIndex: number;
  /** Term of the last entry covered by the snapshot. */
  snapshotTerm: number;
  /** Plain configuration in effect at snapshotIndex. */
  snapshotConfig: ReadonlySet<NodeId> | null;
}

export interface PendingRead {
  query: string;
  /**
   * Read index recorded when the request arrived (leader's commit index).
   * The read may return once a fresh quorum has acknowledged the leader
   * THIS term and the FSM has applied through this index.
   */
  readIndex: number;
  /** Peers that have acked a heartbeat after this read started. */
  acks: Set<NodeId>;
}

export type Role = "follower" | "candidate" | "leader";

export interface VolatileState {
  role: Role;
  /** Best-known current leader (a hint for clients), null if unknown. */
  leaderId: NodeId | null;
  commitIndex: number;
  /** Highest index handed to the host state machine. */
  lastApplied: number;
  /** Leader-only replication progress per peer. */
  progress: Map<NodeId, { nextIndex: number; matchIndex: number }>;
  /** Leader-only client writes awaiting commit, keyed by log index. */
  pendingWrites: Map<number, { requestId: number; command: Command }>;
  /** Candidate-only votes gathered in the current election. */
  votes: Set<NodeId>;
  /** Read-index requests awaiting a fresh quorum ack / apply index. */
  pendingReads: Map<number, PendingRead>;
  /** True from appending a joint entry until the plain entry commits. */
  configChangeInFlight: boolean;
  /** The change being driven, remembered so completion can be reported. */
  pendingConfigChange: ProposeConfigChange | null;
}

export interface NodeState {
  id: NodeId;
  p: PersistentState;
  v: VolatileState;
  /** Logical time of the last election-timer reset (observability). */
  lastHeartbeat: number;
}

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

export interface AppendEntriesRequest {
  type: "AppendEntries";
  term: number;
  leaderId: NodeId;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesResponse {
  type: "AppendEntriesResult";
  term: number;
  followerId: NodeId;
  success: boolean;
  /** On success: follower's last log index after processing. */
  successIndex?: number;
  /** Conflict hints (Raft §5.3 optimization), on failure. */
  conflictTerm?: number;
  conflictIndex?: number;
}

export interface RequestVoteRequest {
  type: "RequestVote";
  term: number;
  candidateId: NodeId;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteResponse {
  type: "RequestVoteResult";
  term: number;
  voterId: NodeId;
  voteGranted: boolean;
}

export interface InstallSnapshotRequest {
  type: "InstallSnapshot";
  term: number;
  leaderId: NodeId;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  /** Plain membership configuration at lastIncludedIndex. */
  config: ReadonlySet<NodeId>;
}

export interface InstallSnapshotResponse {
  type: "InstallSnapshotResult";
  term: number;
  followerId: NodeId;
  lastIncludedIndex: number;
}

export type RpcRequest =
  | AppendEntriesRequest
  | RequestVoteRequest
  | InstallSnapshotRequest;
export type RpcResponse =
  | AppendEntriesResponse
  | RequestVoteResponse
  | InstallSnapshotResponse;

/** Operator request: add or remove exactly one member. */
export interface ProposeConfigChange {
  type: "ProposeConfigChange";
  nodeId: NodeId;
  add: boolean;
}

export interface ClientWrite {
  type: "ClientWrite";
  requestId: number;
  command: Command;
}

export interface ClientRead {
  type: "ClientRead";
  requestId: number;
  /** Opaque key, echoed back in {@link ReadReady}. */
  query: string;
}

/** Local, non-network inputs to the state machine. */
export interface ElectionTimeout {
  type: "ElectionTimeout";
}
export interface Heartbeat {
  type: "Heartbeat";
}
export interface Compact {
  type: "Compact";
  /** Snapshot through this index; must be committed and plain-config. */
  index: number;
}
/** Host notification: its FSM (and any snapshot restore) applied through index. */
export interface Applied {
  type: "Applied";
  index: number;
}

export type Message =
  | RpcRequest
  | RpcResponse
  | ProposeConfigChange
  | ClientWrite
  | ClientRead
  | ElectionTimeout
  | Heartbeat
  | Compact
  | Applied;

// ---------------------------------------------------------------------------
// Effects produced by a step
// ---------------------------------------------------------------------------

/**
 * Every persistent-state change is one of these writes.  The host MUST make
 * each write durable (flush/fsync) BEFORE delivering the same step's
 * {@link StepResult.messages}.  This is the single durability boundary:
 *
 *  - vote:        term/vote persisted before requesting or granting a vote
 *  - append:      new entries persisted before replicating / acking them
 *  - truncate:    replacement suffix persisted before a successful ack
 *  - snapshot:    snapshot persisted before acking InstallSnapshot
 *
 * Nothing else is allowed to be needed after a crash: all other kernel state
 * is reconstructible from these writes or re-learned over the network.
 */
export type DiskWrite =
  | { kind: "vote"; term: number; votedFor: NodeId | null }
  | { kind: "append"; entries: LogEntry[] }
  | { kind: "truncate"; fromIndex: number; entries: LogEntry[] }
  | {
      kind: "snapshot";
      index: number;
      term: number;
      config: ReadonlySet<NodeId>;
    };

export interface OutboundMessage {
  to: NodeId;
  message: RpcRequest | RpcResponse;
}

/** A client write is safe exactly when this completion fires. */
export interface ClientCompletion {
  kind: "write";
  requestId: number;
  command: Command;
  index: number;
  term: number;
}

/** Read-index confirmation: value is the host FSM at any index >= index. */
export interface ReadReady {
  kind: "read";
  requestId: number;
  query: string;
  index: number;
}

export interface ClientError {
  kind: "error";
  /** Null for operator (membership) requests, which carry no client id. */
  requestId: number | null;
  reason: "not-leader" | "busy" | "compact-rejected";
  /** Best-known leader, if the responder has a hint. */
  leaderHint?: NodeId;
}

export interface StepResult {
  disk: DiskWrite[];
  messages: OutboundMessage[];
  /** Client write completions, in index order. */
  completed: ClientCompletion[];
  /** Reads that may now be served from the applied FSM. */
  reads: ReadReady[];
  /** Immediate client rejections (target is not leader, etc.). */
  errors: ClientError[];
  /** Election timer must be (re)armed when true. */
  resetElectionTimer: boolean;
  /** Fired once the plain configuration entry of a change commits. */
  configCompleted?: ProposeConfigChange;
}

// ---------------------------------------------------------------------------
// Host-side stable storage (the simulator provides an in-memory version)
// ---------------------------------------------------------------------------

/**
 * Contract for the durability layer a real deployment supplies.  The kernel
 * itself never performs I/O: each step returns {@link DiskWrite}s which the
 * host maps onto these operations, flushing before releasing messages.
 */
export interface StableStorage {
  writeVote(term: number, votedFor: NodeId | null): void;
  appendEntries(entries: LogEntry[]): void;
  /** Replace the suffix starting at global `fromIndex` with `entries`. */
  truncateAndAppend(fromIndex: number, entries: LogEntry[]): void;
  /** Install a snapshot, discarding the log prefix through `index`. */
  installSnapshot(index: number, term: number, config: ReadonlySet<NodeId>): void;
}
