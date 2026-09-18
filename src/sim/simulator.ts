/**
 * Deterministic cluster simulator.
 *
 * A single process, a virtual clock, and one seed-driven {@link Random}.
 * The simulator owns:
 *   - per-node live {@link NodeState} plus the {@link MemStorage} that
 *     survives crashes,
 *   - the {@link Network} (loss, duplication, delay, partitions),
 *   - election/heartbeat timers as plain queue events,
 *   - crash/restart and partition fault injection,
 *   - a small key/value client workload and per-node applied KV state,
 *   - an event trace whose bytes are a pure function of the seed.
 *
 * Nothing here uses wall-clock time, real timers, sockets or files.
 */
import {
  initNode,
  restoreNode,
  step,
  configAt,
  entryAt,
} from "../core/index.js";
import type {
  ClientCompletion,
  Message,
  NodeId,
  NodeState,
  ReadReady,
  RpcRequest,
  RpcResponse,
  StepResult,
} from "../core/types.js";
import { EventQueue } from "./queue.js";
import type { TimedEvent } from "./queue.js";
import { Network, DEFAULT_NETWORK, type NetworkParams } from "./network.js";
import { MemStorage } from "./storage.js";
import { Random } from "./rng.js";
import type { Operation } from "../checker/model.js";
import type { KvInput, KvOutput } from "../checker/kv.js";

export interface SimParams {
  nodes: number;
  clients: number;
  /** Total successful client operations to drive. */
  operations: number;
  /** Stop issuing after this virtual time; drain afterwards. */
  stopIssuingAt: number;
  /** Hard end of time; the run finalizes then even if ops remain. */
  maxTime: number;
  electionTimeoutMin: number;
  electionTimeoutMax: number;
  heartbeatInterval: number;
  clientTick: number;
  faultTick: number;
  /** Probability that a fault tick changes the topology. */
  faultChance: number;
  crashDownMin: number;
  crashDownMax: number;
  /** Compact leader log once it grows past this many entries. */
  compactThreshold: number;
  network: NetworkParams;
  /** Also exercise single-node add/remove membership changes. */
  membershipChanges: boolean;
}

export const DEFAULT_PARAMS: SimParams = {
  nodes: 5,
  clients: 3,
  operations: 200,
  // Issuing/faults keep running until the operation count is reached; these
  // two are only safety caps (see run()).
  stopIssuingAt: 200_000,
  maxTime: 200_000,
  electionTimeoutMin: 150,
  electionTimeoutMax: 300,
  heartbeatInterval: 40,
  clientTick: 10,
  faultTick: 350,
  faultChance: 0.6,
  crashDownMin: 250,
  crashDownMax: 1200,
  compactThreshold: 40,
  network: DEFAULT_NETWORK,
  membershipChanges: false,
};

type SimEvent =
  | {
      kind: "rpc";
      from: NodeId;
      to: NodeId;
      message: RpcRequest | RpcResponse;
      /** Snapshot FSM payload carried alongside InstallSnapshot headers. */
      snapshotData?: ReadonlyMap<string, string>;
    }
  | { kind: "election"; node: NodeId }
  | { kind: "heartbeat"; node: NodeId }
  | { kind: "restart"; node: NodeId }
  | { kind: "client"; client: number }
  | { kind: "fault" }
  | { kind: "membership" };

interface NodeRuntime {
  id: NodeId;
  state: NodeState;
  storage: MemStorage;
  alive: boolean;
  /** Virtual time until which the node stays crashed. */
  downUntil: number;
  /** Applied key/value FSM. */
  fsm: Map<string, string>;
  /** FSM snapshots by included index (for InstallSnapshot application data). */
  fsmSnapshots: Map<number, Map<string, string>>;
  generation: number;
  /** The single live election timer event for this node, if armed. */
  electionTimer: TimedEvent<SimEvent> | null;
}

interface ClientCtx {
  id: number;
  idle: boolean;
  reqId: number;
  op: "put" | "get";
  key: string;
  value: string | null;
  start: number;
  target: NodeId;
  deadline: number;
  attempts: number;
}

export interface SimTraceEntry {
  t: number;
  kind: string;
  detail: string;
}

export interface SimResult {
  seed: number;
  history: Operation<KvInput, KvOutput>[];
  trace: SimTraceEntry[];
  finalTime: number;
  completedOps: number;
  /** Unresolved invocations at maxTime (should be zero in healthy runs). */
  unresolved: number;
}

const SET_PREFIX = "SET ";

export class Simulator {
  readonly params: SimParams;
  readonly seedValue: number;
  private rng: Random;
  private now = 0;
  private queue = new EventQueue<SimEvent>();
  private net: Network;
  private nodes = new Map<NodeId, NodeRuntime>();
  private clients: ClientCtx[] = [];
  private history: Operation<KvInput, KvOutput>[] = [];
  private trace: SimTraceEntry[] = [];
  private reqSeq = 0;
  private issued = 0;
  private finished = 0;
  /** Terminal results already recorded per request id (dedup retries). */
  private terminal = new Set<number>();
  /** client reqId -> client index, for routing completions. */
  private reqOwner = new Map<number, number>();
  private stopped = false;

  constructor(seed: number, params: Partial<SimParams> = {}) {
    this.seedValue = seed >>> 0;
    this.params = { ...DEFAULT_PARAMS, ...params, network: { ...DEFAULT_NETWORK, ...(params.network ?? {}) } };
    this.rng = new Random(seed);
    this.net = new Network(this.params.network);
    for (let i = 0; i < this.params.nodes; i++) {
      const storage = new MemStorage(new Set(Array.from({ length: this.params.nodes }, (_, k) => k)));
      const state = initNode(i, new Set(Array.from({ length: this.params.nodes }, (_, k) => k)), 0);
      this.nodes.set(i, {
        id: i,
        state,
        storage,
        alive: true,
        downUntil: 0,
        fsm: new Map(),
        fsmSnapshots: new Map(),
        generation: 0,
        electionTimer: null,
      });
      this.net.addNode(i);
    }
    for (let i = 0; i < this.params.clients; i++) {
      this.clients.push({
        id: i,
        idle: true,
        reqId: -1,
        op: "get",
        key: "k0",
        value: null,
        start: 0,
        target: 0,
        deadline: 0,
        attempts: 0,
      });
    }
  }

  // -- tracing -------------------------------------------------------------

  private log(kind: string, detail: string): void {
    this.trace.push({ t: this.now, kind, detail });
  }

  // -- scheduling ----------------------------------------------------------

  private armElection(rt: NodeRuntime): void {
    // Exactly one live election timer per node: every reset cancels the
    // previously armed deadline, exactly like clearTimeout/setTimeout.
    if (rt.electionTimer) this.queue.cancel(rt.electionTimer);
    const t = this.now + this.rng.int(
      this.params.electionTimeoutMin,
      this.params.electionTimeoutMax,
    );
    rt.electionTimer = this.queue.schedule(t, {
      kind: "election",
      node: rt.id,
    });
  }

  private armHeartbeat(rt: NodeRuntime): void {
    this.queue.schedule(this.now + this.params.heartbeatInterval, {
      kind: "heartbeat",
      node: rt.id,
    });
  }

  // -- boot ----------------------------------------------------------------

  run(): SimResult {
    for (const rt of this.nodes.values()) {
      this.armElection(rt);
      this.queue.schedule(this.rng.int(0, this.params.heartbeatInterval), {
        kind: "heartbeat",
        node: rt.id,
      });
    }
    for (let i = 0; i < this.params.clients; i++) {
      this.queue.schedule(this.rng.int(0, this.params.clientTick), {
        kind: "client",
        client: i,
      });
    }
    this.queue.schedule(this.params.faultTick, { kind: "fault" });
    if (this.params.membershipChanges) {
      this.queue.schedule(this.params.faultTick * 3, { kind: "membership" });
    }

    for (;;) {
      const ev = this.queue.nextLive();
      if (!ev) break;
      if (ev.time > this.params.maxTime) break;
      this.now = ev.time;
      this.dispatch(ev.payload);
      // Once the requested number of operations have completed, stop
      // breaking the cluster and give it a bounded quiet window to drain.
      if (this.finished >= this.params.operations && !this.stopped) {
        this.enterRecovery();
      }
    }

    // Safety recovery if maxTime was hit with operations still in flight.
    this.drain();

    const unresolved = this.clients.filter((c) => !c.idle).length;
    return {
      seed: this.seedValue,
      history: this.history,
      trace: this.trace,
      finalTime: this.now,
      completedOps: this.finished,
      unresolved,
    };
  }

  /** Switch from the fault-injection phase to the quiet drain phase. */
  private enterRecovery(): void {
    this.stopped = true;
    this.net.heal();
    for (const rt of this.nodes.values()) {
      if (!rt.alive) {
        const at = this.now + 1;
        this.queue.schedule(at, { kind: "restart", node: rt.id });
      }
    }
    this.log("recover", "faults stopped; healing cluster");
  }

  /**
   * Final heal/restart plus continued processing until every client
   * invocation has returned (bounded by a generous window).
   */
  private drain(): void {
    this.stopped = true;
    for (const rt of this.nodes.values()) {
      if (!rt.alive) this.restart(rt, this.now);
    }
    this.net.heal();
    const deadline = this.now + 60_000;
    for (;;) {
      if (this.clients.every((c) => c.idle)) break;
      const ev = this.queue.nextLive();
      if (!ev || ev.time > deadline) break;
      this.now = ev.time;
      this.dispatch(ev.payload, true);
    }
  }

  // -- dispatch ------------------------------------------------------------

  private dispatch(ev: SimEvent, draining = false): void {
    switch (ev.kind) {
      case "rpc":
        this.deliverRpc(ev.from, ev.to, ev.message, ev.snapshotData);
        break;
      case "election": {
        const rt = this.nodes.get(ev.node)!;
        rt.electionTimer = null;
        if (!rt.alive) break;
        this.applyStep(rt, { type: "ElectionTimeout" });
        break;
      }
      case "heartbeat": {
        const rt = this.nodes.get(ev.node)!;
        if (rt.alive) {
          this.applyStep(rt, { type: "Heartbeat" });
          this.armHeartbeat(rt);
        }
        break;
      }
      case "restart": {
        const rt = this.nodes.get(ev.node);
        if (rt && !rt.alive) this.restart(rt, this.now);
        break;
      }
      case "client":
        this.clientTick(ev.client);
        this.queue.schedule(this.now + this.params.clientTick, ev);
        break;
      case "fault":
        if (!draining && !this.stopped && this.now < this.params.stopIssuingAt) {
          this.fault();
          this.queue.schedule(this.now + this.params.faultTick, ev);
        }
        break;
        break;
      case "membership":
        if (!draining && this.now < this.params.stopIssuingAt) {
          this.membershipFault();
          this.queue.schedule(this.now + this.params.faultTick * 4, ev);
        }
        break;
    }
  }

  // -- kernel step plumbing ------------------------------------------------

  private applyStep(rt: NodeRuntime, msg: Message): StepResult {
    const result = step(rt.state, this.now, msg);

    // DURABILITY BOUNDARY: all disk writes are applied to the crash-surviving
    // MemStorage before ANY outbound message of this step is scheduled.
    for (const w of result.disk) rt.storage.apply(w);

    this.applyCommitted(rt);

    // Read-index confirmations and completions are processed after apply.
    for (const c of result.completed) this.handleCompletion(rt, c);
    for (const r of result.reads) this.handleRead(rt, r);
    for (const e of result.errors) this.handleError(rt, e.requestId, e.leaderHint);

    // Messages only become visible after the writes above.
    for (const out of result.messages) {
      const snapshotData =
        out.message.type === "InstallSnapshot"
          ? (rt.fsmSnapshots.get(out.message.lastIncludedIndex) ?? rt.fsm)
          : undefined;
      this.emit(rt.id, out.to, out.message, snapshotData);
    }
    if (result.resetElectionTimer && rt.alive) this.armElection(rt);
    return result;
  }

  /** Apply newly committed entries to this node's KV FSM, then notify core. */
  private applyCommitted(rt: NodeRuntime): void {
    const commit = rt.state.v.commitIndex;
    while (rt.state.v.lastApplied < commit) {
      const i = rt.state.v.lastApplied + 1;
      const e = entryAt(rt.state.p, i);
      if (e) {
        if (e.command.startsWith(SET_PREFIX)) {
          const rest = e.command.slice(SET_PREFIX.length);
          const sp = rest.indexOf(" ");
          rt.fsm.set(rest.slice(0, sp), rest.slice(sp + 1));
        }
      } else if (i <= rt.state.p.snapshotIndex) {
        // Index covered by a snapshot but FSM not yet rebuilt: rebuilt by
        // InstallSnapshot handling; ignore here.
      }
      rt.state.v.lastApplied = i;
    }
    if (commit > 0) {
      // Notify the kernel in its own step so pending reads can release.
      const r2 = step(rt.state, this.now, { type: "Applied", index: commit });
      for (const w of r2.disk) rt.storage.apply(w);
      for (const r of r2.reads) this.handleRead(rt, r);
      for (const c of r2.completed) this.handleCompletion(rt, c);
      for (const out of r2.messages) this.emit(rt.id, out.to, out.message);
    }
    this.maybeCompact(rt);
  }

  private maybeCompact(rt: NodeRuntime): void {
    if (rt.state.v.role !== "leader") return;
    const logLen = rt.state.p.log.length;
    if (logLen < this.params.compactThreshold) return;
    const target = rt.state.v.commitIndex - 5;
    if (target <= rt.state.p.snapshotIndex) return;
    // Snapshot only at a plain-config index.
    let idx = target;
    while (idx > rt.state.p.snapshotIndex) {
      const cfg = configAt(rt.state.p, idx);
      if (cfg && cfg.kind === "plain") break;
      idx--;
    }
    if (idx <= rt.state.p.snapshotIndex) return;
    const result = this.applyStep(rt, { type: "Compact", index: idx });
    void result;
    if (rt.state.p.snapshotIndex === idx) {
      rt.fsmSnapshots.set(idx, new Map(rt.fsm));
      this.log("snapshot", `node=${rt.id} index=${idx}`);
    }
  }

  // -- messaging -----------------------------------------------------------

  private emit(
    from: NodeId,
    to: NodeId,
    message: RpcRequest | RpcResponse,
    snapshotData?: ReadonlyMap<string, string>,
  ): void {
    const rt = this.nodes.get(from)!;
    if (!rt.alive) return;
    if (!this.net.shouldDeliver(this.rng, from, to)) {
      this.log("drop", `${from}->${to} ${message.type}`);
      return;
    }
    const copies = this.net.shouldDuplicate(this.rng) ? 2 : 1;
    for (let c = 0; c < copies; c++) {
      const at = this.now + this.net.delay(this.rng);
      this.queue.schedule(at, { kind: "rpc", from, to, message, snapshotData });
    }
  }

  private deliverRpc(
    from: NodeId,
    to: NodeId,
    message: RpcRequest | RpcResponse,
    snapshotData?: ReadonlyMap<string, string>,
  ): void {
    // Partition may have formed after the message was sent.
    if (!this.net.connected(from, to)) return;
    const rt = this.nodes.get(to)!;
    if (!rt.alive) return; // crashed: delivery is lost (no receive buffer)

    const before = rt.state.p.snapshotIndex;
    this.applyStep(rt, message as Message);
    // InstallSnapshot durable boundary: after the kernel step the snapshot
    // write is on disk; rebuild the applied FSM from the transferred data so
    // subsequent read-index queries observe a consistent prefix.
    if (message.type === "InstallSnapshot" && rt.state.p.snapshotIndex > before) {
      rt.fsm = new Map(snapshotData ?? []);
      rt.fsmSnapshots.set(rt.state.p.snapshotIndex, new Map(rt.fsm));
      rt.state.v.lastApplied = Math.max(
        rt.state.v.lastApplied,
        rt.state.p.snapshotIndex,
      );
      this.log(
        "snapshot-install",
        `node=${rt.id} index=${rt.state.p.snapshotIndex}`,
      );
    }
  }

  // -- client results ------------------------------------------------------

  private handleCompletion(rt: NodeRuntime, c: ClientCompletion): void {
    if (this.terminal.has(c.requestId)) return;
    const owner = this.reqOwner.get(c.requestId);
    if (owner === undefined) return;
    const cli = this.clients[owner]!;
    if (cli.idle || cli.reqId !== c.requestId) {
      // Late completion for an already-retried request: still completes it
      // the first time we see success, using the invocation start.
    }
    this.terminal.add(c.requestId);
    const value = c.command.startsWith(SET_PREFIX)
      ? c.command.slice(SET_PREFIX.length).split(" ")[1]!
      : null;
    this.history.push({
      id: this.history.length,
      start: cli.start,
      end: this.now,
      client: cli.id,
      node: rt.id,
      input:
        cli.op === "put"
          ? { op: "put", key: cli.key, value: value ?? "" }
          : { op: "get", key: cli.key },
      output: cli.op === "put" ? "ok" : null,
    });
    this.finishClient(cli);
    this.log("complete", `req=${c.requestId} node=${rt.id} idx=${c.index}`);
  }

  private handleRead(rt: NodeRuntime, r: ReadReady): void {
    if (this.terminal.has(r.requestId)) return;
    const owner = this.reqOwner.get(r.requestId);
    if (owner === undefined) return;
    const cli = this.clients[owner]!;
    this.terminal.add(r.requestId);
    this.history.push({
      id: this.history.length,
      start: cli.start,
      end: this.now,
      client: cli.id,
      node: rt.id,
      input: { op: "get", key: cli.key },
      output: rt.fsm.get(r.query) ?? null,
    });
    this.finishClient(cli);
    this.log("read", `req=${r.requestId} node=${rt.id} index=${r.index}`);
  }

  private handleError(rt: NodeRuntime, requestId: number | null, hint?: NodeId): void {
    if (requestId === null) return;
    const owner = this.reqOwner.get(requestId);
    if (owner === undefined || this.terminal.has(requestId)) return;
    const cli = this.clients[owner]!;
    if (hint !== undefined) cli.target = hint;
    this.log("redirect", `req=${requestId} via=${rt.id} hint=${hint ?? "?"}`);
  }

  private finishClient(cli: ClientCtx): void {
    this.finished++;
    cli.idle = true;
    cli.reqId = -1;
  }

  // -- client workload -----------------------------------------------------

  private clientTick(i: number): void {
    const cli = this.clients[i]!;
    if (cli.idle) {
      if (this.stopped || this.issued >= this.params.operations) return;
      this.issue(cli);
      return;
    }
    // Retry timed-out invocations; the invocation start does NOT move.
    if (this.now >= cli.deadline) this.issue(cli, true);
  }

  private issue(cli: ClientCtx, retry = false): void {
    if (!retry) {
      cli.op = this.rng.chance(0.5) ? "put" : "get";
      cli.key = `k${this.rng.int(0, 4)}`;
      if (cli.op === "put") cli.value = `v${this.reqSeq}`;
      cli.reqId = this.reqSeq++;
      cli.start = this.now;
      cli.attempts = 0;
      this.issued++;
      this.reqOwner.set(cli.reqId, cli.id);
      cli.idle = false;
      // First target: sticky random node, learns leaders from hints.
      cli.target = this.rng.int(0, this.params.nodes - 1);
    }
    cli.attempts++;
    cli.deadline = this.now + this.rng.int(200, 500);

    const target = this.chooseTarget(cli);
    const rt = this.nodes.get(target)!;
    if (!rt.alive) {
      // Will retry on the next tick; pick someone else meanwhile.
      cli.target = this.rng.int(0, this.params.nodes - 1);
      return;
    }
    const req: Message =
      cli.op === "put"
        ? {
            type: "ClientWrite",
            requestId: cli.reqId,
            command: `${SET_PREFIX}${cli.key} ${cli.value}`,
          }
        : { type: "ClientRead", requestId: cli.reqId, query: cli.key };
    this.log("invoke", `req=${cli.reqId} ${cli.op} node=${target}`);
    this.applyStep(rt, req);
  }

  private chooseTarget(cli: ClientCtx): NodeId {
    // Prefer the hinted leader; if that node is down, rotate.
    if (!this.nodes.get(cli.target)?.alive) {
      const alive = [...this.nodes.values()].filter((r) => r.alive).map((r) => r.id);
      if (alive.length > 0) cli.target = this.rng.pick(alive);
    }
    return cli.target;
  }

  // -- faults --------------------------------------------------------------

  private fault(): void {
    if (!this.rng.chance(this.params.faultChance)) return;
    const roll = this.rng.float();
    if (roll < 0.45) {
      this.crashOne();
    } else if (roll < 0.7) {
      this.partitionRandom();
    } else {
      this.net.heal();
      this.log("heal", "full connectivity");
    }
  }

  private crashOne(): void {
    const alive = [...this.nodes.values()].filter((r) => r.alive);
    // Keep a majority of 5 alive: only crash when at least 4 are up (so the
    // cluster can usually make progress; partitions do the heavier damage).
    if (alive.length < 4) return;
    const rt = this.rng.pick(alive);
    this.crash(rt);
    const back = this.now + this.rng.int(this.params.crashDownMin, this.params.crashDownMax);
    rt.downUntil = back;
    this.queue.schedule(back, { kind: "restart", node: rt.id });
    this.log("crash", `node=${rt.id} until=${back}`);
  }

  private crash(rt: NodeRuntime): void {
    if (rt.electionTimer) this.queue.cancel(rt.electionTimer);
    rt.electionTimer = null;
    rt.alive = false;
    rt.generation++;
    // Memory state vanishes; storage survives.
    rt.state = null as unknown as NodeState;
    rt.fsm = new Map();
  }

  private restart(rt: NodeRuntime, at: number): void {
    const durable = rt.storage.state;
    rt.state = restoreNode(rt.id, durable, at);
    rt.fsm = durable.snapshotIndex > 0
      ? new Map(rt.fsmSnapshots.get(durable.snapshotIndex) ?? [])
      : new Map();
    rt.alive = true;
    rt.electionTimer = null;
    this.armElection(rt);
    this.queue.schedule(at + this.params.heartbeatInterval, {
      kind: "heartbeat",
      node: rt.id,
    });
    this.log("restart", `node=${rt.id} term=${durable.currentTerm} snap=${durable.snapshotIndex}`);
  }

  private partitionRandom(): void {
    const ids = [...this.nodes.keys()];
    const shuffled = this.rng.shuffle(ids);
    const cut = this.rng.int(1, shuffled.length - 1);
    this.net.partition([shuffled.slice(0, cut), shuffled.slice(cut)]);
    this.log(
      "partition",
      `[${shuffled.slice(0, cut).join(",")}] | [${shuffled.slice(cut).join(",")}]`,
    );
  }

  // -- membership change exercise -----------------------------------------

  private membershipFault(): void {
    const leader = [...this.nodes.values()].find(
      (r) => r.alive && r.state.v.role === "leader",
    );
    if (!leader) return;
    const spare = this.params.nodes; // spare node id == n
    const present = this.nodes.has(spare);
    const change = present
      ? { type: "ProposeConfigChange", nodeId: spare, add: false }
      : { type: "ProposeConfigChange", nodeId: spare, add: true };
    this.log("config", `${change.add ? "add" : "remove"} node ${spare}`);
    const result = this.applyStep(leader, change as Message);
    if (!present && result.errors.length === 0) {
      // Bring the spare node into existence (storage bootstrapped later via
      // snapshot/log catch-up); it is not a voter until the joint entry
      // commits under both majorities.
      const storage = new MemStorage(new Set([...this.nodes.keys()]));
      const state = initNode(spare, new Set([...this.nodes.keys()]), this.now);
      this.nodes.set(spare, {
        id: spare,
        state,
        storage,
        alive: true,
        downUntil: 0,
        fsm: new Map(),
        fsmSnapshots: new Map(),
        generation: 0,
        electionTimer: null,
      });
      this.net.addNode(spare);
      this.armElection(this.nodes.get(spare)!);
      this.queue.schedule(this.now + this.params.heartbeatInterval, {
        kind: "heartbeat",
        node: spare,
      });
    }
    if (present && result.configCompleted) {
      // Node removed: switch it off and drop it from the network view.
      const rt = this.nodes.get(spare);
      if (rt) this.crash(rt);
      this.nodes.delete(spare);
      this.net.removeNode(spare);
    }
  }
}
