/**
 * In-memory stable storage for the simulator.
 *
 * A {@link MemStorage} is what survives a simulated crash: the live
 * {@link NodeState} vanishes, this object does not.  Every kernel
 * {@link DiskWrite} is applied here synchronously (standing in for an fsync)
 * BEFORE the corresponding outbound messages are scheduled — the same
 * ordering a real host must enforce.
 */
import type {
  DiskWrite,
  LogEntry,
  NodeId,
  PersistentState,
} from "../core/types.js";

export class MemStorage {
  /** Durable Raft state.  Treated as opaque outside {@link apply}. */
  readonly state: PersistentState;
  /** Count of durability barriers, for assertions. */
  syncs = 0;

  constructor(initialVoters: ReadonlySet<NodeId>) {
    this.state = {
      currentTerm: 0,
      votedFor: null,
      log: [],
      snapshotIndex: 0,
      snapshotTerm: 0,
      snapshotConfig: new Set(initialVoters),
    };
  }

  /**
   * Apply one disk write.  In a real host this is the function that must
   * flush to the device before returning; here it mutates the durable copy.
   */
  apply(w: DiskWrite): void {
    const p = this.state;
    switch (w.kind) {
      case "vote":
        p.currentTerm = w.term;
        p.votedFor = w.votedFor;
        break;
      case "append":
        for (const e of w.entries) p.log.push(cloneEntry(e));
        break;
      case "truncate": {
        const offset = w.fromIndex - p.snapshotIndex - 1;
        p.log.length = Math.max(0, offset);
        for (const e of w.entries) p.log.push(cloneEntry(e));
        break;
      }
      case "snapshot":
        p.snapshotIndex = w.index;
        p.snapshotTerm = w.term;
        p.snapshotConfig = new Set(w.config);
        p.log = [];
        break;
    }
    this.syncs++;
  }
}

function cloneEntry(e: LogEntry): LogEntry {
  // Config sets are immutable once built, but copying keeps durable and
  // live state from sharing mutable array buffers unexpectedly.
  return {
    term: e.term,
    command: e.command,
    config: e.config ? { ...e.config } : null,
  } as LogEntry;
}
