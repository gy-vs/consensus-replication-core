/**
 * Pure helpers for log indexing and membership configurations.
 */
import type {
  ConfigPayload,
  LogEntry,
  NodeId,
  PersistentState,
} from "./types.js";

/** First log index still physically present (snapshotIndex + 1). */
export function firstIndex(p: PersistentState): number {
  return p.snapshotIndex + 1;
}

/** Global index of the last log entry (equals snapshotIndex when empty). */
export function lastIndex(p: PersistentState): number {
  return p.snapshotIndex + p.log.length;
}

/** Global index of the last entry, taking an unsaved suffix into account. */
export function lastTerm(p: PersistentState): number {
  const e = p.log[p.log.length - 1];
  return e ? e.term : p.snapshotTerm;
}

/** Term of the entry at global `index`, or null when compacted / absent. */
export function termAt(p: PersistentState, index: number): number | null {
  if (index === 0) return 0;
  if (index === p.snapshotIndex) return p.snapshotTerm;
  const e = p.log[index - p.snapshotIndex - 1];
  return e ? e.term : null;
}

/** Entry at global `index`, or null when compacted / absent. */
export function entryAt(p: PersistentState, index: number): LogEntry | null {
  const e = p.log[index - p.snapshotIndex - 1];
  return e ?? null;
}

/** Slice of entries with global index >= from (snapshot entries excluded). */
export function entriesFrom(p: PersistentState, from: number): LogEntry[] {
  const start = from - p.snapshotIndex - 1;
  if (start >= p.log.length) return [];
  return p.log.slice(Math.max(0, start));
}

/**
 * Effective configuration at global `index`.  Snapshot covers indices
 * <= snapshotIndex; configurations ride on individual entries afterwards.
 * Returns null only for index 0 before any configuration exists.
 */
export function configAt(
  p: PersistentState,
  index: number,
): ConfigPayload | null {
  if (index <= p.snapshotIndex) {
    return p.snapshotConfig ? { kind: "plain", voters: p.snapshotConfig } : null;
  }
  let cfg: ConfigPayload | null = p.snapshotConfig
    ? { kind: "plain", voters: p.snapshotConfig }
    : null;
  const upto = Math.min(index, lastIndex(p));
  for (let i = p.snapshotIndex + 1; i <= upto; i++) {
    const c = entryAt(p, i)?.config;
    if (c) cfg = c;
  }
  return cfg;
}

/** Configuration carried at the end of the log (the one a leader appends by). */
export function latestConfig(p: PersistentState): ConfigPayload {
  const cfg = configAt(p, lastIndex(p));
  if (!cfg) throw new Error("cluster has no initial configuration");
  return cfg;
}

/** Voter set used for the *entry* at index: plain voters, or union in joint. */
export function replicationSet(cfg: ConfigPayload): ReadonlySet<NodeId> {
  if (cfg.kind === "plain") return cfg.voters;
  return new Set<NodeId>([...cfg.oldVoters, ...cfg.newVoters]);
}

/** True if a majority of every required voter set has `ack(index)` true. */
export function quorumAt(
  cfg: ConfigPayload,
  index: number,
  selfId: NodeId,
  ack: (peer: NodeId, idx: number) => boolean,
): boolean {
  const oneSet = (voters: ReadonlySet<NodeId>): boolean => {
    let yes = ack(selfId, index) ? 1 : 0;
    let n = 1;
    for (const id of voters) {
      if (id === selfId) continue;
      n++;
      if (ack(id, index)) yes++;
    }
    return yes > n / 2;
  };
  if (cfg.kind === "plain") return oneSet(cfg.voters);
  return oneSet(cfg.oldVoters) && oneSet(cfg.newVoters);
}

/** Membership majority size for a set of `n` voters. */
export function majority(n: number): number {
  return Math.floor(n / 2) + 1;
}

/**
 * Raft "up-to-date" comparison (§5.4.1): a candidate's log is at least as
 * good as a voter's iff the last terms differ the higher wins, else the
 * longer log wins.
 */
export function logAtLeastAsGood(
  candidateLastTerm: number,
  candidateLastIndex: number,
  voterLastTerm: number,
  voterLastIndex: number,
): boolean {
  if (candidateLastTerm !== voterLastTerm)
    return candidateLastTerm > voterLastTerm;
  return candidateLastIndex >= voterLastIndex;
}
