/**
 * Simulated network topology and message delivery.
 *
 * The network is a thin filter used by the simulator when kernel messages
 * come out:
 *   - messages are dropped (at random or because of a partition),
 *   - duplicated (an extra copy queued with another random delay),
 *   - delayed arbitrarily (bounded, but bounds are simulator parameters),
 *   - and therefore may arrive out of order or many times.
 *
 * A partition is a symmetric connectivity relation between node ids.  It is
 * checked both at send and at delivery time, so a partition that forms
 * after a message was sent still swallows it (in-flight loss), and a healed
 * partition delivers again without any reset.
 */
import type { NodeId } from "../core/types.js";
import type { Random } from "./rng.js";

export interface NetworkParams {
  /** Per-message independent loss probability when connected. */
  loss: number;
  /** Probability an extra duplicate is also queued. */
  duplicate: number;
  /** One-way delay range in simulated milliseconds. */
  minDelay: number;
  maxDelay: number;
}

export const DEFAULT_NETWORK: NetworkParams = {
  loss: 0.02,
  duplicate: 0.01,
  minDelay: 1,
  maxDelay: 25,
};

export class Network {
  /** Directed reachability: reachable[a][b]. */
  private reach: Map<NodeId, Set<NodeId>> = new Map();
  params: NetworkParams;

  constructor(params: NetworkParams = DEFAULT_NETWORK) {
    this.params = { ...params };
  }

  addNode(id: NodeId): void {
    if (!this.reach.has(id)) this.reach.set(id, new Set());
    for (const set of this.reach.values()) set.add(id);
    for (const id2 of this.reach.keys()) this.reach.get(id)!.add(id2);
  }

  removeNode(id: NodeId): void {
    this.reach.delete(id);
    for (const set of this.reach.values()) set.delete(id);
  }

  /** Is a directed message a -> b currently deliverable? */
  connected(a: NodeId, b: NodeId): boolean {
    if (a === b) return true;
    return this.reach.get(a)?.has(b) ?? false;
  }

  /** Cut all links between the two groups (symmetric partition). */
  partition(groups: NodeId[][]): void {
    this.heal();
    for (let i = 0; i < groups.length; i++) {
      for (let j = i + 1; j < groups.length; j++) {
        for (const a of groups[i]!) for (const b of groups[j]!) {
          this.reach.get(a)?.delete(b);
          this.reach.get(b)?.delete(a);
        }
      }
    }
  }

  /** Isolate a single node from all others. */
  isolate(id: NodeId): void {
    for (const other of this.reach.keys()) {
      if (other === id) continue;
      this.reach.get(id)?.delete(other);
      this.reach.get(other)?.delete(id);
    }
  }

  /** Restore full connectivity among known nodes. */
  heal(): void {
    const ids = [...this.reach.keys()];
    for (const a of ids) for (const b of ids) this.reach.get(a)!.add(b);
  }

  /** Roll for independent loss given current connectivity (false = drop). */
  shouldDeliver(rng: Random, from: NodeId, to: NodeId): boolean {
    if (!this.connected(from, to)) return false;
    return !rng.chance(this.params.loss);
  }

  /** Roll whether to emit an extra duplicate copy. */
  shouldDuplicate(rng: Random): boolean {
    return rng.chance(this.params.duplicate);
  }

  delay(rng: Random): number {
    return rng.int(this.params.minDelay, this.params.maxDelay);
  }
}
