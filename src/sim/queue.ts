/**
 * Deterministic priority queue of timed events.
 *
 * Ordering is by (time, sequence): events scheduled for the same instant
 * fire in insertion order, and insertion order itself is a pure function of
 * the seed-driven event processing, so identical seeds replay identically.
 */
export interface TimedEvent<T> {
  time: number;
  seq: number;
  payload: T;
  /** When false the event is skipped (cancellation without removal). */
  alive: boolean;
}

export class EventQueue<T> {
  private heap: TimedEvent<T>[] = [];
  private counter = 0;

  get size(): number {
    return this.heap.length;
  }

  /** Cancel a previously scheduled event; its pop becomes a no-op. */
  cancel(ev: TimedEvent<T>): void {
    ev.alive = false;
  }

  schedule(time: number, payload: T): TimedEvent<T> {
    const ev: TimedEvent<T> = { time, seq: this.counter++, payload, alive: true };
    const h = this.heap;
    h.push(ev);
    let i = h.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (less(h[i]!, h[parent]!)) {
        [h[i], h[parent]] = [h[parent]!, h[i]!];
        i = parent;
      } else break;
    }
    return ev;
  }

  peek(): TimedEvent<T> | null {
    while (this.heap.length > 0 && !this.heap[0]!.alive) {
      this.pop();
    }
    return this.heap[0] ?? null;
  }

  pop(): TimedEvent<T> | null {
    const h = this.heap;
    if (h.length === 0) return null;
    const top = h[0]!;
    const last = h.pop()!;
    if (h.length > 0) {
      h[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let smallest = i;
        if (l < h.length && less(h[l]!, h[smallest]!)) smallest = l;
        if (r < h.length && less(h[r]!, h[smallest]!)) smallest = r;
        if (smallest === i) break;
        [h[i], h[smallest]] = [h[smallest]!, h[i]!];
        i = smallest;
      }
    }
    return top;
  }

  /** Pop the next live event (skipping cancelled ones). */
  nextLive(): TimedEvent<T> | null {
    for (;;) {
      const ev = this.pop();
      if (!ev) return null;
      if (ev.alive) return ev;
    }
  }
}

function less<T>(a: TimedEvent<T>, b: TimedEvent<T>): boolean {
  if (a.time !== b.time) return a.time < b.time;
  return a.seq < b.seq;
}
