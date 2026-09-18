/**
 * Deterministic pseudo-random source.
 *
 * Everything stochastic in the simulator — timer durations, message drops,
 * delays, duplicates, crash/restart/partition choices — draws from THIS
 * object in a fixed call order.  There is no Math.random(), no Date.now()
 * and no real timer anywhere; two runs with the same seed observe the exact
 * same sequence of draws and hence the exact same event trace.
 *
 * splitmix64-style state updates rendered into 32-bit output (JS numbers
 * stay within safe range by working on the two halves).
 */
export class Random {
  private state: number;
  /** Count of draws, useful for traces and "no draws during replay" checks. */
  draws = 0;

  constructor(seed: number) {
    // Normalize the seed to an unsigned 32-bit value.
    this.state = seed >>> 0;
    if (this.state === 0) this.state = 0x9e3779b9;
  }

  /** Raw unsigned 32-bit draw. */
  next32(): number {
    // splitmix32: cheap, well-distributed, fully deterministic.
    this.state = (this.state + 0x9e3779b9) >>> 0;
    let z = this.state;
    z = Math.imul(z ^ (z >>> 16), 0x21f0aaad);
    z = Math.imul(z ^ (z >>> 15), 0x735a2d97);
    z = (z ^ (z >>> 15)) >>> 0;
    this.draws++;
    return z;
  }

  /** Float in [0, 1). */
  float(): number {
    return this.next32() / 0x1_0000_0000;
  }

  /** Uniform integer in [lo, hi] inclusive. */
  int(lo: number, hi: number): number {
    if (hi < lo) throw new Error(`int: empty range [${lo}, ${hi}]`);
    return lo + (this.next32() % (hi - lo + 1));
  }

  /** Pick a uniformly random element. */
  pick<T>(items: readonly T[]): T {
    if (items.length === 0) throw new Error("pick: empty");
    return items[this.next32() % items.length]!;
  }

  /** Fisher–Yates shuffle (returns a new array). */
  shuffle<T>(items: readonly T[]): T[] {
    const out = [...items];
    for (let i = out.length - 1; i > 0; i--) {
      const j = this.next32() % (i + 1);
      const tmp = out[i]!;
      out[i] = out[j]!;
      out[j] = tmp;
    }
    return out;
  }

  /** True with probability p. */
  chance(p: number): boolean {
    return this.float() < p;
  }
}
