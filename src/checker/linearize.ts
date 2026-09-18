/**
 * Linearizability checker.
 *
 * Self-contained implementation of the Herlihy–Wing queue-variable backtrack
 * search (Wing–Gong style), with no third-party checking library:
 *
 *   - Event times are the sorted distinct call/return times of the history.
 *   - At each time t:
 *       * operations called at t become "in flight";
 *       * any in-flight operation may be linearized at t;
 *       * an operation whose return time is t MUST be linearized at or
 *         before t — enforced before the search moves to the next slot.
 *   - Every candidate is tried against the sequential specification.
 *
 * The search is an ITERATIVE depth-first search over an explicit stack of
 * frames, so histories with hundreds of operations/event slots cannot blow
 * the JavaScript call stack.
 *
 * One pruning, deliberately conservative so it is obviously sound: an exact
 * failure memo on the triple (time slot, model state, pending set).  The
 * continuation of such a triple is deterministic — event times, candidate
 * ops and the model are fixed — so once it has failed it can never succeed
 * via a different arrival path.  Read-only operations reuse the same state
 * object, which is what makes read-heavy histories cheap.
 *
 * Generic over any {@link SequentialModel}; contains no Raft/KV knowledge.
 */
import type {
  CheckResult,
  Operation,
  OpInterval,
  SequentialModel,
  Time,
  Violation,
} from "./model.js";

export interface CheckOptions {
  /** Model-step safety valve; when exhausted the result is a violation. */
  maxSteps?: number;
}

interface Frame {
  tIdx: number;
  state: unknown;
  /** Calls admitted when THIS frame opened its slot (empty for frames that
   *  merely linearize one more op inside the same slot). */
  added: number[];
  /** Candidate ops still to try at this slot. */
  candidates: number[];
  ptr: number;
  /** Set once the frame has pushed its advance-to-next-slot child. */
  advanceTried: boolean;
  memo?: string;
}

interface SearchCtx<I, O, S> {
  model: SequentialModel<S, I, O>;
  ops: Operation<I, O>[];
  times: Time[];
  callsAt: number[][];
  dueAt: number[][];
  maxSteps: number;
  steps: number;
  failed: Map<unknown, Set<string>>;
  bestDepth: number;
  bestOrder: number[];
  bestPending: number[];
  bestTimeIndex: number;
  bestTime: Time;
  budgetExceeded: boolean;
}

function interval<I, O>(op: Operation<I, O>, note?: string): OpInterval<I, O> {
  return { op, note };
}

function sigOf(pending: Set<number>): string {
  if (pending.size === 0) return "";
  return [...pending].sort((a, b) => a - b).join(",");
}

function memoKey(tIdx: number, sig: string): string {
  return `${tIdx}:${sig}`;
}

/** Check a complete history for linearizability against `model`. */
export function checkLinearizable<I, O, S>(
  model: SequentialModel<S, I, O>,
  history: readonly Operation<I, O>[],
  options: CheckOptions = {},
): CheckResult<I, O> {
  const bad = validateHistory(history);
  if (bad) return bad;

  const ops = [...history].sort((a, b) => a.id - b.id);

  const timeSet = new Set<Time>();
  for (const op of ops) {
    timeSet.add(op.start);
    timeSet.add(op.end);
  }
  const times = [...timeSet].sort((a, b) => (a < b ? -1 : 1));
  const timeIndex = new Map<Time, number>();
  times.forEach((t, i) => timeIndex.set(t, i));

  const callsAt: number[][] = times.map(() => []);
  const dueAt: number[][] = times.map(() => []);
  for (const op of ops) {
    callsAt[timeIndex.get(op.start)!]!.push(op.id);
    dueAt[timeIndex.get(op.end)!]!.push(op.id);
  }

  const ctx: SearchCtx<I, O, S> = {
    model,
    ops,
    times,
    callsAt,
    dueAt,
    maxSteps: options.maxSteps ?? 5_000_000,
    steps: 0,
    failed: new Map(),
    bestDepth: -1,
    bestOrder: [],
    bestPending: [],
    bestTimeIndex: 0,
    bestTime: times[times.length - 1] ?? 0,
    budgetExceeded: false,
  };

  const pending = new Set<number>();
  const order: number[] = [];
  const ok = search(ctx, model.initial(), pending, order);
  return ok ? { ok: true } : buildViolation(ctx);
}

function validateHistory<I, O>(
  history: readonly Operation<I, O>[],
): Violation<I, O> | null {
  for (const op of history) {
    if (op.end < op.start) {
      return {
        ok: false,
        kind: "bad-history",
        message: `operation ${op.id} ends (${op.end}) before it starts (${op.start})`,
        witness: [interval(op, "end before start")],
        attemptedOrder: [],
        inFlight: [],
        atTime: op.start,
      };
    }
  }
  return null;
}

/**
 * Iterative DFS.  A frame is either "opening" a time slot (admitting its
 * calls) or linearizing one more eligible op inside the current slot; both
 * are represented uniformly.  Advancing time keeps the parent frame on the
 * stack, so admitted calls correctly remain pending across slots and are
 * only rolled back when the whole subtree backtracks past the slot.
 */
function search<I, O, S>(
  ctx: SearchCtx<I, O, S>,
  initial: S,
  pending: Set<number>,
  order: number[],
): boolean {
  const stack: Frame[] = [
    { tIdx: 0, state: initial as unknown, added: [...ctx.callsAt[0]!], candidates: [], ptr: 0, advanceTried: false },
  ];

  while (stack.length > 0) {
    const f = stack[stack.length - 1]!;

    // First visit: admit calls and build candidate list.
    if (f.candidates.length === 0 && f.ptr === 0) {
      for (const id of f.added) pending.add(id);

      if (f.tIdx >= ctx.times.length) {
        if (pending.size === 0) return true;
        failFrame(ctx, stack, pending, order, f);
        continue;
      }

      // Exact failure memo for (slot, state, pending set).
      const mk = memoKey(f.tIdx, sigOf(pending));
      if (ctx.failed.get(f.state as object)?.has(mk)) {
        failFrame(ctx, stack, pending, order, f);
        continue;
      }
      f.memo = mk;

      const due = ctx.dueAt[f.tIdx]!;
      f.candidates = [...pending].sort((a, b) => {
        const da = due.includes(a) ? 0 : 1;
        const db = due.includes(b) ? 0 : 1;
        if (da !== db) return da - db;
        return a - b;
      });
    }

    // Try the next legal candidate op (a linearization stays in the slot).
    let extended = false;
    while (f.ptr < f.candidates.length) {
      const id = f.candidates[f.ptr++]!;
      // Guard: candidates were computed on entry and an id may already have
      // been consumed along another path reflected in `pending`.
      if (!pending.has(id)) continue;
      const op = ctx.ops[id]!;
      ctx.steps++;
      if (ctx.steps > ctx.maxSteps) {
        ctx.budgetExceeded = true;
        recordStuck(ctx, f.tIdx, order, pending);
        return false;
      }
      const next = ctx.model.step(f.state as S, op.input, op.output);
      if (next === null) continue;
      pending.delete(id);
      order.push(id);
      stack.push({
        tIdx: f.tIdx,
        state: next as unknown,
        added: [],
        candidates: [],
        ptr: 0,
        advanceTried: false,
      });
      extended = true;
      break;
    }
    if (extended) continue;

    // No more legal placements from this frame.  If no op is due here, try
    // advancing to the next event time exactly once.
    const inRange = f.tIdx < ctx.times.length;
    const overdue = inRange && ctx.dueAt[f.tIdx]!.some((id) => pending.has(id));
    if (inRange && !overdue && !f.advanceTried) {
      f.advanceTried = true;
      const nextIdx = f.tIdx + 1;
      stack.push({
        tIdx: nextIdx,
        state: f.state,
        added: nextIdx < ctx.times.length ? [...ctx.callsAt[nextIdx]!] : [],
        candidates: [],
        ptr: 0,
        advanceTried: false,
      });
      continue;
    }

    // Dead end.
    failFrame(ctx, stack, pending, order, f);
  }
  return false;
}

/** Pop one failed frame: record the failure memo and undo its effect. */
function failFrame<I, O, S>(
  ctx: SearchCtx<I, O, S>,
  stack: Frame[],
  pending: Set<number>,
  order: number[],
  f: Frame,
): void {
  if (order.length > ctx.bestDepth) recordStuck(ctx, f.tIdx, order, pending);
  if (f.memo) {
    let set = ctx.failed.get(f.state as object);
    if (!set) {
      set = new Set<string>();
      ctx.failed.set(f.state as object, set);
    }
    set.add(f.memo);
  }
  stack.pop();
  if (f.added.length > 0) {
    // Slot-entry frame: roll back the calls it admitted.
    for (const id of f.added) pending.delete(id);
  } else {
    // Linearization frame: undo the op it placed.
    const id = order.pop();
    if (id !== undefined) pending.add(id);
  }
}

function recordStuck<I, O, S>(
  ctx: SearchCtx<I, O, S>,
  tIdx: number,
  order: number[],
  pending: Set<number>,
): void {
  ctx.bestDepth = order.length;
  ctx.bestOrder = [...order];
  ctx.bestPending = [...pending];
  ctx.bestTimeIndex = tIdx;
  ctx.bestTime = ctx.times[tIdx] ?? ctx.times[ctx.times.length - 1] ?? 0;
}

function buildViolation<I, O, S>(
  ctx: SearchCtx<I, O, S>,
): Violation<I, O> {
  const describe = (id: number, note?: string): OpInterval<I, O> =>
    interval(ctx.ops[id]!, note);

  const attemptedOrder = ctx.bestOrder.map((id) => describe(id));
  const inFlight = ctx.bestPending
    .slice()
    .sort((a, b) => a - b)
    .map((id) => {
      const op = ctx.ops[id]!;
      const overdue = op.end <= ctx.bestTime;
      return describe(
        id,
        overdue ? "return already due but no legal placement exists" : undefined,
      );
    });

  // Witness: the tail of the legal prefix plus every op that could not fit.
  const tail = attemptedOrder.slice(-8);
  const witness: OpInterval<I, O>[] = [...tail, ...inFlight];

  const lines: string[] = [];
  lines.push("history is not linearizable");
  lines.push(
    `longest legal prefix: ${Math.max(0, ctx.bestDepth)} of ${ctx.ops.length} ops`,
  );
  lines.push(`stuck at time ${ctx.bestTime}`);
  if (ctx.budgetExceeded) lines.push("search step budget exhausted");
  if (tail.length > 0) {
    lines.push("last operations in the attempted linearization:");
    for (const w of tail) {
      lines.push(
        `  #${w.op.id} [${w.op.start},${w.op.end}] ${ctx.model.describeInput(
          w.op.input,
        )} -> ${ctx.model.describeOutput(w.op.output)} ` +
          `(client ${w.op.client}, node ${w.op.node ?? "?"})`,
      );
    }
  }
  lines.push("operations that could not be placed:");
  for (const w of inFlight) {
    lines.push(
      `  #${w.op.id} [${w.op.start},${w.op.end}] ${ctx.model.describeInput(
        w.op.input,
      )} -> ${ctx.model.describeOutput(w.op.output)} ` +
        `(client ${w.op.client}, node ${w.op.node ?? "?"})${w.note ? ` — ${w.note}` : ""}`,
    );
  }

  return {
    ok: false,
    kind: "non-sequential",
    message: lines.join("\n"),
    witness,
    attemptedOrder,
    inFlight,
    atTime: ctx.bestTime,
  };
}
