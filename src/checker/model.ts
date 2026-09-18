/**
 * Linearizability model and history types.
 *
 * A history is a set of *operations* with real-time call/return times.
 * Linearizability (Herlihy/Wing) asks for a total order of the operations
 * that (a) respects every overlapping-or-before real-time pair and (b) is a
 * legal execution of the sequential {@link SequentialModel}.
 */

/** Millisecond-free logical clock value; only ordering matters. */
export type Time = number;

/**
 * One client-observed operation.
 *
 * @property id        unique id, assigned in call order
 * @property start     call time (inclusive); linearization point >= start
 * @property end       return time (inclusive); linearization point <= end
 * @property client    client that issued the operation (diagnostics)
 * @property node      node that returned the result (diagnostics)
 * @property input     model-specific request payload
 * @property output    model-specific response payload
 */
export interface Operation<I = unknown, O = unknown> {
  id: number;
  start: Time;
  end: Time;
  client: number;
  node: number | null;
  input: I;
  output: O;
}

/**
 * A sequential specification.  States are immutable: `step` returns the
 * successor state (or null when `output` is not a legal response in `state`),
 * which makes backtracking allocation-only.
 */
export interface SequentialModel<S, I, O> {
  initial(): S;
  /** Legal transition, or null if this operation cannot linearize in state. */
  step(state: S, input: I, output: O): S | null;
  /** Human-readable rendering for violation reports. */
  describeState(state: S): string;
  describeInput(input: I): string;
  describeOutput(output: O): string;
}

/** Output for the built-in key-value model: null means "key absent". */
export interface KvPut {
  op: "put";
  key: string;
  value: string;
}
export interface KvGet {
  op: "get";
  key: string;
}
export type KvInput = KvPut | KvGet;
export type KvOutput = string | null;

/** The operation interval plus why it mattered, for violation reports. */
export interface OpInterval<I = unknown, O = unknown> {
  op: Operation<I, O>;
  /** What the model requires in the state just before this op, if known. */
  note?: string;
}

export interface Linearizable {
  ok: true;
}

export interface Violation<I = unknown, O = unknown> {
  ok: false;
  kind:
    | "non-sequential" // no legal total order exists
    | "bad-history"; // e.g. end < start
  message: string;
  /** The operations participating in the failed ordering attempt. */
  witness: OpInterval<I, O>[];
  /** Prefix of the attempted linearization that was legal. */
  attemptedOrder: OpInterval<I, O>[];
  /** Operations still in flight when the attempt got stuck. */
  inFlight: OpInterval<I, O>[];
  /** Time (in simulation clock units) at which the attempt got stuck. */
  atTime: Time;
}

export type CheckResult<I = unknown, O = unknown> =
  | Linearizable
  | Violation<I, O>;
