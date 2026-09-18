/**
 * Public surface of the linearizability checker.
 *
 * It is deliberately independent of the Raft core: feed it any history of
 * call/return intervals plus any sequential specification.
 */
export { checkLinearizable } from "./linearize.js";
export type { CheckOptions } from "./linearize.js";
export { keyValueModel, registerModel } from "./kv.js";
export type {
  Operation,
  OpInterval,
  SequentialModel,
  CheckResult,
  Violation,
  Linearizable,
  Time,
  KvInput,
  KvOutput,
} from "./model.js";
