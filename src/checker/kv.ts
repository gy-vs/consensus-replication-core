/**
 * Built-in sequential models.
 */
import type { KvInput, KvOutput, SequentialModel } from "./model.js";

export type { KvInput, KvOutput };

/**
 * Register/key-value store:
 *   put(k,v) -> "ok"   always legal, installs v
 *   get(k)   -> v      legal iff v is the current value (or null if absent)
 */
export function keyValueModel(): SequentialModel<
  Map<string, string>,
  KvInput,
  KvOutput
> {
  return {
    initial: () => new Map(),
    step(state, input, output) {
      if (input.op === "put") {
        if (output !== "ok") return null;
        const next = new Map(state);
        next.set(input.key, input.value);
        return next;
      }
      const current = state.has(input.key) ? state.get(input.key)! : null;
      if (output !== current) return null;
      return state;
    },
    describeState(state) {
      if (state.size === 0) return "{}";
      const pairs = [...state.entries()]
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .map(([k, v]) => `${k}=${v}`);
      return `{${pairs.join(", ")}}`;
    },
    describeInput(i) {
      return i.op === "put" ? `put(${i.key}, ${i.value})` : `get(${i.key})`;
    },
    describeOutput(o) {
      return o === null ? "<absent>" : o;
    },
  };
}

/** Single-register convenience model (one key, string value). */
export function registerModel(): SequentialModel<
  string | null,
  { op: "write"; value: string } | { op: "read" },
  string | null
> {
  return {
    initial: () => null,
    step(state, input, output) {
      if (input.op === "write") {
        if (output !== "ok") return null;
        return input.value;
      }
      return output === state ? state : null;
    },
    describeState: (s) => (s === null ? "<absent>" : s),
    describeInput: (i) => (i.op === "write" ? `write(${i.value})` : "read"),
    describeOutput: (o) => (o === null ? "<absent>" : o),
  };
}
