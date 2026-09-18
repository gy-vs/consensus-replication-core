// Quick smoke driver (not a test): run one seed and summarize.
import { Simulator } from "../src/sim/index.js";
import { checkLinearizable, keyValueModel } from "../src/checker/index.js";

const seed = Number(process.argv[2] ?? 1);
const sim = new Simulator(seed, { operations: 200 });
const result = sim.run();
const check = checkLinearizable(keyValueModel(), result.history);
console.log({
  seed,
  ops: result.history.length,
  finished: result.completedOps,
  unresolved: result.unresolved,
  finalTime: result.finalTime,
  trace: result.trace.length,
  linearizable: check.ok,
});
if (!check.ok) console.log(check.message);
