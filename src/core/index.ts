/**
 * Public surface of the Raft replication core.
 *
 * The core is transport-, storage- and clock-agnostic:
 *  - {@link step} consumes one {@link Message} and returns the effects;
 *  - the host must perform {@link StepResult.disk} writes with durability
 *    BEFORE sending {@link StepResult.messages};
 *  - {@link initNode}/{@link restoreNode} cover boot and crash recovery.
 */
export { step, initNode, restoreNode } from "./node.js";
export {
  firstIndex,
  lastIndex,
  lastTerm,
  termAt,
  entryAt,
  configAt,
  latestConfig,
  replicationSet,
  quorumAt,
  majority,
  logAtLeastAsGood,
} from "./log.js";
export type {
  NodeId,
  Command,
  LogEntry,
  ConfigPayload,
  PersistentState,
  VolatileState,
  NodeState,
  StableStorage,
  Message,
  RpcRequest,
  RpcResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
  RequestVoteRequest,
  RequestVoteResponse,
  InstallSnapshotRequest,
  InstallSnapshotResponse,
  ClientWrite,
  ClientRead,
  ProposeConfigChange,
  DiskWrite,
  OutboundMessage,
  StepResult,
  ClientCompletion,
  ReadReady,
  ClientError,
  PendingRead,
} from "./types.js";
