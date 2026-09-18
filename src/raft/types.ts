/**
 * Raft 复制内核的公共类型定义。
 *
 * 本模块只定义类型，不包含算法逻辑。Raft 算法实现见 node.ts。
 */

// ---------------------------------------------------------------------------
// 集群配置
// ---------------------------------------------------------------------------

/**
 * 一个投票配置。
 *
 * - 稳定配置：oldSet 为全部投票成员，newSet 为 null。
 * - 联合配置（joint consensus 进行中）：oldSet / newSet 分别保存新旧两套投票成员，
 *   任何需要多数派的决策（选举投票、日志提交、读确认）都必须同时在两个集合中获得多数。
 *   这样在配置切换期间，任何两个多数派一定相交，不可能出现两组节点各自宣布多数派。
 */
export interface Config {
  oldSet: ReadonlyArray<string>;
  /** 联合配置中的新成员集合；null 表示当前为稳定配置。 */
  newSet: ReadonlyArray<string> | null;
}

// ---------------------------------------------------------------------------
// 日志条目与快照
// ---------------------------------------------------------------------------

/** 客户端写入命令（KV 示例）。 */
export interface PutCmd {
  kind: 'put';
  key: string;
  value: string;
  /** 会话去重：客户 id + 该客户的单调序号，同一命令重试得到同一个结果。 */
  clientId: string;
  seq: number;
}

export type ClientCommand = PutCmd;

/**
 * 一条日志记录。
 *
 * 一条记录要么携带客户端命令（cmd），要么携带配置变更（configEntry），
 * 要么两者都没有（新领导人上任时写入的 no-op，用于推进 commitIndex）。
 */
export interface Entry<C = ClientCommand> {
  term: number;
  index: number;
  cmd?: C;
  configEntry?: ConfigEntry;
}

/** 配置变更日志条目：联合配置或稳定配置。 */
export type ConfigEntry =
  | { phase: 'joint'; joint: Config }
  | { phase: 'stable'; config: Config };

/**
 * 快照。快照覆盖到 lastIncludedIndex（含），安装快照后该位置及之前的日志全部丢弃。
 */
export interface Snapshot<C = ClientCommand> {
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  /** 应用层状态机在 lastIncludedIndex 处的状态（由 StateMachine.serialize 产生）。 */
  state: unknown;
  /** 快照点的最新配置（快照中可能包含配置条目，用于落后节点追赶配置）。 */
  config: Config;
  /** 快照点已完成的客户端命令去重表，重启后命令幂等性依赖它，因此必须随快照落盘。 */
  clients: Readonly<Record<string, { lastSeq: number; lastResult: string }>>;
}

// ---------------------------------------------------------------------------
// 节点持久化与易失状态的存储接口
// ---------------------------------------------------------------------------

/**
 * 持久化存储接口。真实实现中每个方法对应一次 fsync（或等价的持久化屏障）。
 *
 * 安全性边界（节点代码严格遵守，检查时照此核对）：
 *   1. 节点在“回复任何消息”或“向客户端确认写入”之前，所依赖的
 *      term/vote / 新日志条目 / 快照必须已经经过下面这些方法之一落盘。
 *   2. 节点的内存状态在崩溃后全部丢失；恢复时只允许依赖 load() 读出的内容。
 *
 * MemoryStorage 是测试/仿真用实现；真实介质由使用者自行接入。
 */
export interface Storage<C = ClientCommand> {
  /** 启动 / 重启时读出全部持久化状态。空存储返回 null，节点据此完成首次初始化。 */
  load(): Persisted<C> | null;

  /**
   * 落盘【硬状态】：当前任期 currentTerm 与本轮任期内的投票 votedFor。
   * 节点在回复 RequestVote / PreVote 后立即走这里（PreVote 不投票，不落盘）。
   */
  saveHardState(currentTerm: number, votedFor: string | null): void;

  /**
   * 落盘日志：截断到 prefixIndex（保留 log[0..prefixIndex]），再写入 entries。
   * 节点在追加/覆盖日志之后、回复 AppendEntries 成功之前必须走到这里。
   */
  replaceSuffix(prefixIndex: number, entries: ReadonlyArray<Entry<C>>): void;

  /**
   * 原子落盘一份快照（覆盖旧快照），并丢弃 index <= snap.lastIncludedIndex 的日志。
   * 节点在 InstallSnapshot 完成、回复成功之前必须走到这里。
   */
  installSnapshot(snap: Snapshot<C>): void;
}

/** load() 返回的持久化状态视图。 */
export interface Persisted<C = ClientCommand> {
  currentTerm: number;
  votedFor: string | null;
  /** 保留的日志后缀；其第一条的索引由 baseIndex 给出。 */
  baseIndex: number;
  log: ReadonlyArray<Entry<C>>;
  /** 尚无快照时为 null。 */
  snapshot: Snapshot<C> | null;
}

// ---------------------------------------------------------------------------
// 节点对外环境依赖（传输、时钟、随机数全部由外部注入）
// ---------------------------------------------------------------------------

/** 节点运行环境：仿真器实现它；接入真实系统时换成真实时钟/传输即可。 */
export interface Env {
  /** 当前逻辑时间（毫秒，单调递增）。 */
  now(): number;
  /** 返回 [0, 1) 的伪随机数（确定性由实现保证）。 */
  random(): number;
  /** 请求环境在 deadlineMs 时刻唤醒本节点（到期会调用 node.wake()）。 */
  wakeAt(deadlineMs: number): void;
  /** 向对端发送一条消息（可能丢失/重复/延迟，由环境决定）。 */
  send(to: string, msg: RaftMessage): void;
  /** 输出节点事件（客户端结果、领导人变化、应用日志等）。 */
  emit(event: NodeEvent): void;
}

/** 客户端操作引用：调用方在 propose/read/changeConfig 时传入，结果事件原样带回。 */
export interface OpRef {
  clientId: string;
  seq: number;
}

// ---------------------------------------------------------------------------
// 节点产生的事件
// ---------------------------------------------------------------------------

export type NodeEvent =
  | { type: 'clientResult'; ref: OpRef; status: OpStatus; result?: string }
  | { type: 'readResult'; ref: OpRef; status: OpStatus; result?: string }
  | { type: 'configResult'; ref: OpRef; status: OpStatus; config: Config }
  | { type: 'leaderChange'; leaderId: string | null; term: number }
  | { type: 'applied'; index: number; term: number; cmd?: ClientCommand };

export type OpStatus = 'ok' | 'retry' | 'not-leader';

// ---------------------------------------------------------------------------
// Raft RPC 消息
// ---------------------------------------------------------------------------

export type RaftMessage =
  | PreVoteRequest
  | PreVoteResponse
  | VoteRequest
  | VoteResponse
  | AppendEntriesRequest
  | AppendEntriesResponse
  | InstallSnapshotRequest
  | InstallSnapshotResponse;

interface BaseMsg {
  from: string;
  term: number;
}

/** PreVote（第 9.6 节防止新节点因过期 term 反复打断在任领导人的优化，不增加持久化）。 */
export interface PreVoteRequest extends BaseMsg {
  kind: 'preVote';
  lastLogIndex: number;
  lastLogTerm: number;
}
export interface PreVoteResponse extends BaseMsg {
  kind: 'preVoteResp';
  granted: boolean;
}

export interface VoteRequest extends BaseMsg {
  kind: 'vote';
  lastLogIndex: number;
  lastLogTerm: number;
}
export interface VoteResponse extends BaseMsg {
  kind: 'voteResp';
  granted: boolean;
}

export interface AppendEntriesRequest extends BaseMsg {
  kind: 'appendEntries';
  prevLogIndex: number;
  prevLogTerm: number;
  entries: ReadonlyArray<Entry>;
  leaderCommit: number;
  /** ReadIndex：领导人发起的读确认心跳带回读 id，对端确认即代表读 quorum 一票。 */
  readId: number | null;
}

export interface AppendEntriesResponse extends BaseMsg {
  kind: 'appendEntriesResp';
  success: boolean;
  /** 成功：对端与领导人匹配到的位置；失败：冲突信息，供领导人回退 nextIndex。 */
  matchIndex: number;
  conflictTerm: number | null;
  conflictIndex: number;
  readId: number | null;
}

/** InstallSnapshot 分块传输的一块。 */
export interface InstallSnapshotRequest extends BaseMsg {
  kind: 'installSnapshot';
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  config: Config;
  /** 序列化后的状态机快照字节（base64）。 */
  stateChunk: string;
  /** 客户端去重表快照字节（base64）。 */
  clientsChunk: string;
  offset: number;
  /** 本块之后是否还有后续块。 */
  done: boolean;
}

export interface InstallSnapshotResponse extends BaseMsg {
  kind: 'installSnapshotResp';
  lastIncludedIndex: number;
  success: boolean;
  offset: number;
}
