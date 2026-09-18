import type { Entry, Persisted, Snapshot, Storage } from './types.js';

/**
 * 进程内持久化存储实现（仿真用）。
 *
 * 重要语义：
 *  - 节点崩溃时 RaftNode 内存对象被销毁，但本对象保留（模拟掉电不丢盘）；
 *    重启时新建 RaftNode，从 load() 恢复。要模拟“盘也没了”的成员，
 *    调用方直接丢掉本对象、换一个新实例即可（见仿真器 addNode 的处理）。
 *  - 每个写方法都是一个明确的 fsync 边界：调用返回即视为已落盘，
 *    RaftNode 只有在这些方法返回之后才允许对外发消息/回客户端。
 */
export class MemoryStorage<C> implements Storage<C> {
  private currentTerm = 0;
  private votedFor: string | null = null;
  private snapshot: Snapshot<C> | null = null;
  /** 保留日志后缀；baseIndex 是 log[0] 前一条的索引（无快照时为 0）。 */
  private baseIndex = 0;
  private log: Array<Entry<C>> = [];

  /** fsync 计数（测试用来核对落盘边界）。 */
  fsyncs = 0;

  load(): Persisted<C> | null {
    // 从未初始化：空盘
    if (this.currentTerm === 0 && this.votedFor === null && this.log.length === 0 && this.snapshot === null) {
      return null;
    }
    return {
      currentTerm: this.currentTerm,
      votedFor: this.votedFor,
      baseIndex: this.baseIndex,
      log: this.log,
      snapshot: this.snapshot,
    };
  }

  saveHardState(currentTerm: number, votedFor: string | null): void {
    // fsync 边界：任期 / 投票持久化（先于投票响应）
    this.currentTerm = currentTerm;
    this.votedFor = votedFor;
    this.fsyncs++;
  }

  replaceSuffix(prefixIndex: number, entries: ReadonlyArray<Entry<C>>): void {
    // fsync 边界：日志截断 + 追加（先于 AppendEntries 成功响应）
    if (prefixIndex < this.baseIndex - 1) {
      throw new Error(`replaceSuffix prefix ${prefixIndex} below base ${this.baseIndex}`);
    }
    // log[k].index == baseIndex + 1 + k，保留 index <= prefixIndex 的条目
    const keep = Math.max(0, prefixIndex - this.baseIndex);
    this.log = this.log.slice(0, keep).concat(entries.map((e) => ({ ...e })));
    this.fsyncs++;
  }

  installSnapshot(snap: Snapshot<C>): void {
    // fsync 边界：快照原子落盘（先于 InstallSnapshot 成功响应）
    this.snapshot = JSON.parse(JSON.stringify(snap)) as Snapshot<C>;
    this.baseIndex = snap.lastIncludedIndex;
    // 丢弃快照覆盖的日志；严格晚于快照点的日志保留（论文允许）
    this.log = this.log.filter((e) => e.index > snap.lastIncludedIndex);
    this.fsyncs++;
  }

  /** 仅供测试：查看 fsync 次数。 */
  fsyncCount(): number {
    return this.fsyncs;
  }
}
