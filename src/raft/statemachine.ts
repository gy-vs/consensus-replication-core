import type { PutCmd } from './types.js';

/**
 * 复制状态机接口。Raft 内核只负责把一串有序命令复制到各节点；
 * 命令的具体语义由实现方提供（示例实现见 KvStateMachine）。
 */
export interface StateMachine<C, S, Q = string> {
  /** 应用一条命令，返回给客户端的结果字符串。 */
  apply(cmd: C): string;
  /** 执行一次只读查询（ReadIndex 保证查询时状态机至少同步到读时间点）。 */
  query(q: Q): string | null;
  /** 序列化当前状态（写入快照），输出必须能被 hydrate 还原。 */
  serialize(): string;
  /** 从快照字节重建状态。 */
  hydrate(data: string): void;
  /** 深拷贝当前状态（快照在内存中要与后续 apply 隔离）。 */
  cloneState(): S;
}

/** 简单的多键 KV 状态机，作为示例与仿真负载。 */
export class KvStateMachine implements StateMachine<PutCmd, Record<string, string>, string> {
  private kv: Record<string, string> = {};

  apply(cmd: PutCmd): string {
    this.kv[cmd.key] = cmd.value;
    return 'ok';
  }

  query(key: string): string | null {
    return Object.prototype.hasOwnProperty.call(this.kv, key) ? this.kv[key] : null;
  }

  serialize(): string {
    return JSON.stringify(this.kv);
  }

  hydrate(data: string): void {
    this.kv = data.length === 0 ? {} : (JSON.parse(data) as Record<string, string>);
  }

  cloneState(): Record<string, string> {
    return JSON.parse(JSON.stringify(this.kv)) as Record<string, string>;
  }
}
