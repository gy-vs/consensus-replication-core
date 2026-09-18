/** 时间与批处理常量，单位均为逻辑毫秒（不接触真实定时器）。 */
export const HEARTBEAT_INTERVAL = 20;
/** 选举超时区间，每个节点在重启/收到合法心跳后随机取一个值。 */
export const ELECTION_TIMEOUT_MIN = 80;
export const ELECTION_TIMEOUT_MAX = 150;
/** PreVote/RequestVote 等待期内重广播的周期。 */
export const ELECTION_RETRY = 50;
/** 领导人每次 AppendEntries 携带的最大条目数（迫使多轮传输，便于仿真）。 */
export const MAX_ENTRIES_PER_APPEND = 32;
/** InstallSnapshot 每块字节数。 */
export const SNAPSHOT_CHUNK_BYTES = 4096;
/** 落后节点快照重传周期（块确认丢失时）。 */
export const SNAPSHOT_RESEND = 60;
/** 应用多少条日志后做一次快照压缩。 */
export const SNAPSHOT_INTERVAL = 64;
/** 快照之后保留的日志条数（给正常追赶的节点留窗口，只有更落后的才需要快照）。 */
export const SNAPSHOT_TAIL = 32;

/**
 * 确定性 PRNG：mulberry32。
 * 仿真器全程只持有这一个随机源；同一 seed 下调用顺序一致 ⇒ 事件序列一致。
 */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** 区间 [min, max) 的整数随机数。 */
export function randInt(rng: () => number, min: number, max: number): number {
  return min + Math.floor(rng() * (max - min));
}

/** 从数组中等概率挑一个元素。 */
export function pick<T>(rng: () => number, xs: ReadonlyArray<T>): T {
  const x = xs[Math.floor(rng() * xs.length)];
  if (x === undefined) throw new Error('pick from empty');
  return x;
}

/** 深拷贝（快照内容必须与内存状态解耦）。 */
export function deepClone<T>(v: T): T {
  return JSON.parse(JSON.stringify(v)) as T;
}

/** 把字符串编码为 base64（快照块传输用，真实接入时可换成二进制通道）。 */
export function toBase64(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
}
export function fromBase64(s: string): string {
  return Buffer.from(s, 'base64').toString('utf8');
}
