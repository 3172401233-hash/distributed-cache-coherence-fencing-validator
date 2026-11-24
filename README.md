# distributed-cache-coherence-fencing-validator
验证分区缓存失效、fencing 和版本收敛

## 场景

两个区域同时缓存同一对象，写入端只广播带版本的失效消息。网络分区恢复后，
旧版本失效消息晚到，可能把新缓存当作旧值删除。本仓库用纯 Python 模拟该过程，
验证 fencing token 与版本号能否保证缓存一致性：

- 旧 fencing token 不能删除新版本缓存；
- 过期读不能通过重新填缓存绕过失效语义；
- 分区恢复时按 (token, version) 升序应用排队消息，给出安全恢复顺序。

## 本地模型说明

**所有缓存均为本地模型**：区域缓存（`RegionCache`）、权威存储（`Store`）、
消息队列与网络分区都是单进程内的内存对象，不依赖真实网络、Redis 或任何
外部服务。事件按逻辑时钟（`time` 字段）排序回放，仅用于一致性语义验证。

## 输入

`simulate(scenario)` 接受一个字典：

- `regions`：区域名列表，如 `["r1", "r2"]`
- `ttl`：默认缓存 TTL（逻辑时钟单位，`None` 表示不过期）
- `writes`：`[{key, value, version, token, time, ttl?, regions?}]`，
  携带 fencing token 与版本号；token 相同且内容一致的重复写视为延迟重广播
- `invalidations`：`[{id, key, version, token, time, targets}]`，
  写入端广播的带版本失效消息
- `reads`：`[{region, key, time, fill?}]`，`fill` 用于模拟慢读/过期读回填
- `partitions`：`[{regions, start, end}]`，分区期间消息排队，恢复时投递

## 输出

- `final_cache`：各区域最终有效缓存（值、版本、token、过期时刻）
- `visible_reads`：每次读请求的可见值与来源（cache/store/miss/refill/rejected-fill）
- `wrongly_deleted`：被错误删除的缓存项（旧 token 删新版本，仅 fencing 关闭时发生）
- `protected`：被 fencing 拦截的危险删除
- `rejected_writes` / `rejected_fills`：被 fencing 拒绝的写入与回填
- `recovery_order`：分区恢复时的安全消息应用顺序（按 token/version 升序）

## 运行测试

兼容 Python 3.8.2+：

```
python -m unittest
```

测试覆盖：分区合并、晚到失效、重复写入、多区域读（含 TTL 过期与过期读回填限制）。
