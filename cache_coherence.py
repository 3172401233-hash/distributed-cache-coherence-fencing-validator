"""分布式缓存一致性验证器（本地模型）。

模拟两个（或多个）区域缓存同一对象、写入端广播带版本的失效消息的场景：
- 写入携带单调递增的 fencing token 与版本号；
- 失效消息可能因网络分区延迟，分区恢复后旧版本失效消息晚到；
- 旧 fencing token 不能删除新版本缓存；
- 过期读不能通过重新填缓存绕过失效语义。

所有缓存均为进程内本地模型，不涉及真实网络或外部存储。
兼容 Python 3.8.2+。
"""

from collections import defaultdict


class CacheEntry(object):
    """单个区域中单个 key 的缓存项。"""

    __slots__ = ("value", "version", "token", "expires_at")

    def __init__(self, value, version, token, expires_at):
        self.value = value
        self.version = version
        self.token = token
        self.expires_at = expires_at

    def is_expired(self, now):
        return self.expires_at is not None and now >= self.expires_at

    def snapshot(self):
        return {
            "value": self.value,
            "version": self.version,
            "token": self.token,
            "expires_at": self.expires_at,
        }


class RegionCache(object):
    """一个区域的本地缓存模型（cache-aside）。"""

    def __init__(self, name, fencing_enabled=True):
        self.name = name
        self.fencing_enabled = fencing_enabled
        self.entries = {}          # key -> CacheEntry
        self.max_token = {}        # key -> 本区域见过的最大 fencing token
        self.min_fill_token = {}   # key -> 重新填缓存允许的最低 token（失效屏障）

    def _token_floor(self, key):
        return max(self.max_token.get(key, -1),
                   self.min_fill_token.get(key, -1))

    def fill(self, key, value, version, token, expires_at, now):
        """写入/读回填充缓存。旧 token 不得覆盖新 token。"""
        if self.fencing_enabled and token < self._token_floor(key):
            return False
        current = self.entries.get(key)
        if (self.fencing_enabled and current is not None
                and not current.is_expired(now)
                and token < current.token):
            return False
        self.entries[key] = CacheEntry(value, version, token, expires_at)
        if token > self.max_token.get(key, -1):
            self.max_token[key] = token
        return True

    def invalidate(self, key, msg_version, msg_token, now):
        """处理失效消息。

        返回 (deleted, entry_snapshot_or_None)。
        开启 fencing 时：消息 token 低于缓存项 token（旧 token 删新版本）
        或消息版本低于缓存项版本（晚到的旧失效）时拒绝删除。
        """
        entry = self.entries.get(key)
        if entry is None or entry.is_expired(now):
            # 即使无缓存项，失效仍抬升填缓存屏障，防止过期读回填旧值。
            if msg_token > self.min_fill_token.get(key, -1):
                self.min_fill_token[key] = msg_token
            return False, None
        if self.fencing_enabled:
            if msg_token < entry.token or msg_version < entry.version:
                return False, entry.snapshot()
        deleted = entry.snapshot()
        del self.entries[key]
        if msg_token > self.min_fill_token.get(key, -1):
            self.min_fill_token[key] = msg_token
        if msg_token > self.max_token.get(key, -1):
            self.max_token[key] = msg_token
        return True, deleted

    def read(self, key, now):
        """读缓存。过期项视为未命中，且不得因读取而复活。"""
        entry = self.entries.get(key)
        if entry is None:
            return None
        if entry.is_expired(now):
            del self.entries[key]
            return None
        return entry.snapshot()


class Store(object):
    """权威存储（本地模型），写入端带 fencing token。"""

    def __init__(self):
        self.data = {}  # key -> {"value","version","token"}

    def write(self, key, value, version, token):
        """旧 fencing token 的写入被拒绝，返回是否接受。"""
        current = self.data.get(key)
        if current is not None and token <= current["token"]:
            return False
        self.data[key] = {"value": value, "version": version, "token": token}
        return True

    def get(self, key):
        return self.data.get(key)


def _is_partitioned(partitions, region, now):
    for p in partitions:
        if region in p["regions"] and p["start"] <= now < p["end"]:
            return True
    return False


def simulate(scenario, fencing_enabled=True):
    """运行场景，返回最终可见状态。

    scenario 字段：
      regions:        区域名列表
      writes:         [{key,value,version,token,time,ttl,regions?}]
      invalidations:  [{id,key,version,token,time,targets}]
      reads:          [{region,key,time,fill?{value,version,token,ttl}}]
      partitions:     [{regions:[...],start,end}]
    """
    regions = list(scenario.get("regions", []))
    ttl_default = scenario.get("ttl")
    partitions = list(scenario.get("partitions", []))
    caches = {r: RegionCache(r, fencing_enabled) for r in regions}
    store = Store()

    # 失效消息在分区期间排队，恢复时投递。
    pending = defaultdict(list)   # region -> [msg]

    # 统一事件流：(time, 优先级, 序号, 事件)
    # 优先级：写 0 < 失效/恢复 1 < 读 2，保证同一时刻写先于读可见。
    events = []
    seq = [0]

    def push(time, prio, kind, payload):
        events.append((time, prio, seq[0], kind, payload))
        seq[0] += 1

    for w in scenario.get("writes", []):
        push(w["time"], 0, "write", w)
    for m in scenario.get("invalidations", []):
        push(m["time"], 1, "invalidate", m)
    for r in scenario.get("reads", []):
        push(r["time"], 2, "read", r)
    for p in partitions:
        push(p["end"], 1, "heal", p)
    events.sort()

    visible_reads = []
    wrongly_deleted = []   # 违反 fencing 的删除（fencing 关闭时才会发生）
    protected = []         # 被 fencing 拦截的危险删除
    rejected_writes = []
    rejected_fills = []
    recovery_order = {}    # "区域@恢复时刻" -> 安全应用的消息 id 顺序

    def deliver(region, msg, now):
        cache = caches[region]
        deleted, snap = cache.invalidate(
            msg["key"], msg["version"], msg["token"], now)
        if deleted:
            if msg["token"] < snap["token"] or msg["version"] < snap["version"]:
                wrongly_deleted.append({
                    "region": region,
                    "key": msg["key"],
                    "version": snap["version"],
                    "token": snap["token"],
                    "by_message": msg["id"],
                    "msg_version": msg["version"],
                    "msg_token": msg["token"],
                })
        elif snap is not None and (
                msg["token"] < snap["token"]
                or msg["version"] < snap["version"]):
            protected.append({
                "region": region,
                "key": msg["key"],
                "version": snap["version"],
                "token": snap["token"],
                "by_message": msg["id"],
                "msg_version": msg["version"],
                "msg_token": msg["token"],
            })

    for time, prio, _, kind, payload in events:
        if kind == "write":
            ok = store.write(payload["key"], payload["value"],
                             payload["version"], payload["token"])
            if not ok:
                current = store.get(payload["key"])
                rebroadcast = (
                    current is not None
                    and current["token"] == payload["token"]
                    and current["version"] == payload["version"]
                    and current["value"] == payload["value"])
                if rebroadcast:
                    # 同一写入的延迟重广播（如分区恢复后补投），
                    # 不改动存储，但仍填充尚未收到该写的区域缓存。
                    ttl = payload.get("ttl", ttl_default)
                    expires = None if ttl is None else time + ttl
                    targets = payload.get("regions", regions)
                    for region in targets:
                        if _is_partitioned(partitions, region, time):
                            continue
                        caches[region].fill(payload["key"],
                                            payload["value"],
                                            payload["version"],
                                            payload["token"],
                                            expires, time)
                    continue
                rejected_writes.append({
                    "key": payload["key"],
                    "version": payload["version"],
                    "token": payload["token"],
                    "time": time,
                })
                continue
            ttl = payload.get("ttl", ttl_default)
            expires = None if ttl is None else time + ttl
            targets = payload.get("regions", regions)
            for region in targets:
                if _is_partitioned(partitions, region, time):
                    continue  # 分区中的区域收不到写广播
                caches[region].fill(payload["key"], payload["value"],
                                    payload["version"], payload["token"],
                                    expires, time)
        elif kind == "invalidate":
            for region in payload["targets"]:
                if _is_partitioned(partitions, region, time):
                    pending[region].append(payload)
                else:
                    deliver(region, payload, time)
        elif kind == "heal":
            for region in payload["regions"]:
                queue = pending.get(region)
                if not queue:
                    continue
                # 安全恢复顺序：按 (token, version, 到达序) 升序应用，
                # 保证旧失效先于新失效落地，fencing 逐条拦截危险消息。
                queue.sort(key=lambda m: (m["token"], m["version"],
                                          m["time"], m["id"]))
                recovery_order["%s@%s" % (region, payload["end"])] = [
                    m["id"] for m in queue]
                for msg in queue:
                    deliver(region, msg, payload["end"])
                del pending[region]
        elif kind == "read":
            cache = caches[payload["region"]]
            hit = cache.read(payload["key"], time)
            if hit is not None:
                visible_reads.append({
                    "region": payload["region"],
                    "key": payload["key"],
                    "time": time,
                    "value": hit["value"],
                    "version": hit["version"],
                    "source": "cache",
                })
                continue
            fill = payload.get("fill")
            if fill is not None:
                # 过期/慢读试图回填：token 低于失效屏障则被拒绝，
                # 不能借重新填缓存绕过失效语义。
                ttl = fill.get("ttl", ttl_default)
                expires = None if ttl is None else time + ttl
                ok = cache.fill(payload["key"], fill["value"],
                                fill["version"], fill["token"],
                                expires, time)
                if not ok:
                    rejected_fills.append({
                        "region": payload["region"],
                        "key": payload["key"],
                        "token": fill["token"],
                        "version": fill["version"],
                        "time": time,
                    })
                    visible_reads.append({
                        "region": payload["region"],
                        "key": payload["key"],
                        "time": time,
                        "value": None,
                        "version": None,
                        "source": "rejected-fill",
                    })
                    continue
                visible_reads.append({
                    "region": payload["region"],
                    "key": payload["key"],
                    "time": time,
                    "value": fill["value"],
                    "version": fill["version"],
                    "source": "refill",
                })
                continue
            current = store.get(payload["key"])
            if current is None:
                visible_reads.append({
                    "region": payload["region"],
                    "key": payload["key"],
                    "time": time,
                    "value": None,
                    "version": None,
                    "source": "miss",
                })
            else:
                ttl = ttl_default
                expires = None if ttl is None else time + ttl
                cache.fill(payload["key"], current["value"],
                           current["version"], current["token"],
                           expires, time)
                visible_reads.append({
                    "region": payload["region"],
                    "key": payload["key"],
                    "time": time,
                    "value": current["value"],
                    "version": current["version"],
                    "source": "store",
                })

    final_cache = {}
    for name, cache in caches.items():
        final_cache[name] = {
            key: entry.snapshot() for key, entry in cache.entries.items()
        }

    return {
        "final_cache": final_cache,
        "visible_reads": visible_reads,
        "wrongly_deleted": wrongly_deleted,
        "protected": protected,
        "rejected_writes": rejected_writes,
        "rejected_fills": rejected_fills,
        "recovery_order": recovery_order,
    }
