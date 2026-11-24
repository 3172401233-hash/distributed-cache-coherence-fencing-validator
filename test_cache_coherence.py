# -*- coding: utf-8 -*-
"""缓存一致性验证器测试：分区合并、晚到失效、重复写入、多区域读。"""

import unittest

from cache_coherence import simulate


def base_scenario():
    return {
        "regions": ["r1", "r2"],
        "ttl": None,
        "writes": [],
        "invalidations": [],
        "reads": [],
        "partitions": [],
    }


class PartitionMergeTest(unittest.TestCase):
    """分区恢复后，排队的旧失效消息不得删除恢复前写入的新版本。"""

    def test_partition_merge_stale_invalidation_blocked(self):
        sc = base_scenario()
        sc["writes"] = [
            {"key": "k", "value": "v1", "version": 1, "token": 10,
             "time": 0, "regions": ["r1"]},
            {"key": "k", "value": "v2", "version": 2, "token": 20,
             "time": 5, "regions": ["r1"]},
            {"key": "k", "value": "v2", "version": 2, "token": 20,
             "time": 8, "regions": ["r2"]},
        ]
        sc["invalidations"] = [
            {"id": "inv1", "key": "k", "version": 1, "token": 10,
             "time": 1, "targets": ["r2"]},
        ]
        sc["partitions"] = [{"regions": ["r2"], "start": 1, "end": 8}]
        sc["reads"] = [{"region": "r2", "key": "k", "time": 10}]
        res = simulate(sc)
        self.assertEqual(res["final_cache"]["r2"]["k"]["value"], "v2")
        self.assertEqual(res["final_cache"]["r2"]["k"]["version"], 2)
        self.assertEqual(res["wrongly_deleted"], [])
        self.assertEqual(len(res["protected"]), 1)
        self.assertEqual(res["protected"][0]["by_message"], "inv1")
        self.assertEqual(res["recovery_order"], {"r2@8": ["inv1"]})
        self.assertEqual(res["visible_reads"][0]["value"], "v2")

    def test_recovery_order_sorted_by_token(self):
        sc = base_scenario()
        sc["invalidations"] = [
            {"id": "inv_new", "key": "a", "version": 3, "token": 30,
             "time": 2, "targets": ["r2"]},
            {"id": "inv_old", "key": "a", "version": 1, "token": 10,
             "time": 3, "targets": ["r2"]},
            {"id": "inv_mid", "key": "a", "version": 2, "token": 20,
             "time": 4, "targets": ["r2"]},
        ]
        sc["partitions"] = [{"regions": ["r2"], "start": 1, "end": 6}]
        res = simulate(sc)
        self.assertEqual(res["recovery_order"]["r2@6"],
                         ["inv_old", "inv_mid", "inv_new"])


class LateInvalidationTest(unittest.TestCase):
    """旧 fencing token 的失效消息晚到，不能删除新版本缓存。"""

    def test_late_invalidation_cannot_delete_newer_version(self):
        sc = base_scenario()
        sc["writes"] = [
            {"key": "k", "value": "v1", "version": 1, "token": 10, "time": 0},
            {"key": "k", "value": "v2", "version": 2, "token": 20, "time": 2},
        ]
        sc["invalidations"] = [
            {"id": "inv1", "key": "k", "version": 1, "token": 10,
             "time": 3, "targets": ["r1", "r2"]},
        ]
        res = simulate(sc)
        for region in ("r1", "r2"):
            self.assertEqual(res["final_cache"][region]["k"]["value"], "v2")
        self.assertEqual(res["wrongly_deleted"], [])
        self.assertEqual(len(res["protected"]), 2)

    def test_naive_cache_would_wrongly_delete(self):
        sc = base_scenario()
        sc["writes"] = [
            {"key": "k", "value": "v1", "version": 1, "token": 10, "time": 0},
            {"key": "k", "value": "v2", "version": 2, "token": 20, "time": 2},
        ]
        sc["invalidations"] = [
            {"id": "inv1", "key": "k", "version": 1, "token": 10,
             "time": 3, "targets": ["r1", "r2"]},
        ]
        res = simulate(sc, fencing_enabled=False)
        self.assertEqual(res["final_cache"]["r1"], {})
        self.assertEqual(res["final_cache"]["r2"], {})
        self.assertEqual(len(res["wrongly_deleted"]), 2)
        self.assertEqual(res["wrongly_deleted"][0]["token"], 20)
        self.assertEqual(res["wrongly_deleted"][0]["msg_token"], 10)

    def test_matching_invalidation_still_deletes(self):
        sc = base_scenario()
        sc["writes"] = [
            {"key": "k", "value": "v1", "version": 1, "token": 10, "time": 0},
        ]
        sc["invalidations"] = [
            {"id": "inv1", "key": "k", "version": 1, "token": 10,
             "time": 1, "targets": ["r1"]},
        ]
        res = simulate(sc)
        self.assertEqual(res["final_cache"]["r1"], {})
        self.assertEqual(res["final_cache"]["r2"]["k"]["value"], "v1")
        self.assertEqual(res["wrongly_deleted"], [])
        self.assertEqual(res["protected"], [])


class DuplicateWriteTest(unittest.TestCase):
    """重复/乱序写入：旧 fencing token 的写入与回填一律被拒绝。"""

    def test_stale_duplicate_write_rejected(self):
        sc = base_scenario()
        sc["writes"] = [
            {"key": "k", "value": "v2", "version": 2, "token": 20, "time": 0},
            {"key": "k", "value": "v1", "version": 1, "token": 10, "time": 1},
        ]
        res = simulate(sc)
        self.assertEqual(len(res["rejected_writes"]), 1)
        self.assertEqual(res["rejected_writes"][0]["token"], 10)
        for region in ("r1", "r2"):
            self.assertEqual(res["final_cache"][region]["k"]["value"], "v2")
            self.assertEqual(res["final_cache"][region]["k"]["token"], 20)

    def test_equal_token_write_rejected(self):
        sc = base_scenario()
        sc["writes"] = [
            {"key": "k", "value": "v1", "version": 1, "token": 10, "time": 0},
            {"key": "k", "value": "v1b", "version": 1, "token": 10, "time": 1},
        ]
        res = simulate(sc)
        self.assertEqual(len(res["rejected_writes"]), 1)
        self.assertEqual(res["final_cache"]["r1"]["k"]["value"], "v1")


class MultiRegionReadTest(unittest.TestCase):
    """多区域读：命中、回源、TTL 过期与过期读回填限制。"""

    def test_reads_across_regions(self):
        sc = base_scenario()
        sc["writes"] = [
            {"key": "k", "value": "v1", "version": 1, "token": 10,
             "time": 0, "regions": ["r1"]},
        ]
        sc["reads"] = [
            {"region": "r1", "key": "k", "time": 1},
            {"region": "r2", "key": "k", "time": 1},
        ]
        res = simulate(sc)
        r1_read, r2_read = res["visible_reads"]
        self.assertEqual((r1_read["source"], r1_read["value"]),
                         ("cache", "v1"))
        self.assertEqual((r2_read["source"], r2_read["value"]),
                         ("store", "v1"))
        self.assertEqual(res["final_cache"]["r2"]["k"]["value"], "v1")

    def test_expired_entry_not_resurrected_by_read(self):
        sc = base_scenario()
        sc["ttl"] = 5
        sc["writes"] = [
            {"key": "k", "value": "v1", "version": 1, "token": 10, "time": 0},
            {"key": "k", "value": "v2", "version": 2, "token": 20,
             "time": 1, "regions": []},
        ]
        sc["reads"] = [{"region": "r1", "key": "k", "time": 6}]
        res = simulate(sc)
        read = res["visible_reads"][0]
        self.assertEqual(read["source"], "store")
        self.assertEqual(read["value"], "v2")
        self.assertEqual(res["final_cache"]["r1"]["k"]["version"], 2)

    def test_stale_read_cannot_bypass_via_refill(self):
        sc = base_scenario()
        sc["writes"] = [
            {"key": "k", "value": "v1", "version": 1, "token": 10, "time": 0},
            {"key": "k", "value": "v2", "version": 2, "token": 20, "time": 2},
        ]
        sc["invalidations"] = [
            {"id": "inv2", "key": "k", "version": 2, "token": 20,
             "time": 3, "targets": ["r2"]},
        ]
        sc["reads"] = [
            {"region": "r2", "key": "k", "time": 4,
             "fill": {"value": "v1", "version": 1, "token": 10}},
        ]
        res = simulate(sc)
        self.assertEqual(res["final_cache"]["r2"], {})
        self.assertEqual(len(res["rejected_fills"]), 1)
        self.assertEqual(res["visible_reads"][0]["source"], "rejected-fill")
        self.assertIsNone(res["visible_reads"][0]["value"])


if __name__ == "__main__":
    unittest.main()
