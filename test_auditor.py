#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_auditor.py — 离线审计器测试

以子进程方式运行 CLI (--json)，对独立编码的二进制夹具做断言；不启动真实 broker。
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fixture_encoder
import kafka_audit
from make_fixtures import FIXTURES

HERE = os.path.dirname(os.path.abspath(__file__))
AUDITOR = os.path.join(HERE, "kafka_audit.py")


def run_cli(path, hw, extra=()):
    proc = subprocess.run(
        [sys.executable, AUDITOR, path, "--hw", str(hw), "--json", *extra],
        capture_output=True,
    )
    out = proc.stdout.decode("utf-8")
    return proc, json.loads(out)


class TestPrimitives(unittest.TestCase):
    def test_crc32c_known_vector(self):
        # 标准测试向量: CRC32C("123456789") = 0xE3069283
        self.assertEqual(kafka_audit.crc32c(b"123456789"), 0xE3069283)
        self.assertEqual(fixture_encoder.crc32c_bitwise(b"123456789"), 0xE3069283)

    def test_crc_implementations_agree(self):
        # 审计器 (表驱动) 与编码器 (逐位) 两套独立实现必须一致
        for name, blob in FIXTURES.items():
            self.assertEqual(
                kafka_audit.crc32c(blob), fixture_encoder.crc32c_bitwise(blob), name
            )

    def test_svarint_roundtrip(self):
        for v in [0, 1, -1, 63, -64, 8192, -8192, 2**31 - 1, -(2**31), 2**60, -(2**60)]:
            enc = fixture_encoder.encode_svarint(v)
            dec, pos = kafka_audit._read_svarint(enc, 0, len(enc))
            self.assertEqual(dec, v)
            self.assertEqual(pos, len(enc))


class AuditorCLITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.paths = {}
        for name, blob in FIXTURES.items():
            p = os.path.join(cls.tmp.name, name + ".bin")
            with open(p, "wb") as fh:
                fh.write(blob)
            cls.paths[name] = p

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def audit(self, name, hw):
        return run_cli(self.paths[name], hw)

    @staticmethod
    def by_offset(res):
        return {r["offset"]: r for r in res["records"]}

    # ------------------------------------------------ 跨批提交
    def test_cross_batch_commit_full_hw(self):
        proc, res = self.audit("cross_batch_commit", 10)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [0, 1, 2, 3, 4, 5, 6, 8, 9])
        self.assertEqual(
            [res["deliverable"]["range_start"], res["deliverable"]["range_end"]],
            [0, 10],
        )
        recs = self.by_offset(res)
        self.assertEqual(recs[7]["status"], "hidden")
        self.assertEqual(recs[7]["kind"], "control")
        self.assertIn("COMMIT", recs[7]["reason"])
        for off in (2, 3, 5, 6):
            self.assertEqual(recs[off]["status"], "deliverable")
            self.assertIn("已提交", recs[off]["reason"])
        txn = res["transactions"][0]
        self.assertEqual(txn["outcome"], "committed")
        self.assertEqual(txn["first_offset"], 2)
        self.assertEqual(txn["marker_offset"], 7)
        self.assertEqual(txn["record_offsets"], [2, 3, 5, 6])
        self.assertIsNone(res["lso"])
        self.assertEqual(res["effective_limit"], 10)

    def test_cross_batch_commit_hw_before_marker(self):
        # HW 在提交标记之前：事务未决，LSO 压住在首条记录
        proc, res = self.audit("cross_batch_commit", 5)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [0, 1])
        self.assertEqual(res["lso"], 2)
        self.assertEqual(res["effective_limit"], 2)
        recs = self.by_offset(res)
        self.assertEqual(recs[2]["status"], "hidden")
        self.assertIn("未决", recs[2]["reason"])
        self.assertEqual(recs[4]["status"], "hidden")
        self.assertIn("LSO", recs[4]["reason"])
        for off in (5, 6, 7, 8, 9):
            self.assertEqual(recs[off]["status"], "unevaluated")

    # ------------------------------------------------ 中止
    def test_abort(self):
        proc, res = self.audit("abort", 7)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [3, 5, 6])
        recs = self.by_offset(res)
        for off in (0, 1, 2):
            self.assertEqual(recs[off]["status"], "hidden")
            self.assertIn("中止", recs[off]["reason"])
        self.assertEqual(recs[4]["kind"], "control")
        self.assertIn("ABORT", recs[4]["reason"])
        hidden = {h["offset"] for h in res["deliverable"]["hidden_in_range"]}
        self.assertEqual(hidden, {0, 1, 2, 4})

    # ------------------------------------------------ 开放事务压住 LSO
    def test_open_txn_holds_lso(self):
        proc, res = self.audit("open_txn", 7)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["lso"], 2)
        self.assertEqual(res["effective_limit"], 2)
        self.assertEqual(res["deliverable"]["offsets"], [0, 1])
        self.assertEqual(
            [res["deliverable"]["range_start"], res["deliverable"]["range_end"]], [0, 2]
        )
        recs = self.by_offset(res)
        for off in (2, 3, 6):
            self.assertEqual(recs[off]["status"], "hidden")
            self.assertIn("未决", recs[off]["reason"])
        for off in (4, 5):
            self.assertEqual(recs[off]["status"], "hidden")
            self.assertIn("LSO", recs[off]["reason"])

    # ------------------------------------------------ 损坏长度
    def test_corrupt_length_refuses_when_hw_beyond(self):
        proc, res = self.audit("corrupt_length", 6)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertEqual(res["scan_stop"]["reason"], "corrupt")
        self.assertIn("batchLength", res["scan_stop"]["detail"])
        self.assertEqual(len(res["batches"]), 1)  # 只有批次 0 通过验证
        self.assertTrue(any("超过已验证区域" in e for e in res["errors"]))

    def test_corrupt_length_partial_delivery_before_damage(self):
        # 损坏点在 HW 之后：已验证前缀内的交付仍然有效
        proc, res = self.audit("corrupt_length", 2)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [0, 1])
        self.assertTrue(res["warnings"])

    # ------------------------------------------------ 尾部不完整
    def test_truncated_tail_marked(self):
        proc, res = self.audit("truncated_tail", 4)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["scan_stop"]["reason"], "incomplete-tail")
        self.assertTrue(any("尾部不完整" in w for w in res["warnings"]))
        self.assertEqual(res["deliverable"]["offsets"], [0, 1, 2, 3])

    def test_truncated_tail_hw_beyond_verified(self):
        proc, res = self.audit("truncated_tail", 5)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("超过已验证区域" in e for e in res["errors"]))

    # ------------------------------------------------ 中段 CRC 失败禁止跳过
    def test_crc_failure_stops_no_skip(self):
        proc, res = self.audit("crc_failure", 6)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertEqual(res["scan_stop"]["reason"], "corrupt")
        self.assertIn("CRC32C", res["scan_stop"]["detail"])
        self.assertEqual(len(res["batches"]), 1)
        # 禁止跳过损坏批次继续扫描：offset 3/4 的记录不得出现
        self.assertEqual(sorted(self.by_offset(res)), [0, 1])

    def test_crc_failure_partial_delivery_before_damage(self):
        proc, res = self.audit("crc_failure", 2)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [0, 1])

    # ------------------------------------------------ 协议违规须拒绝
    def test_marker_without_txn_rejected(self):
        proc, res = self.audit("marker_without_txn", 2)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("无对应" in e for e in res["errors"]))

    def test_wrong_epoch_rejected(self):
        proc, res = self.audit("wrong_epoch", 2)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("epoch" in e for e in res["errors"]))

    def test_concurrent_txn_rejected(self):
        proc, res = self.audit("concurrent_txn", 2)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("并发" in e for e in res["errors"]))

    # ------------------------------------------------ high watermark 规则
    def test_hw_mid_batch_rejected(self):
        proc, res = self.audit("cross_batch_commit", 3)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("批次边界" in e for e in res["errors"]))

    def test_hw_beyond_verified_rejected(self):
        proc, res = self.audit("cross_batch_commit", 11)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("超过已验证区域" in e for e in res["errors"]))

    def test_hw_zero(self):
        proc, res = self.audit("clean_normal", 0)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [])
        self.assertEqual(res["deliverable"]["range_end"], 0)

    # ------------------------------------------------ 普通记录 / headers / null
    def test_clean_normal(self):
        proc, res = self.audit("clean_normal", 3)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [0, 1, 2])
        recs = self.by_offset(res)
        self.assertEqual(
            recs[0]["headers"],
            [
                {"key": "trace-id", "value": b"abc123".hex()},
                {"key": "empty", "value": None},
            ],
        )
        self.assertIsNone(recs[2]["key"])
        self.assertIsNone(recs[2]["value"])

    # ------------------------------------------------ 解析正确性
    def test_byte_positions_and_offsets(self):
        proc, res = self.audit("cross_batch_commit", 10)
        self.assertEqual(proc.returncode, 0, res["errors"])
        batches = res["batches"]
        self.assertEqual(batches[0]["byte_start"], 0)
        for prev, cur in zip(batches, batches[1:]):
            self.assertEqual(prev["byte_end"], cur["byte_start"])
        # 首个记录紧跟 61 字节批次头
        self.assertEqual(res["records"][0]["byte_pos"], 61)
        # 记录 offset 连续且从 0 起
        self.assertEqual([r["offset"] for r in res["records"]], list(range(10)))
        # 所有批次 CRC 校验值与存储值一致
        for b in batches:
            self.assertTrue(b["crc_ok"])

    def test_text_mode(self):
        proc = subprocess.run(
            [sys.executable, AUDITOR, self.paths["cross_batch_commit"], "--hw", "10"],
            capture_output=True,
        )
        out = proc.stdout.decode("utf-8")
        self.assertEqual(proc.returncode, 0)
        self.assertIn("可交付范围", out)
        self.assertIn("字节位置", out)
        self.assertIn("COMMIT", out)


class InterleavedCLITest(unittest.TestCase):
    """--interleaved 模式：多生产者交错事务的独立追踪与 LSO 判定。"""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.paths = {}
        for name, blob in FIXTURES.items():
            p = os.path.join(cls.tmp.name, name + ".bin")
            with open(p, "wb") as fh:
                fh.write(blob)
            cls.paths[name] = p

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def audit(self, name, hw):
        return run_cli(self.paths[name], hw, extra=("--interleaved",))

    @staticmethod
    def by_offset(res):
        return {r["offset"]: r for r in res["records"]}

    # ------------------------------------------------ 交错提交 / LSO
    def test_interleaved_commit_full_hw(self):
        proc, res = self.audit("interleaved_commit", 10)
        self.assertEqual(proc.returncode, 0, res["errors"])
        # 控制标记 (offset 6, 8) 不得进入交付列表
        self.assertEqual(res["deliverable"]["offsets"], [0, 1, 2, 3, 4, 5, 7, 9])
        self.assertIsNone(res["lso"])
        self.assertEqual(res["effective_limit"], 10)
        recs = self.by_offset(res)
        for off in (6, 8):
            self.assertEqual(recs[off]["kind"], "control")
            self.assertEqual(recs[off]["status"], "hidden")
            self.assertIn("COMMIT", recs[off]["reason"])
        self.assertEqual(len(res["transactions"]), 2)
        outcomes = {t["producer_id"]: t["outcome"] for t in res["transactions"]}
        self.assertEqual(outcomes, {1000: "committed", 1001: "committed"})
        rec_map = {t["producer_id"]: t["record_offsets"] for t in res["transactions"]}
        self.assertEqual(rec_map[1000], [1, 2])
        self.assertEqual(rec_map[1001], [3, 4, 7])

    def test_interleaved_lso_earliest_open(self):
        # 两个事务都未决：LSO 取最早首条 offset，其后记录不得提前交付
        proc, res = self.audit("interleaved_commit", 6)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["lso"], 1)
        self.assertEqual(res["effective_limit"], 1)
        self.assertEqual(res["deliverable"]["offsets"], [0])
        recs = self.by_offset(res)
        for off in (1, 2, 3, 4):
            self.assertEqual(recs[off]["status"], "hidden")
            self.assertIn("未决", recs[off]["reason"])
        self.assertEqual(recs[5]["status"], "hidden")
        self.assertIn("LSO", recs[5]["reason"])
        for off in (6, 7, 8, 9):
            self.assertEqual(recs[off]["status"], "unevaluated")

    def test_interleaved_partial_commit(self):
        # pid=1000 已提交、pid=1001 未决：LSO 压住在 3
        proc, res = self.audit("interleaved_commit", 7)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["lso"], 3)
        self.assertEqual(res["effective_limit"], 3)
        self.assertEqual(res["deliverable"]["offsets"], [0, 1, 2])
        recs = self.by_offset(res)
        self.assertEqual(recs[6]["status"], "hidden")
        self.assertIn("COMMIT", recs[6]["reason"])

    def test_interleaved_marker_beyond_hw_not_deciding(self):
        # 提交标记在 HW 之外：事务保持未决，继续压住 LSO
        proc, res = self.audit("interleaved_abort_recommit", 5)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["lso"], 3)
        self.assertEqual(res["effective_limit"], 3)
        self.assertEqual(res["deliverable"]["offsets"], [])
        recs = self.by_offset(res)
        self.assertEqual(recs[5]["kind"], "control")
        self.assertEqual(recs[5]["status"], "unevaluated")

    # ------------------------------------------------ 连续事务独立
    def test_interleaved_abort_then_commit(self):
        # 同一生产者先中止再提交：旧事务记录保持隐藏，新事务独立可见
        proc, res = self.audit("interleaved_abort_recommit", 7)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [3, 4, 6])
        self.assertIsNone(res["lso"])
        recs = self.by_offset(res)
        for off in (0, 1):
            self.assertEqual(recs[off]["status"], "hidden")
            self.assertIn("中止", recs[off]["reason"])
        for off in (3, 4):
            self.assertEqual(recs[off]["status"], "deliverable")
            self.assertIn("已提交", recs[off]["reason"])
        self.assertEqual(len(res["transactions"]), 2)
        t1, t2 = res["transactions"]
        self.assertEqual(t1["outcome"], "aborted")
        self.assertEqual(t1["record_offsets"], [0, 1])
        self.assertEqual(t1["marker_offset"], 2)
        self.assertEqual(t2["outcome"], "committed")
        self.assertEqual(t2["record_offsets"], [3, 4])
        self.assertEqual(t2["marker_offset"], 5)

    def test_interleaved_epoch_bump_after_close(self):
        # 关闭后 epoch 增加开新事务：合法
        proc, res = self.audit("interleaved_epoch_bump", 5)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [2, 4])
        recs = self.by_offset(res)
        self.assertEqual(recs[0]["status"], "hidden")
        self.assertIn("中止", recs[0]["reason"])
        self.assertEqual(len(res["transactions"]), 2)
        self.assertEqual(res["transactions"][0]["producer_epoch"], 0)
        self.assertEqual(res["transactions"][1]["producer_epoch"], 1)

    # ------------------------------------------------ 混合结局
    def test_interleaved_mixed_outcomes(self):
        # 一中止、一提交、一未决：LSO 由未决事务压住
        proc, res = self.audit("interleaved_mixed", 6)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["lso"], 2)
        self.assertEqual(res["effective_limit"], 2)
        self.assertEqual(res["deliverable"]["offsets"], [1])
        recs = self.by_offset(res)
        self.assertEqual(recs[0]["status"], "hidden")
        self.assertIn("中止", recs[0]["reason"])
        self.assertEqual(recs[2]["status"], "hidden")
        self.assertIn("未决", recs[2]["reason"])
        self.assertEqual(recs[5]["status"], "hidden")
        self.assertIn("LSO", recs[5]["reason"])
        outcomes = {t["producer_id"]: t["outcome"] for t in res["transactions"]}
        self.assertEqual(
            outcomes, {8000: "aborted", 8001: "committed", 8002: "open"}
        )

    # ------------------------------------------------ 协议违规须拒绝
    def test_interleaved_wrong_epoch_marker_rejected(self):
        proc, res = self.audit("interleaved_wrong_epoch_marker", 2)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("epoch" in e for e in res["errors"]))

    def test_interleaved_marker_without_txn_rejected(self):
        proc, res = self.audit("interleaved_marker_without_txn", 2)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("无对应" in e for e in res["errors"]))

    def test_interleaved_epoch_change_while_open_rejected(self):
        proc, res = self.audit("interleaved_epoch_change_open", 2)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("epoch" in e for e in res["errors"]))

    def test_interleaved_epoch_regression_rejected(self):
        proc, res = self.audit("interleaved_epoch_regression", 3)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("epoch" in e for e in res["errors"]))

    # ------------------------------------------------ 前提与模式边界
    def test_interleaved_too_many_producers_warned(self):
        proc, res = self.audit("interleaved_too_many_producers", 10)
        self.assertEqual(proc.returncode, 0, res["errors"])
        self.assertEqual(res["deliverable"]["offsets"], [0, 1, 2, 3, 4])
        self.assertTrue(any("生产者" in w for w in res["warnings"]))

    def test_interleaved_fixture_default_mode_unaffected(self):
        # 同一夹具在默认单事务模式下按原规则判定：交错即并发 -> 拒绝
        proc, res = run_cli(self.paths["interleaved_commit"], 10)
        self.assertEqual(proc.returncode, 2)
        self.assertIsNone(res["deliverable"])
        self.assertTrue(any("并发" in e for e in res["errors"]))

    def test_interleaved_text_mode(self):
        proc = subprocess.run(
            [
                sys.executable,
                AUDITOR,
                self.paths["interleaved_abort_recommit"],
                "--hw",
                "7",
                "--interleaved",
            ],
            capture_output=True,
        )
        out = proc.stdout.decode("utf-8")
        self.assertEqual(proc.returncode, 0)
        self.assertIn("已中止", out)
        self.assertIn("已提交", out)
        self.assertIn("可交付范围", out)


if __name__ == "__main__":
    unittest.main()
