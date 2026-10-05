#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_fixtures.py — 生成测试用二进制夹具 (fixtures/*.bin)

编码器 (fixture_encoder) 与审计器 (kafka_audit) 相互独立，不启动真实 broker。
"""
import os

from fixture_encoder import encode_batch, encode_control_batch, encode_record


def build_fixtures():
    f = {}
    rec = encode_record

    # 1) 跨批提交：事务跨 2 个数据批 + 独立提交批，普通记录穿插其间
    #    offset: 0-1 普通 | 2-3 事务 | 4 普通 | 5-6 事务 | 7 COMMIT | 8-9 普通
    f["cross_batch_commit"] = b"".join(
        [
            encode_batch(
                0, [rec(0, key="k0", value="alpha"), rec(1, key="k1", value="beta")]
            ),
            encode_batch(
                2,
                [rec(0, key="t0", value="tx-a"), rec(1, key="t1", value="tx-b")],
                producer_id=100,
                producer_epoch=3,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(4, [rec(0, key="k4", value="gamma")]),
            encode_batch(
                5,
                [rec(0, key="t2", value="tx-c"), rec(1, key="t3", value="tx-d")],
                producer_id=100,
                producer_epoch=3,
                base_sequence=2,
                transactional=True,
            ),
            encode_control_batch(7, "commit", producer_id=100, producer_epoch=3),
            encode_batch(
                8, [rec(0, key="k8", value="delta"), rec(1, key="k9", value="epsilon")]
            ),
        ]
    )

    # 2) 中止事务：事务记录全部隐藏
    #    offset: 0-2 事务 | 3 普通 | 4 ABORT | 5-6 普通
    f["abort"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="x0"), rec(1, value="x1"), rec(2, value="x2")],
                producer_id=200,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(3, [rec(0, key="n3", value="normal-3")]),
            encode_control_batch(4, "abort", producer_id=200, producer_epoch=0),
            encode_batch(5, [rec(0, value="n5"), rec(1, value="n6")]),
        ]
    )

    # 3) 开放事务 + 其后穿插普通消息：LSO 被压住在事务首条记录
    #    offset: 0-1 普通 | 2-3 事务 | 4-5 普通 | 6 事务 (无标记)
    f["open_txn"] = b"".join(
        [
            encode_batch(0, [rec(0, value="a"), rec(1, value="b")]),
            encode_batch(
                2,
                [rec(0, value="t0"), rec(1, value="t1")],
                producer_id=300,
                producer_epoch=1,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(4, [rec(0, value="c"), rec(1, value="d")]),
            encode_batch(
                6,
                [rec(0, value="t2")],
                producer_id=300,
                producer_epoch=1,
                base_sequence=2,
                transactional=True,
            ),
        ]
    )

    # 4) 中段损坏长度：第 2 个批次 batchLength=5 (< 最小合法值 49)
    f["corrupt_length"] = b"".join(
        [
            encode_batch(0, [rec(0, value="ok0"), rec(1, value="ok1")]),
            encode_batch(2, [rec(0, value="bad")], length_override=5),
            encode_batch(3, [rec(0, value="ok3")]),
        ]
    )

    # 5) 尾部不完整：末批被截断
    tail_full = encode_batch(4, [rec(0, value="p4")])
    f["truncated_tail"] = b"".join(
        [
            encode_batch(0, [rec(0, value="p0"), rec(1, value="p1")]),
            encode_batch(2, [rec(0, value="p2"), rec(1, value="p3")]),
            tail_full[:40],
        ]
    )

    # 6) 中段 CRC32C 损坏
    f["crc_failure"] = b"".join(
        [
            encode_batch(0, [rec(0, value="c0"), rec(1, value="c1")]),
            encode_batch(2, [rec(0, value="c2")], crc_override=0xDEADBEEF),
            encode_batch(3, [rec(0, value="c3"), rec(1, value="c4")]),
        ]
    )

    # 7) 无对应事务的提交标记
    f["marker_without_txn"] = b"".join(
        [
            encode_batch(0, [rec(0, value="m0")]),
            encode_control_batch(1, "commit", producer_id=500, producer_epoch=0),
        ]
    )

    # 8) epoch 错误的提交标记 (事务 epoch=2，标记 epoch=3)
    f["wrong_epoch"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="w0")],
                producer_id=600,
                producer_epoch=2,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(1, "commit", producer_id=600, producer_epoch=3),
        ]
    )

    # 9) 并发事务 (pid=700 未结束，pid=701 又开事务)
    f["concurrent_txn"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="t0")],
                producer_id=700,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(
                1,
                [rec(0, value="t1")],
                producer_id=701,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
        ]
    )

    # 10) 纯普通记录（含 headers / null key / null value）
    f["clean_normal"] = b"".join(
        [
            encode_batch(
                0,
                [
                    rec(
                        0,
                        key="kk",
                        value="v0",
                        headers=[("trace-id", b"abc123"), ("empty", None)],
                    ),
                    rec(1, value="v1"),
                ],
            ),
            encode_batch(2, [rec(0, key=None, value=None)]),
        ]
    )

    # 11) 交错事务：两个生产者穿插，均提交
    #     offset: 0 普通 | 1-2 事务A(pid=1000) | 3-4 事务B(pid=1001) | 5 普通 |
    #             6 COMMIT A | 7 事务B | 8 COMMIT B | 9 普通
    f["interleaved_commit"] = b"".join(
        [
            encode_batch(0, [rec(0, value="n0")]),
            encode_batch(
                1,
                [rec(0, value="a0"), rec(1, value="a1")],
                producer_id=1000,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(
                3,
                [rec(0, value="b0"), rec(1, value="b1")],
                producer_id=1001,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(5, [rec(0, value="n5")]),
            encode_control_batch(6, "commit", producer_id=1000, producer_epoch=0),
            encode_batch(
                7,
                [rec(0, value="b2")],
                producer_id=1001,
                producer_epoch=0,
                base_sequence=2,
                transactional=True,
            ),
            encode_control_batch(8, "commit", producer_id=1001, producer_epoch=0),
            encode_batch(9, [rec(0, value="n9")]),
        ]
    )

    # 12) 同一生产者先中止再提交新事务：连续事务必须独立，
    #     旧事务记录不得改变归属或重新可见
    #     offset: 0-1 事务1(pid=2000,epoch=0) | 2 ABORT |
    #             3-4 事务2(同 pid/epoch) | 5 COMMIT | 6 普通
    f["interleaved_abort_recommit"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="x0"), rec(1, value="x1")],
                producer_id=2000,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(2, "abort", producer_id=2000, producer_epoch=0),
            encode_batch(
                3,
                [rec(0, value="y0"), rec(1, value="y1")],
                producer_id=2000,
                producer_epoch=0,
                base_sequence=2,
                transactional=True,
            ),
            encode_control_batch(5, "commit", producer_id=2000, producer_epoch=0),
            encode_batch(6, [rec(0, value="n6")]),
        ]
    )

    # 13) 关闭后 epoch 增加再开新事务（合法）
    #     offset: 0 事务1(pid=3000,epoch=0) | 1 ABORT |
    #             2 事务2(pid=3000,epoch=1) | 3 COMMIT(epoch=1) | 4 普通
    f["interleaved_epoch_bump"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="e0")],
                producer_id=3000,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(1, "abort", producer_id=3000, producer_epoch=0),
            encode_batch(
                2,
                [rec(0, value="e1")],
                producer_id=3000,
                producer_epoch=1,
                base_sequence=1,
                transactional=True,
            ),
            encode_control_batch(3, "commit", producer_id=3000, producer_epoch=1),
            encode_batch(4, [rec(0, value="n4")]),
        ]
    )

    # 14) 交错：标记 epoch 与开放事务不匹配 -> 拒绝
    f["interleaved_wrong_epoch_marker"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="w0")],
                producer_id=4000,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(1, "commit", producer_id=4000, producer_epoch=1),
        ]
    )

    # 15) 交错：标记无对应开放事务（另一生产者事务仍开放） -> 拒绝
    f["interleaved_marker_without_txn"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="t0")],
                producer_id=5000,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(1, "commit", producer_id=5001, producer_epoch=0),
        ]
    )

    # 16) 交错：事务开放期间 epoch 变更 -> 拒绝
    f["interleaved_epoch_change_open"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="t0")],
                producer_id=6000,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(
                1,
                [rec(0, value="t1")],
                producer_id=6000,
                producer_epoch=1,
                base_sequence=1,
                transactional=True,
            ),
        ]
    )

    # 17) 交错：关闭后 epoch 回退 -> 拒绝
    f["interleaved_epoch_regression"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="t0")],
                producer_id=7000,
                producer_epoch=1,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(1, "commit", producer_id=7000, producer_epoch=1),
            encode_batch(
                2,
                [rec(0, value="t1")],
                producer_id=7000,
                producer_epoch=0,
                base_sequence=1,
                transactional=True,
            ),
        ]
    )

    # 18) 三个生产者穿插：一中止、一提交、一未决（LSO 由未决者压住）
    #     offset: 0 事务A | 1 事务B | 2 事务C | 3 ABORT A | 4 COMMIT B | 5 普通
    f["interleaved_mixed"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="a0")],
                producer_id=8000,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(
                1,
                [rec(0, value="b0")],
                producer_id=8001,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(
                2,
                [rec(0, value="c0")],
                producer_id=8002,
                producer_epoch=0,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(3, "abort", producer_id=8000, producer_epoch=0),
            encode_control_batch(4, "commit", producer_id=8001, producer_epoch=0),
            encode_batch(5, [rec(0, value="n5")]),
        ]
    )

    # 19) 五个事务生产者：超出交错模式前提 (<= 4)，警告但仍给出判定
    #     offset: 0-4 各生产者一条事务记录 | 5-9 各生产者 COMMIT
    f["interleaved_too_many_producers"] = b"".join(
        [
            *(
                encode_batch(
                    i,
                    [rec(0, value=f"p{i}")],
                    producer_id=9000 + i,
                    producer_epoch=0,
                    base_sequence=0,
                    transactional=True,
                )
                for i in range(5)
            ),
            *(
                encode_control_batch(
                    5 + i, "commit", producer_id=9000 + i, producer_epoch=0
                )
                for i in range(5)
            ),
        ]
    )

    return f


FIXTURES = build_fixtures()


def main():
    outdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
    os.makedirs(outdir, exist_ok=True)
    for name, blob in FIXTURES.items():
        path = os.path.join(outdir, name + ".bin")
        with open(path, "wb") as fh:
            fh.write(blob)
        print(f"{path} ({len(blob)} 字节)")


if __name__ == "__main__":
    main()
