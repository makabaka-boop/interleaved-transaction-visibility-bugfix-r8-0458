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

    # 9a) 两个生产者交错；同一生产者关闭后可连续开启新事务，并可在关闭后提升 epoch
    #     offset:
    #       0 pid800/e1 事务开始
    #       1 普通（pid800 未决时不得越过 LSO=0）
    #       2 pid801/e1 事务开始
    #       3 普通
    #       4 pid801 COMMIT
    #       5 pid800/e1 事务续写
    #       6 pid800 COMMIT
    #       7 pid800/e1 新事务（同 epoch，关闭后允许）
    #       8 pid800 ABORT
    #       9 pid800/e2 下一代新事务
    #      10 pid800 COMMIT
    #      11 普通
    f["interleaved_commits"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="a-0")],
                producer_id=800,
                producer_epoch=1,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(1, [rec(0, value="normal-1")]),
            encode_batch(
                2,
                [rec(0, value="b-0")],
                producer_id=801,
                producer_epoch=1,
                base_sequence=0,
                transactional=True,
            ),
            encode_batch(3, [rec(0, value="normal-3")]),
            encode_control_batch(4, "commit", producer_id=801, producer_epoch=1),
            encode_batch(
                5,
                [rec(0, value="a-1")],
                producer_id=800,
                producer_epoch=1,
                base_sequence=1,
                transactional=True,
            ),
            encode_control_batch(6, "commit", producer_id=800, producer_epoch=1),
            encode_batch(
                7,
                [rec(0, value="a-aborted")],
                producer_id=800,
                producer_epoch=1,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(8, "abort", producer_id=800, producer_epoch=1),
            encode_batch(
                9,
                [rec(0, value="a-next-generation")],
                producer_id=800,
                producer_epoch=2,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(10, "commit", producer_id=800, producer_epoch=2),
            encode_batch(11, [rec(0, value="normal-11")]),
        ]
    )

    # 9b) 结束标记 epoch 与仍开放事务不一致
    f["interleaved_wrong_epoch_marker"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="open-e1")],
                producer_id=802,
                producer_epoch=1,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(1, "commit", producer_id=802, producer_epoch=2),
        ]
    )

    # 9c) 事务关闭后 producer epoch 不得回退
    f["interleaved_stale_epoch_data"] = b"".join(
        [
            encode_batch(
                0,
                [rec(0, value="e2")],
                producer_id=803,
                producer_epoch=2,
                base_sequence=0,
                transactional=True,
            ),
            encode_control_batch(1, "commit", producer_id=803, producer_epoch=2),
            encode_batch(
                2,
                [rec(0, value="stale-e1")],
                producer_id=803,
                producer_epoch=1,
                base_sequence=0,
                transactional=True,
            ),
        ]
    )

    # 9d) 交错模式最多允许四个同时开放的事务生产者
    f["interleaved_five_producers"] = b"".join(
        encode_batch(
            i,
            [rec(0, value=f"producer-{i}")],
            producer_id=810 + i,
            producer_epoch=0,
            base_sequence=0,
            transactional=True,
        )
        for i in range(5)
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
