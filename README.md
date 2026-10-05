# Kafka RecordBatch 离线审计器 (read_committed)

离线命令行工具：读取一个分区从 offset 0 起的 magic=2 未压缩 RecordBatch 字节流，
**不使用 Kafka 客户端**，手工解析并判定哪些业务记录可按 `read_committed` 交付。

## 用法

```bash
python3 kafka_audit.py <文件> --hw <high watermark> [--interleaved] [--json]
```

- `--hw`：high watermark，必须落在批次边界上；只有它之前的完整批次参与判断
- `--interleaved`：允许最多四个**同时开放**的事务生产者交错；每个生产者各自维护当前开放事务、连续事务编号和 producer epoch
- `--json`：输出结构化结果（批次 / 记录 / 事务 / LSO / 可交付范围 / 警告 / 错误）

退出码：`0` = 已给出交付列表；`2` = 拒绝交付（损坏影响判断 / 事务协议违规 / HW 无效）；`1` = 用法或 IO 错误。

## 输入前提

- 从 offset 0 起、未压缩、未日志压实、offset 连续、至多 32 个批次
- 至多一个同时进行的事务；其他生产者的普通记录可穿插

## 手工解析内容

- 批次头：`baseOffset / batchLength / partitionLeaderEpoch / magic / CRC32C /
  attributes / lastOffsetDelta / 时间戳 / producerId / producerEpoch /
  baseSequence / recordsCount`
- CRC32C (Castagnoli) 逐批校验 —— 中段失败立即停止，**禁止跳过继续扫描**
- 记录：有符号 (zigzag) varint 长度、`timestampDelta`、`offsetDelta`、key/value、headers
- 控制批次：COMMIT / ABORT 标记（key = version + type），不暴露为业务记录

## 判定规则 (read_committed)

- 以 `(producerId, producerEpoch)` 关联连续事务；同 pid 的 epoch 冲突、
  标记与开放事务不匹配、无对应事务的标记、并发事务 → **拒绝**（退出码 2）
- 中止事务的记录全部隐藏；已提交事务的记录可见
- 未决事务从首条记录起压住 LSO，其后的普通记录同样不得越过该界限
- 尾部不完整明确标记；HW 超过已验证区域 → 不提供交付列表
- 每条记录输出：原始 offset、字节位置、可见/隐藏原因（交错模式还包含 `generation`）；结论给出 LSO、
  生效上界 `min(LSO, HW)`、可交付范围与可交付 offset 列表

## 文件

| 文件 | 说明 |
|---|---|
| `kafka_audit.py` | 审计器（解析 + 判定 + 报告，表驱动 CRC32C） |
| `fixture_encoder.py` | 独立编码器（逐位 CRC32C，与审计器零共享代码） |
| `make_fixtures.py` | 生成 `fixtures/*.bin` 二进制夹具 |
| `test_auditor.py` | 测试：子进程跑 CLI + JSON 断言，不启动真实 broker |

## 测试

```bash
python3 make_fixtures.py          # 生成 fixtures/
python3 -m unittest test_auditor -v
```

夹具覆盖：跨批提交（含 HW 落在提交标记之前的未决情形）、中止、开放事务后穿插
普通消息（LSO 压住）、多生产者交错提交、同生产者连续事务与 epoch 代次、中段损坏长度、
尾部截断、中段 CRC 失败（禁止跳过）、无对应事务的标记、epoch 错误、交错生产者超限、
并发事务、HW 越界 / 不在边界、headers 与 null 字段。


`--interleaved` 模式允许最多四个同时开放的事务生产者，各自同时至多有一个未结束事务，事务可跨批次相互穿插。每个生产者独立追踪当前开放事务、关闭后的连续事务编号（`generation`）和 producer epoch。结束标记必须命中该生产者仍开放事务的 pid/epoch；无开放事务或错误 epoch 的标记会拒绝交付。关闭后同一生产者可开始新的独立事务，也可在没有旧开放事务时增加 epoch；epoch 回退或在旧事务开放时切换 epoch 均拒绝。LSO 取所有未结束事务中最早的首条 offset；HW 之后的控制标记不参与决定。默认模式（不带 `--interleaved`）继续遵守原单事务约定。
