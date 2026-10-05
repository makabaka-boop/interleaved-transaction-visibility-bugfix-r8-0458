# -*- coding: utf-8 -*-
"""交错模式 (--interleaved) 事务追踪。

前提: 最多四个事务生产者，各自同时至多一个未结束事务，事务可跨批次相互穿插。
每个生产者的当前开放事务、连续事务与 producer epoch 都独立追踪:

  * 结束标记必须对应仍开放的 (producer id, epoch)；无对应事务或 epoch 不符 -> 拒绝
  * 事务关闭后同一生产者可开始新的独立事务（连续事务互不影响）；
    epoch 只能在没有旧开放事务时增加，开放期间变更或回退 -> 拒绝
  * 全局 LSO 取所有开放事务中最早的首条 offset（受最早未结束事务约束）
  * HW 之外的批次（含控制标记）不参与决定
"""

MAX_PRODUCERS = 4  # 输入前提: 最多四个事务生产者


def _mark(batch, status, reason):
    for rec in batch.records:
        rec.status = status
        rec.reason = reason


def run_transactions(batches, hw, transactions, errors, warnings=None):
    """在 HW 之前的已验证批次上关联交错事务；返回 (lso, effective_limit)。

    协议违规（标记无对应开放事务 / 标记 epoch 不匹配 / epoch 非法变更）
    记入 errors —— 拒绝交付。
    """
    open_txn = {}      # pid -> 当前开放事务
    epoch_by_pid = {}  # pid -> 已确认的最近 producer epoch
    overflow_warned = False
    for batch in batches:
        if batch.base_offset >= hw:
            continue  # HW 之外的批次（含控制标记）不参与判断
        if not batch.transactional:
            continue
        pid = batch.producer_id
        epoch = batch.producer_epoch
        if batch.control:
            rec = batch.records[0]
            marker = batch.control_type.upper()
            txn = open_txn.get(pid)
            if txn is None:
                errors.append(
                    f"offset {batch.base_offset}: {marker} 标记 pid={pid} "
                    f"epoch={epoch} 无对应开放事务，拒绝"
                )
                rec.status = "hidden"
                rec.reason = f"{marker} 控制标记（无对应事务，协议错误）"
                continue
            if epoch != txn["epoch"]:
                errors.append(
                    f"offset {batch.base_offset}: {marker} 标记 pid={pid} "
                    f"epoch={epoch} 与开放事务 epoch={txn['epoch']} 不匹配"
                    f"（epoch 错误），拒绝"
                )
                rec.status = "hidden"
                rec.reason = f"{marker} 控制标记（epoch 不匹配，协议错误）"
                continue
            txn["outcome"] = (
                "committed" if batch.control_type == "commit" else "aborted"
            )
            txn["marker_offset"] = batch.base_offset
            rec.status = "hidden"
            rec.reason = f"{marker} 控制标记，不属业务记录"
            del open_txn[pid]
            continue
        txn = open_txn.get(pid)
        if txn is not None:
            if epoch != txn["epoch"]:
                errors.append(
                    f"offset {batch.base_offset}: pid={pid} 在事务开放期间出现 "
                    f"epoch={epoch}（开放事务 epoch={txn['epoch']}；epoch 只能在"
                    f"没有旧开放事务时增加），拒绝"
                )
                _mark(batch, "error", "epoch 冲突（协议错误）")
                continue
        else:
            known = epoch_by_pid.get(pid)
            if known is not None and epoch < known:
                errors.append(
                    f"offset {batch.base_offset}: pid={pid} epoch={epoch} 低于"
                    f"已建立的 epoch={known}（epoch 回退），拒绝"
                )
                _mark(batch, "error", "epoch 回退（协议错误）")
                continue
            if known is None and len(epoch_by_pid) >= MAX_PRODUCERS:
                if not overflow_warned and warnings is not None:
                    overflow_warned = True
                    warnings.append(
                        f"事务生产者数量超出交错模式前提 (<= {MAX_PRODUCERS})"
                    )
            # 每个新事务都是独立对象：关闭后的旧事务不复用、不改归属
            txn = {
                "pid": pid,
                "epoch": epoch,
                "first_offset": batch.base_offset,
                "record_offsets": [],
                "outcome": "open",
                "marker_offset": None,
            }
            transactions.append(txn)
            open_txn[pid] = txn
        epoch_by_pid[pid] = epoch
        for rec in batch.records:
            rec.txn = txn
            txn["record_offsets"].append(rec.offset)
    # 全局稳定界限受最早未结束事务约束
    lso = min((t["first_offset"] for t in open_txn.values()), default=None)
    effective = min(lso, hw) if lso is not None else hw
    return lso, effective
