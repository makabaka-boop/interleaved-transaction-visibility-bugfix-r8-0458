"""多生产者交错事务追踪。

与默认单事务模式不同，本模块允许不同 producerId 的事务跨批次交错。
每个 producerId 同时至多有一个开放事务；事务关闭后，可用同一 epoch 开启连续
事务，也可以在旧事务已关闭后使用更大的 epoch。仍开放事务的 producer 不得切换
epoch；任何比该 producer 已使用 epoch 更旧的数据批次都属于错误代次。
"""

MAX_INTERLEAVED_PRODUCERS = 4


def _mark_records(batch, status, reason):
    for rec in batch.records:
        rec.status = status
        rec.reason = reason


def run_transactions(batches, hw, transactions, errors):
    """关联 HW 之前批次中的交错事务。

    返回 ``(lso, effective_limit)``。LSO 是所有仍开放事务中最早的首条记录
    offset（不是最晚的开放事务），因此任何未结束事务都能阻止其后记录提前交付。
    """
    active = {}
    latest_epoch = {}
    generation_by_pid = {}

    for batch in batches:
        if batch.base_offset >= hw or not batch.transactional:
            continue

        pid = batch.producer_id
        epoch = batch.producer_epoch
        marker_name = batch.control_type.upper() if batch.control else None

        if batch.control:
            rec = batch.records[0]
            txn = active.get(pid)
            if txn is None:
                errors.append(
                    f"offset {batch.base_offset}: {marker_name} 标记无对应开放事务 "
                    f"pid={pid} epoch={epoch}，拒绝交付"
                )
                rec.status = "hidden"
                rec.reason = f"{marker_name} 控制标记（无对应事务，协议错误）"
                continue

            if epoch != txn["epoch"]:
                errors.append(
                    f"offset {batch.base_offset}: {marker_name} 标记 "
                    f"pid={pid} epoch={epoch} 与开放事务 epoch={txn['epoch']} "
                    f"不匹配（错误代次），拒绝交付"
                )
                rec.status = "hidden"
                rec.reason = f"{marker_name} 控制标记（pid/epoch 不匹配，协议错误）"
                continue

            txn["outcome"] = "committed" if batch.control_type == "commit" else "aborted"
            txn["marker_offset"] = batch.base_offset
            del active[pid]
            rec.status = "hidden"
            rec.reason = f"{marker_name} 控制标记，不属业务记录"
            continue

        current = active.get(pid)
        if current is not None:
            if epoch != current["epoch"]:
                errors.append(
                    f"offset {batch.base_offset}: pid={pid} 的开放事务 epoch="
                    f"{current['epoch']} 尚未结束，不能切换到 epoch={epoch}"
                    f"（错误代次），拒绝交付"
                )
                _mark_records(batch, "error", "开放事务未结束时切换 epoch（协议错误）")
                continue
            txn = current
        else:
            known_epoch = latest_epoch.get(pid)
            if known_epoch is not None and epoch < known_epoch:
                errors.append(
                    f"offset {batch.base_offset}: pid={pid} 使用过期 epoch={epoch}，"
                    f"该生产者已使用 epoch={known_epoch}（错误代次），拒绝交付"
                )
                _mark_records(batch, "error", "producer epoch 回退（协议错误）")
                continue

            if len(active) >= MAX_INTERLEAVED_PRODUCERS:
                errors.append(
                    f"offset {batch.base_offset}: 交错模式最多允许四个同时开放的"
                    f"事务生产者，pid={pid} 超出限制，拒绝交付"
                )
                _mark_records(batch, "error", "并发交错事务生产者数量超限")
                continue

            generation = generation_by_pid.get(pid, 0)
            txn = {
                "pid": pid,
                "epoch": epoch,
                "generation": generation,
                "first_offset": batch.records[0].offset,
                "record_offsets": [],
                "outcome": "open",
                "marker_offset": None,
            }
            transactions.append(txn)
            active[pid] = txn
            generation_by_pid[pid] = generation + 1
            latest_epoch[pid] = max(epoch, latest_epoch.get(pid, epoch))

        for rec in batch.records:
            rec.txn = txn
            txn["record_offsets"].append(rec.offset)

    lso = min((txn["first_offset"] for txn in active.values()), default=None)
    effective_limit = min(lso, hw) if lso is not None else hw
    return lso, effective_limit
