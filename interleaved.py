"""Interleaved producer tracking."""


def run_transactions(batches, hw, transactions, errors):
    active = {}
    cached = {}
    for batch in batches:
        if batch.base_offset >= hw or not batch.transactional:
            continue
        pid = batch.producer_id
        if batch.control:
            txn = active.get(pid)
            if txn is None:
                errors.append("unmatched marker")
                continue
            txn["outcome"] = (
                "committed" if batch.control_type == "commit" else "aborted"
            )
            txn["marker_offset"] = batch.base_offset
            del active[pid]
        else:
            if pid not in active:
                txn = cached.get(pid)
                if txn is None:
                    txn = {
                        "pid": pid,
                        "epoch": batch.producer_epoch,
                        "first_offset": batch.base_offset,
                        "record_offsets": [],
                        "outcome": "open",
                        "marker_offset": None,
                    }
                    transactions.append(txn)
                    cached[pid] = txn
                active[pid] = txn
            for rec in batch.records:
                rec.txn = active[pid]
                active[pid]["record_offsets"].append(rec.offset)
    lso = max((t["first_offset"] for t in active.values()), default=None)
    return lso, min(lso, hw) if lso is not None else hw
