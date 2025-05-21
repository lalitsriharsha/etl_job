def is_duplicate(pid, seen_pids):
    if pid in seen_pids:
        return True
    seen_pids.add(pid)
    return False
