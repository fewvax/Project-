from collections import namedtuple
from threading import Lock, get_ident

class LockHolder:

    def __init__(self):
        self._readers = set()
        self._writer = None

    def add_reader(self, reader) -> bool:
        if self._writer is None or self._writer == reader:
            self._readers.add(reader)
            return True
        return False

    def upgrade_to_writer(self, writer) -> bool:
        assert writer in self._readers
        if len(self._readers) == 1:
            self._writer = writer
            return True
        return False

    def release(self, reader) -> None:
        assert reader in self._readers
        if self._writer == reader:
            self._writer = None
        self._readers.remove(reader)

class LockManager:

    def __init__(self):
        self._latch = Lock()
        self._lock_holders = {}

    def get_shared_lock(self, rid, thread_id) -> bool:
        with self._latch:
            lock_holder = self._lock_holders.setdefault(rid, LockHolder())
            return lock_holder.add_reader(thread_id)

    def get_exclusive_lock(self, rid, thread_id) -> bool:
        with self._latch:
            lock_holder = self._lock_holders[rid]
            return lock_holder.upgrade_to_writer(thread_id)

    def release_lock(self, rid, thread_id) -> None:
        with self._latch:
            lock_holder = self._lock_holders[rid]
            lock_holder.release(thread_id)

lockmanager = LockManager()
