from threading import get_ident

from lstore.table import Table, Record
from lstore.index import Index
from lstore.lockmanager import lockmanager

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self._acquired_locks = set()
        self._executed_queries = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        table = query.__self__.table
        name = query.__name__
        self.queries.append((query, table, name, args))

    def _release_locks(self):
        for rid in self._acquired_locks:
            lockmanager.release_lock(rid, get_ident())

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):

        for query, table, name, args in self.queries:
            if name == 'delete':
                key = args[0]
                rids = table.index.locate(table.key, key)
                if not rids:
                    return self.abort()
                for rid in rids:
                    if not lockmanager.get_shared_lock(rid, get_ident()):
                        return self.abort()
                    self._acquired_locks.add(rid)
                    if not lockmanager.get_exclusive_lock(rid, get_ident()):
                        return self.abort()
                self._executed_queries.append((name, table, rids[0]))
            elif name == 'update':
                key = args[0]
                rids = table.index.locate(table.key, key)
                if not rids:
                    return self.abort()
                for rid in rids:
                    if not lockmanager.get_shared_lock(rid, get_ident()):
                        return self.abort()
                    self._acquired_locks.add(rid)
                    if not lockmanager.get_exclusive_lock(rid, get_ident()):
                        return self.abort()
                self._executed_queries.append((name, table, rids[0]))
            elif name == 'select':
                key = args[0]
                column = args[1]
                rids = table.index.locate(column, key)
                if not rids:
                    return self.abort()
                for rid in rids:
                    if not lockmanager.get_shared_lock(rid, get_ident()):
                        return self.abort()
                    self._acquired_locks.add(rid)
                self._executed_queries.append((name, table, rids[0]))
            elif name == 'sum':
                start = args[0]
                end = args[1]
                rids = table.index.locate_range(start, end, table.key)
                if not rids:
                    return self.abort()
                for rid in rids:
                    if not lockmanager.get_shared_lock(rid, get_ident()):
                        return self.abort()
                    self._acquired_locks.add(rid)
                self._executed_queries.append((name, table, rids[0]))
            elif name == 'increment':
                key = args[0]
                rids = table.index.locate(table.key, key)
                if not rids:
                    return self.abort()
                for rid in rids:
                    if not lockmanager.get_shared_lock(rid, get_ident()):
                        return self.abort()
                    self._acquired_locks.add(rid)
                    if not lockmanager.get_exclusive_lock(rid, get_ident()):
                        return self.abort()
                self._executed_queries.append((name, table, rids[0]))

            try:
                result = query(*args)
            except Exception:
                self._executed_queries.pop()
                return self.abort()

            # If the query has failed the transaction should abort
            if result == False:
                self._executed_queries.pop()
                return self.abort()

        return self.commit()

    def abort(self):
        while self._executed_queries:
            name, table, rid = self._executed_queries.pop()
            if name == 'insert':
                table.delete_record(rid)
            elif name == 'update' or name == 'increment':
                table.undo_update(rid)
        self._release_locks()
        return False

    def commit(self):
        self._release_locks()
        return True

