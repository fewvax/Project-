from threading import Thread

from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker():

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.stats = []
        self.transactions = []
        self.result = 0
        self.thread = None

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs a transaction
    """
    def run(self):
        def _run():
            for transaction in self.transactions:
                # each transaction returns True if committed or False if aborted
                self.stats.append(transaction.run())
            # stores the number of transactions that committed
            self.result = len(list(filter(lambda x: x, self.stats)))

        self.thread = Thread(target=_run)
        self.thread.start()

