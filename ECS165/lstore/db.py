from lstore.table import Table
from lstore.bufferpool import bufferpool
from collections import OrderedDict
from pathlib import Path
from shutil import rmtree

class Database():

    def __init__(self):
        self.tables = OrderedDict()
        self._dropped_tables = {}
        self.path = ""

    def open(self, path):
        self.path = Path(path)
        if self.path.exists():
            if not self.path.is_dir():
                raise Exception("path is not a directory")
        else:
            self.path.mkdir(exist_ok=True)

        for child in self.path.iterdir():
            if child.is_dir():
                table = Table.read_table_from_dir(child)
                self.tables[child.name] = table

    def close(self):
        for table in self.tables.values():
            table.close()
        bufferpool.flush()
        for table in self._dropped_tables.values():
            rmtree(table.path)
        self.tables.clear()
        self._dropped_tables.clear()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    #created OrderdedDict to maintain the order of insertion
    def create_table(self, name: str, num_columns: int, key:int):
        assert name != None,\
            "Must enter valid name"
        assert name not in self.tables,\
            "Table name taken: " + name
        assert key in range(num_columns),\
            "Key must be in range of num_columns: " + str(range(num_columns))
        assert num_columns >= 1,\
            "Table must have at least 1 data column"

        table_path = self.path / name
        table = Table.create_new_table(num_columns, key, table_path)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        assert name in self.tables,\
            "Table not found in database: " + name
        self._dropped_tables[name] = self.tables[name]
        del self.tables[name]

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        assert name in self.tables,\
            "Table not found in database: " + name
        return self.tables[name]
