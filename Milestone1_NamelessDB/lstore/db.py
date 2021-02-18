from lstore.table import Table
from collections import OrderedDict
class Database():

    def __init__(self):
        self.tables = OrderedDict()

    def open(self, path):
        pass

    def close(self):
        pass

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

        table = Table(name, num_columns, key)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        assert name in self.tables,\
            "Table not found in database: " + name
        del self.tables[name]

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        assert name in self.tables,\
            "Table not found in database: " + name
        return self.tables[name]
