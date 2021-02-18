from lstore.config import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):

        data = self.table.get_column(column)
        record_ids = self.table.get_metadata_column(RID_COLUMN)
        temp = zip(data,record_ids)
        locations = []
        for item in temp:
            if item[0] == value:
                locations.append(item[1])
        return locations

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        
        
        data = self.table.get_column(column)
        record_ids = self.table.get_metadata_column(RID_COLUMN)
        locations = []
        temp = zip(data,record_ids)

        for item in temp:
            if item[0] >= begin:
                if item[0] <= end:
                    locations.append[item[1]]
        return locations 

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
