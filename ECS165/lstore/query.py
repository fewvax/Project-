from threading import Lock
from lstore.config import *
from lstore.table import *
from lstore.index import *


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        self._latch = Lock()

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def print_it(self):
        self.table.index.create_index(self.table.key)
        self.table.index.indices[self.table.key].print_tree(self.table.index.indices[self.table.key].root)
        
        
    def delete(self, key):

        if self.table.index.indices[self.table.key] is None:
            self.table.index.create_index(self.table.key)

        rid = self.table.index.locate(self.table.key, key)[0]

        
        if rid is None or rid is False or rid == []:
            return False
        record = self.table.read_record(rid)
        self.table.delete_record(rid) 
        
        i = 0
        while i < len(record.columns):
            if self.table.index.indices[i]:
                record_node = self.table.index.indices[i].search_tree(record.columns[i])
                if record_node is None:
                    return False
                rid_list = record_node[1].keys[record_node[0]]
                
                if rid_list is None:
                    return False
                rid_list = rid_list[1]
                if len(rid_list) == 1:
                    self.table.index.indices[i].delete_in_tree(record_node[1],[record.columns[i],rid])
                elif len(rid_list)>1:
                    self.table.index.indices[i].mini_delete(record.columns[i],rid)
            i = i+1

        return True

        #     record = self.table.read_record(i[0])
        #     record_node = i[1]
        #     j = 0
        #     while j < len(record.columns):
        #         if self.table.index.indices[j]:
        #             self.table.index.indices[j].delete_in_tree(record_node, record.columns[j])
        #     self.table.delete_record(i[0])
        # return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        if len(columns) != self.table.num_columns or not list_validate(columns):
            return False
        rid = self.table.append_record(*columns)
        i = 0
        while i < len(columns):
            index_key = [columns[i], [rid]]
            if self.table.index.indices[i] is not None:
                self.table.index.indices[i].insert_to_tree(index_key)
            i = i + 1
        return True

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, key, column, query_columns):
        if self.table.index.indices[column] is None:
            self.table.index.create_index(column)

        rids = self.table.index.locate(column, key)
        record_list = []
        if rids is False or rids is None or rids == []:
            return False

        for i in rids:
            temp = self.table.read_record(i)
            for j in range(len(query_columns)):
                if query_columns[j] == 0:
                    temp.columns[j] = None
            record_list.append(temp)

        if record_list == []:
            return False
        else:
            return record_list

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, key, *columns):

        if self.table.index.indices[self.table.key] is None:
            self.table.index.create_index(self.table.key)
            #self.table.index.indices[self.table.key].print_tree(self.table.index.indices[self.table.key].root)
            
        for i in range(len(columns)):
            columns = list(columns)
            if columns[i] is None:
                columns[i] = NULL_VALUE
                
        #should be single value list of rid
        rid = self.table.index.locate(self.table.key, key)

        if rid is False or rid is None:
            return False
        
        rid = rid[0]
        past_record = self.table.read_record(rid)
        self.table.update_record(rid, *columns)

        i = 0
        while i < len(columns):
            if self.table.index.indices[i] is not None:
                if past_record.columns[i]!=columns[i]:
                    self.table.index.indices[i].update_tree(past_record.columns[i], rid, columns[i])
            i = i + 1
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):

        if self.table.index.indices[self.table.key] is None:
            self.table.index.create_index(self.table.key)
        summation = 0
        locations = self.table.index.locate_range(start_range, end_range, self.table.key)
        if locations == []:
            return False

        for i in locations:
            summation = summation + self.table.get_field(aggregate_column_index, i)
        return summation

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
