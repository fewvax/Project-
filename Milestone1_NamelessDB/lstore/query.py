from lstore.config import *
from lstore.table import *
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        if key in self.table.key_rid_directory:
            rec = self.table.get_data_from_rid(self.table.key_rid_directory[key])
            self.table.ordered_keys.remove(key)
            del self.table.key_rid_directory[key]
            while rec[INDIRECTION_COLUMN] != MAX_UNSIGNED_INT:
                if rec[INDIRECTION_COLUMN] == 0:
                    self.table.set_data_at_rid(rec[RID_COLUMN], MAX_UNSIGNED_INT, MAX_UNSIGNED_INT)
                    break
                else:
                    temp_rec = rec[INDIRECTION_COLUMN]
                    self.table.set_data_at_rid(rec[RID_COLUMN], MAX_UNSIGNED_INT, MAX_UNSIGNED_INT)
                    rec = self.table.get_data_from_rid(temp_rec)
            return True
        return False

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        if len(columns) != self.table.num_columns or not list_validate(columns) or columns[self.table.key] in \
                self.table.key_rid_directory:
            return False
        # schema_encoding = '0' * self.table.num_columns
        schema_encoding = 0
        rid = self.table.max_rid + 1
        self.table.max_rid += 1
        record = Record(rid, self.table.key, columns)
        self.table.insert_record(record, schema_encoding)
        return True

        pass

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        if key not in self.table.key_rid_directory or column != self.table.key:
            return False
        rid = self.table.key_rid_directory[key]
        data = self.table.get_data_from_rid(rid)
        if data[INDIRECTION_COLUMN] != 0:
            rid = data[INDIRECTION_COLUMN]
            data = self.table.get_data_from_rid(rid)
        for i in range(4, 4 + self.table.num_columns):
            if query_columns[i - 4] == 0:
                data[i] = None
        return [Record(rid, 0, data[4:])]
        pass

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        pass
        if len(columns) != self.table.num_columns or not validate(key) or not list_validate(columns):
            return False
        # Prevents keys from being updated
        if columns[self.table.key] is not None:
            return False
        range_num = self.table.find_range_of_key(key)
        if range_num == -1:
            return False
        rid = self.table.key_rid_directory[key]
        base_data = self.table.get_data_from_rid(rid)
        new_rid = self.table.max_rid + 1
        self.table.max_rid += 1
        new_schema = 0
        new_indirection = base_data[INDIRECTION_COLUMN]
        for i in range(0, len(columns)):
            if columns[i] is not None:
                new_schema += 2 ** (len(columns) - i - 1)
        new_schema = new_schema | base_data[SCHEMA_ENCODING_COLUMN]
        self.table.set_data_at_rid(rid, new_rid, new_schema)
        if new_indirection != 0:
            base_data = self.table.get_data_from_rid(new_indirection)
        record_columns = []
        for i in range(0, len(columns)):
            if columns[i] is None:
                record_columns.append(base_data[4 + i])
            else:
                record_columns.append(columns[i])
        record = Record(new_rid, self.table.key, record_columns)
        self.table.update_record(range_num, record, 0, new_indirection)
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        summation = 0
        
        for key in self.table.key_rid_directory:
            if start_range <= key:
                if end_range >= key:
                    selected_columns = [0] * self.table.num_columns
                    selected_columns[aggregate_column_index] = 1
                    record = self.select(key, self.table.key, selected_columns)[0]
                    value = record.columns[aggregate_column_index]
                    summation = summation + value
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

