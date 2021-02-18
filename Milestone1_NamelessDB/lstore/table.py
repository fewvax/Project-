
from typing import Sequence, Iterator

from lstore.config import *
from lstore.pagerange import PageRange
from lstore.index import Index


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.key_rid_directory = {}                         # Maps keys to base page RIDs
        self.ordered_keys = []                              # Creates an ordered list of primary key values
        self.index = Index(self)
        self._page_ranges = []
        self._create_new_page_range()
        self.max_rid = START_RID
        self._next_rid = START_RID

    def __getitem__(self, key):
        return self._page_ranges[key]

    def __setitem__(self, key, value):
        self._page_ranges[key] = value

    def insert_to_directory(self, rid):
        range_num = len(self._page_ranges) - 1
        cur_range = self._page_ranges[-1]
        row_num = cur_range.find_offset_index(True)
        cur_row = cur_range.find_offset_data(True)
        page_num = cur_row.find_offset_index()
        cur_page = cur_row.find_offset_data()
        line_num = cur_page.get_num_records() - 1
        self.page_directory[rid] = [range_num, True, row_num, page_num, line_num]

    def update_to_directory(self, rid, range_num):
        cur_range = self._page_ranges[range_num]
        row_num = cur_range.find_offset_index(False)
        cur_row = cur_range.find_offset_data(False)
        page_num = cur_row.find_offset_index()
        cur_page = cur_row.find_offset_data()
        line_num = cur_page.get_num_records() - 1
        self.page_directory[rid] = [range_num, False, row_num, page_num, line_num]

    def find_range_of_key(self, key):
        if key not in self.key_rid_directory:
            return -1
        rid = self.key_rid_directory[key]
        return self.page_directory[rid][0]

    def insert_record(self, record: Record, schema_encoder):
        if self._page_ranges[-1].is_full():
            self._page_ranges.append(PageRange(self.num_columns))
        self._page_ranges[-1].add_record(record, schema_encoder)
        self.insert_to_directory(record.rid)
        key = record.columns[record.key]
        self.key_rid_directory[key] = record.rid
        self.ordered_keys.append(key)

    def update_record(self, range_num, record: Record, schema_encoder, indirection_column):
        # Check to make sure range number is correct is done inside the query
        self._page_ranges[range_num].update_record(record, schema_encoder, indirection_column)
        self.update_to_directory(record.rid, range_num)

    def _create_new_page_range(self):
        next_range_index = len(self._page_ranges)
        range_base_rid = next_range_index * RECORDS_PER_PAGE_RANGE + START_RID
        new_page_range = PageRange(self.num_columns, base_rid=range_base_rid)
        self._page_ranges.append(new_page_range)

    def _get_current_page_range(self):
        return self._page_ranges[-1]

    def append_record(self, *column_values: int) -> None:
        if self._get_current_page_range().is_full():
            self._create_new_page_range()
        self._get_current_page_range().append_record(self._next_rid, *column_values)
        self._next_rid += 1

    def _get_page_range_of_rid(self, rid: int) -> PageRange:
        page_range_index = (rid - START_RID) // RECORDS_PER_PAGE_RANGE
        assert page_range_index in range(len(self._page_ranges)), \
                "RID out of range: " + str(rid)
        return self._page_ranges[page_range_index]

    def read_raw_record(self, rid: int) -> Sequence[int]:
        page_range = self._get_page_range_of_rid(rid)
        return page_range.read_record(rid)

    def read_record(self, rid: int) -> Record:
        raw_record = self.read_raw_record(rid)
        # check if record has been deleted?
        columns = raw_record[METADATA_COLUMN_COUNT:]
        return Record(rid, self.key, columns)

    def delete_record(self, rid: int) -> Sequence[int]:
        page_range = self._get_page_range_of_rid(rid)
        page_range.delete_record(rid)

    def update_record_new(self, rid: int, *column_values: int) -> None:
        page_range = self._get_page_range_of_rid(rid)
        page_range.update_record_new(rid, *column_values)

    def get_field(self, column_index: int, rid: int) -> int:
        page_range = self._get_page_range_of_rid(rid)
        return page_range.get_field(column_index + METADATA_COLUMN_COUNT, rid)

    def get_column(self, column_index: int) -> Iterator[int]:
        for page_range in self._page_ranges:
            yield from page_range.get_column(column_index + METADATA_COLUMN_COUNT)

    def get_metadata_column(self, column_index: int) -> Iterator[int]:
        assert column_index in range(METADATA_COLUMN_COUNT), \
                "column_index out of range: " + str(column_index)
        for page_range in self._page_ranges:
            yield from page_range.get_column(column_index)

    def __str__(self):
        s = "I\tR\tTimestamp\tSE\t"
        for i in range(0, self.num_columns):
            if self.key == i:
                s += "K"
            s += ("C" + str(i + 1) + "\t")
        s += "\n"
        s += ("_" * (4 * (self.num_columns + 4) + 6) + "\n")
        for r in self._page_ranges:
            for row in r._base_row_group:
                for i in range(0, len(row[0].pages)):
                    for j in range(0, PAGE_SIZE // TYPE_SIZE):
                        for k in range(0, len(row.big_pages)):
                            s += (str(row[k][i][j]) + "\t")
                        s += "\n"
                    s += ("_" * (4 * len(row.big_pages) - 3) + "\n")
                s += ("_" * (4 * len(row.big_pages)) + "\n")
            s += ("_" * (4 * (self.num_columns + 4) + 3) + "\n")
            for row in r._tail_row_group:
                for i in range(0, len(row[0].pages)):
                    for j in range(0, PAGE_SIZE // TYPE_SIZE):
                        for k in range(0, len(row.big_pages)):
                            s += (str(row[k][i][j]) + "\t")
                        s += "\n"
                    s += ("_" * (4 * len(row.big_pages) - 3) + "\n")
                s += ("_" * (4 * len(row.big_pages)) + "\n")
            s += ("_" * (4 * (self.num_columns + 4) + 6) + "\n")
        return s

    def get_data_from_rid(self, rid):
        range_num, is_base, row_num, page_num, line_num = self.page_directory[rid]
        cur_range = self._page_ranges[range_num]
        if is_base:
            cur_row = cur_range._base_row_group[row_num]
        else:
            cur_row = cur_range._tail_row_group[row_num]
        values = []
        for i in range(0, self.num_columns + 4):
            values.append(cur_row[i][page_num][line_num])
        return values

    def set_data_at_rid(self, rid, indirection_value=None, schema_value=None):
        range_num, is_base, row_num, page_num, line_num = self.page_directory[rid]
        cur_range = self._page_ranges[range_num]
        if is_base:
            cur_row = cur_range._base_row_group[row_num]
        else:
            cur_row = cur_range._tail_row_group[row_num]
        if indirection_value is not None:
            cur_row[INDIRECTION_COLUMN][page_num].write_to_pos(indirection_value, line_num)
        if schema_value is not None:
            cur_row[SCHEMA_ENCODING_COLUMN][page_num].write_to_pos(schema_value, line_num)

    def __iter__(self):
        for page_range in self._page_ranges:
            yield from page_range

    def __merge(self):
        pass

