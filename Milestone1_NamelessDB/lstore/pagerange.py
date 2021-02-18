from time import time
from typing import Tuple, Iterator, Sequence

from lstore.bigpage import BasePage
from lstore.config import *

# Unlimited tail rows, RANGE_SIZE base rows
class PageRange:
    def __init__(self, num_columns, base_rid=0):
        self.num_columns = num_columns
        init_base = BasePage(num_columns)
        init_tail = BasePage(num_columns)
        self._base_row_group = [init_base]
        self._tail_row_group = [init_tail]
        self._base_rid = base_rid
        self._next_tail_page_rid = base_rid + RECORDS_PER_PAGE_RANGE

    def add_new_row(self, is_base: bool):
        new_row = BasePage(self.num_columns)
        if is_base:
            self._base_row_group.append(new_row)
        else:
            self._tail_row_group.append(new_row)

    def is_full(self):
        return len(self._base_row_group) >= RANGE_SIZE and \
                self._base_row_group[-1].is_full()

    # Returns the row number of the current row
    def find_offset_index(self, is_base: bool):
        if is_base:
            return len(self._base_row_group) - 1
        else:
            return len(self._tail_row_group) - 1

    # Returns the current row
    def find_offset_data(self, is_base: bool):
        if is_base:
            return self._base_row_group[-1]
        else:
            return self._tail_row_group[-1]

    # Adds a record to a page range
    def add_record(self, record, schema_encoder):
        if self.is_full():
            return
        cur_row = self.find_offset_data(True)
        if cur_row.is_full():
            self._base_row_group.append(BasePage(self.num_columns))
            cur_row = self.find_offset_data(True)
        cur_row[INDIRECTION_COLUMN].add_value(0)
        cur_row[RID_COLUMN].add_value(record.rid)
        cur_row[TIMESTAMP_COLUMN].add_value(int(time()))
        cur_row[SCHEMA_ENCODING_COLUMN].add_value(schema_encoder)
        for i in range(self.num_columns):
            cur_row[4 + i].add_value(record.columns[i])

    # Updates a record to a tail page row
    def update_record(self, record, schema_encoder, indirection_column):
        cur_row = self.find_offset_data(False)
        if cur_row.is_full():
            self._tail_row_group.append(BasePage(self.num_columns))
            cur_row = self.find_offset_data(False)
        cur_row[INDIRECTION_COLUMN].add_value(indirection_column)
        cur_row[RID_COLUMN].add_value(record.rid)
        cur_row[TIMESTAMP_COLUMN].add_value(int(time()))
        cur_row[SCHEMA_ENCODING_COLUMN].add_value(schema_encoder)
        for i in range(self.num_columns):
            cur_row[4 + i].add_value(record.columns[i])

    def _get_current_base_page(self):
        return self._base_row_group[-1]

    def _get_current_tail_page(self):
        return self._tail_row_group[-1]

    def _add_base_page(self):
        new_base_page = BasePage(self.num_columns)
        self._base_row_group.append(new_base_page)

    def _add_tail_page(self):
        new_tail_page = BasePage(self.num_columns)
        self._tail_row_group.append(new_tail_page)

    def _get_base_page_indices(self, rid: int) -> Tuple[int, int]:
        assert rid in range(self._base_rid, self._base_rid + RECORDS_PER_PAGE_RANGE), \
                "RID out of range: " + str(rid)
        adjusted_rid = rid - self._base_rid
        base_page_index = adjusted_rid // RECORDS_PER_BIG_PAGE
        base_page_slot_index = adjusted_rid % RECORDS_PER_BIG_PAGE
        return base_page_index, base_page_slot_index

    def _get_tail_page_indices(self, rid: int) -> Tuple[int, int]:
        assert rid >= self._base_rid + RECORDS_PER_PAGE_RANGE, \
                "Invalid tail page RID: " + str(rid)
        adjusted_rid = rid - self._base_rid - RECORDS_PER_PAGE_RANGE
        tail_page_index = adjusted_rid // RECORDS_PER_BIG_PAGE
        tail_page_slot_index = adjusted_rid % RECORDS_PER_BIG_PAGE
        return tail_page_index, tail_page_slot_index

    def append_record(self, rid: int, *column_values: int) -> None:
        assert not self.is_full(), "Page range is full"
        if self._get_current_base_page().is_full():
            self._add_base_page()
        self._get_current_base_page().append_record(rid, *column_values)

    def update_record_new(self, rid: int, *column_values: int) -> None:
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        base_page = self._base_row_group[base_page_index]
        assert base_page.get_rid(base_page_slot_index) == rid, "RIDs don't match"
        last_update_rid = base_page.get_indirection_pointer(base_page_slot_index)
        tail_rid = self._next_tail_page_rid
        self._append_record_to_tail_page(last_update_rid, *column_values)
        base_page.update_indirection_pointer(base_page_slot_index, tail_rid)

    def _append_record_to_tail_page(self, indirection_pointer, *column_values: int) -> None:
        if self._get_current_tail_page().is_full():
            self._add_tail_page()
        self._get_current_tail_page().append_record(self._next_tail_page_rid, *column_values)
        _, slot_index = self._get_tail_page_indices(self._next_tail_page_rid)
        self._get_current_tail_page().update_indirection_pointer(slot_index, indirection_pointer)
        self._next_tail_page_rid += 1

    def read_record(self, rid: int) -> Sequence[int]:
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        base_page = self._base_row_group[base_page_index]
        last_update_rid = base_page.get_indirection_pointer(base_page_slot_index)
        if last_update_rid == rid:
            return base_page.read_record(base_page_slot_index)
        return self._read_updated_record(last_update_rid)

    def delete_record(self, rid: int) -> None:
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        base_page = self._base_row_group[base_page_index]
        base_page.invalidate_record(base_page_slot_index)
        # TODO invalidate all of its tail records as well

    def _read_updated_record(self, rid: int) -> Sequence[int]:
        # assuming cumulative for now
        # may need to fix this depending on how we implement updates
        tail_page_index, tail_page_slot_index = self._get_tail_page_indices(rid)
        tail_page = self._tail_row_group[tail_page_index]
        return tail_page.read_record(tail_page_slot_index)

    def __iter__(self):
        for row in self._base_row_group:
            for rid in row.get_column(RID_COLUMN):
                yield self.read_record(rid)

    def get_field(self, column_index: int, rid: int) -> int:
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        base_page = self._base_row_group[base_page_index]
        last_update_rid = base_page.get_indirection_pointer(base_page_slot_index)
        if last_update_rid == rid:
            return base_page.get_field(column_index, base_page_slot_index)
        tail_page_index, tail_page_slot_index = self._get_tail_page_indices(rid)
        tail_page = self._tail_row_group[tail_page_index]
        return tail_page.get_field(column_index, tail_page_slot_index)

    def get_column(self, column_index: int) -> Iterator[int]:
        for row in self._base_row_group:
            for rid in row.get_column(RID_COLUMN):
                yield self.get_field(column_index, rid)
