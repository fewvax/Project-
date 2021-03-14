from pathlib import Path
from time import time
from threading import Lock
from typing import Tuple, Iterator, Sequence

from lstore.basepage import BasePage, Page
from lstore.config import *
from lstore.bufferpool import bufferpool
from lstore.bufferpool import BufferPool


# Unlimited tail rows, RANGE_SIZE base rows
class PageRange:
    def __init__(self, num_columns, page_range_data_path, version_nums, num_base_pages=0, num_tail_records=0, base_rid=0):
        self.num_columns = num_columns
        self._base_rid = base_rid
        self._next_tail_page_rid = base_rid + RECORDS_PER_PAGE_RANGE + num_tail_records
        self._page_range_data_path = Path(page_range_data_path)
        self._base_pages_counter = num_base_pages
        self._version_nums = version_nums
        if self._base_pages_counter == 0:
            self._tail_pages_counter = 0
            self._add_base_page()
            self._add_tail_page()
        else:
            self._tail_pages_counter = num_tail_records // MAX_RECORDS + 1
        self._latch = Lock()

    def get_metadata(self):
        return [self._base_pages_counter, self._next_tail_page_rid - (self._base_rid + RECORDS_PER_PAGE_RANGE)]

    def get_path(self):
        return self._page_range_data_path

    def _get_base(self, index: int) -> BasePage:
        base_index = (index, self._version_nums[index])
        base_page = bufferpool.get_base_page(base_index, self._page_range_data_path)
        return base_page

    def _get_tail(self, index: int) -> BasePage:
        tail_index = (index, 0)
        tail_page = bufferpool.get_tail_page(tail_index, self._page_range_data_path)
        return tail_page

    def is_full(self):
        if self._base_pages_counter == RANGE_SIZE:
            with self._get_base(RANGE_SIZE - 1) as base_page:
                return base_page.is_full()
        return False

    def _get_current_base_page(self):
        index = self._base_pages_counter - 1
        return self._get_base(index)

    def _get_current_tail_page(self):
        index = self._tail_pages_counter - 1
        return self._get_tail(index)

    def _add_base_page(self):
        version_num = 0
        index = (self._base_pages_counter, version_num)
        bufferpool.new_base_page(index, self._page_range_data_path, self.num_columns + METADATA_COLUMN_COUNT)
        self._base_pages_counter += 1

    def _add_tail_page(self):
        version_num = 0
        index = (self._tail_pages_counter, version_num)
        bufferpool.new_tail_page(index, self._page_range_data_path, self.num_columns + METADATA_COLUMN_COUNT)
        self._tail_pages_counter += 1

    def _is_base_rid(self, rid: int) -> bool:
        return rid in range(self._base_rid, self._base_rid + RECORDS_PER_PAGE_RANGE)

    def _get_base_page_indices(self, rid: int) -> Tuple[int, int]:
        assert self._is_base_rid(rid), \
            "RID out of range: " + str(rid)
        adjusted_rid = rid - self._base_rid
        base_page_index = adjusted_rid // MAX_RECORDS
        base_page_slot_index = adjusted_rid % MAX_RECORDS
        return base_page_index, base_page_slot_index

    def _get_tail_page_indices(self, rid: int) -> Tuple[int, int]:
        assert rid >= self._base_rid + RECORDS_PER_PAGE_RANGE, \
            "Invalid tail page RID: " + str(rid)
        adjusted_rid = rid - self._base_rid - RECORDS_PER_PAGE_RANGE
        tail_page_index = adjusted_rid // MAX_RECORDS
        tail_page_slot_index = adjusted_rid % MAX_RECORDS
        return tail_page_index, tail_page_slot_index

    def append_record(self, rid: int, *column_values: int) -> None:
        assert not self.is_full(), "Page range is full"
        with self._get_current_base_page() as current_base_page:
            if current_base_page.is_full():
                self._add_base_page()
        with self._get_current_base_page() as base_page:
            base_page.append_record(rid, *column_values)

    def _determine_schema_encoding(self, column_values: [int]) -> int:
        schema = 0
        for i, value in enumerate(column_values):
            if value != NULL_VALUE:
                schema |= 1 << (63 - i)
        return schema

    def update_record(self, rid: int, *column_values: int) -> None:
        def create_snapshot_record():
            current_columns = self.read_record(rid)[METADATA_COLUMN_COUNT:]
            update_rid, tail_full = self._append_record_to_tail_page(rid, *current_columns, schema_encoding=0, base_rid=NULL_VALUE)
            base_page.update_indirection_pointer(base_page_slot_index, update_rid)
            return update_rid, tail_full
        def calculate_new_schema_encoding():
            schema_encoding = self._determine_schema_encoding(column_values)
            schema_encoding |= self.get_field(SCHEMA_ENCODING_COLUMN, rid)
            return schema_encoding
        def determine_cumulative_columns(last_update_rid: int):
            tail_page_index, tail_page_slot_index = self._get_tail_page_indices(last_update_rid)
            with self._get_tail(tail_page_index) as tail_page:
                result = tail_page.read_record(tail_page_slot_index)[METADATA_COLUMN_COUNT:]
                for i, value in enumerate(column_values):
                    if value != NULL_VALUE:
                        result[i] = value
            return result
        self._assert_not_deleted(rid)
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        with self._get_base(base_page_index) as base_page:
            assert base_page.get_rid(base_page_slot_index) == rid, "RIDs don't match"
            last_update_rid = base_page.get_indirection_pointer(base_page_slot_index)
            tail_full1 = False
            if last_update_rid == rid:
                last_update_rid, tail_full1 = create_snapshot_record()
            schema_encoding = calculate_new_schema_encoding()
            columns = determine_cumulative_columns(last_update_rid)
            # add base_rid to append_record_to_tail_page
            tail_rid, tail_full2 = self._append_record_to_tail_page(last_update_rid, *columns, schema_encoding=schema_encoding, base_rid=rid)
            base_page.update_indirection_pointer(base_page_slot_index, tail_rid)
            base_page.update_schema_encoding(base_page_slot_index, schema_encoding)
            if tail_full1 != False:
                return tail_full1
            return tail_full2

    def _append_record_to_tail_page(self, indirection_pointer, *column_values: int, schema_encoding: int=0, base_rid: int=NULL_VALUE) -> None:
        with self._latch:
            with self._get_current_tail_page() as current_tail_page:
                assert not current_tail_page.is_full()
                current_tail_page.append_record(self._next_tail_page_rid, *column_values, schema_encoding=schema_encoding)
                _, slot_index = self._get_tail_page_indices(self._next_tail_page_rid)
                current_tail_page.update_indirection_pointer(slot_index, indirection_pointer)
                current_tail_page.update_base_rid(slot_index, base_rid)
                self._next_tail_page_rid += 1
                tail_full = False
                if current_tail_page.is_full():
                    self._add_tail_page()
                    tail_full = True
                return self._next_tail_page_rid - 1, tail_full

    def _assert_not_deleted(self, rid):
        if self._is_record_deleted(rid):
            raise InvalidRIDException("Record with rid " + str(rid) + " is deleted")

    def read_base_record(self, rid: int) -> Sequence[int]:
        self._assert_not_deleted(rid)
        self._assert_not_deleted(rid)
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        with self._get_base(base_page_index) as base_page:
            base_record = base_page.read_record(base_page_slot_index)
            return base_record 

    def undo_update(self, rid: int):
        self._assert_not_deleted(rid)
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        with self._get_base(base_page_index) as base_page:
            assert base_page.get_rid(base_page_slot_index) == rid, "RIDs don't match"
            last_update_rid = base_page.get_indirection_pointer(base_page_slot_index)
            tail_page_index, tail_page_slot_index = self._get_tail_page_indices(last_update_rid)
            with self._get_tail(tail_page_index) as tail_page:
                last_version = tail_page.get_indirection_pointer(tail_page_slot_index)
                last_schema = tail_page.get_schema_encoding(tail_page_slot_index)
                base_page.update_indirection_pointer(base_page_slot_index, last_version)
                base_page.update_schema_encoding(base_page_slot_index, last_schema)

    def read_record(self, rid: int) -> Sequence[int]:
        def build_updated_record():
            updated_record = self._read_updated_record(last_update_rid)
            # keep the metadata columns from the base record
            # we want the rid to be the base rid
            result = base_record[:METADATA_COLUMN_COUNT]
            for i in range(METADATA_COLUMN_COUNT, len(base_record)):
                if updated_record[i] == NULL_VALUE:
                    result.append(base_record[i])
                else:
                    result.append(updated_record[i])
            return result

        self._assert_not_deleted(rid)
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        with self._get_base(base_page_index) as base_page:
            last_update_rid = base_page.get_indirection_pointer(base_page_slot_index)
            tps = base_page.get_tps(base_page_slot_index)
            base_record = base_page.read_record(base_page_slot_index)
            if last_update_rid == rid or (tps >= last_update_rid and tps != NULL_VALUE):
                return base_record
        return build_updated_record()

    def delete_record(self, rid: int) -> None:
        self._assert_not_deleted(rid)
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        with self._get_base(base_page_index) as base_page:
            last_update_rid = base_page.get_indirection_pointer(base_page_slot_index)
            self._invalidate_tail_record(last_update_rid, rid)
            base_page.invalidate_record(base_page_slot_index)

    def _invalidate_tail_record(self, rid: int, base_rid: int) -> None:
        if rid == base_rid:
            return
        tail_page_index, tail_page_slot_index = self._get_tail_page_indices(rid)
        with self._get_tail(tail_page_index) as tail_page:
            previous_record_rid = tail_page.get_indirection_pointer(tail_page_slot_index)
            tail_page.invalidate_record(tail_page_slot_index)
        self._invalidate_tail_record(previous_record_rid, base_rid)

    def _read_updated_record(self, rid: int) -> Sequence[int]:
        tail_page_index, tail_page_slot_index = self._get_tail_page_indices(rid)
        with self._get_tail(tail_page_index) as tail_page:
            return tail_page.read_record(tail_page_slot_index)

    def _is_record_deleted(self, rid: int) -> bool:
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        with self._get_base(base_page_index) as base_page:
            indirection_pointer = base_page.get_indirection_pointer(base_page_slot_index)
        return indirection_pointer == NULL_VALUE

    def _is_field_updated(self, schema_encoding: int, column_index: int) -> bool:
        mask = 1
        bit_index = 63 - column_index
        return ((schema_encoding >> bit_index) & 1) == 1

    def get_field(self, column_index: int, rid: int) -> int:
        def get_field_from_tail_record():
            tail_page_index, tail_page_slot_index = self._get_tail_page_indices(last_update_rid)
            with self._get_tail(tail_page_index) as tail_page:
                return tail_page.get_field(column_index, tail_page_slot_index)

        self._assert_not_deleted(rid)
        base_page_index, base_page_slot_index = self._get_base_page_indices(rid)
        with self._get_base(base_page_index) as base_page:
            last_update_rid = base_page.get_indirection_pointer(base_page_slot_index)
            if last_update_rid == rid or column_index < METADATA_COLUMN_COUNT:
                # return meta fields from the base record to be consistent with read record
                return base_page.get_field(column_index, base_page_slot_index)

            schema_encoding = base_page.get_field(SCHEMA_ENCODING_COLUMN, base_page_slot_index)
            if self._is_field_updated(schema_encoding, column_index - METADATA_COLUMN_COUNT):
                return get_field_from_tail_record()
            return base_page.get_field(column_index, base_page_slot_index)

    def get_column(self, column_index: int) -> Iterator[int]:
        for i in range(self._base_pages_counter):
            with self._get_base(i) as base_page_row:
                for rid in base_page_row.get_page(RID_COLUMN):
                    if not self._is_record_deleted(rid):
                        yield self.get_field(column_index, rid)

    def _print(self):
        for i in range(self._base_pages_counter):
            with self._get_base(i) as base_page:
                for record in base_page:
                    print(record)
        print()
        for i in range(self._tail_pages_counter):
            with self._get_tail(i) as tail_page:
                for record in tail_page:
                    print(record)

    def __iter__(self):
        for rid in self.get_column(RID_COLUMN):
            yield self.read_record(rid)

    # reads without using bufferpool object
    def read_basepage(self, index: Tuple[int, int]):
        return BufferPool.load_base_page(index, self.get_path())

    def read_tailpage(self, index: Tuple[int, int]):
        return BufferPool.load_tail_page(index, self.get_path())

    def write_basepage(self, index: Tuple[int, int], basepage: BasePage):
        basepage_bytes = BasePage.to_data_bytes_merge(basepage)
        BufferPool._write_base_page_data(index, self._page_range_data_path, basepage_bytes)

    def merge(self):
        bufferpool.flush_before_merge()
        merged_page = BasePage(self.num_columns)
        base_pages = []
        basepage_full = dict()
        for base_page_num in range(self._base_pages_counter):
            index = (base_page_num, self._version_nums[base_page_num])
            base_pages.append(self.read_basepage(index))
            basepage_full[base_page_num] = base_pages[base_page_num].is_full()
        index = (self._tail_pages_counter-1, 0)
        last_tail_page = self.read_tailpage(index)
        while last_tail_page.get_num_records() == 0:
            index = (index[0]-1, index[1])
            last_tail_page = self.read_tailpage(index)
        last_tail_record = last_tail_page.read_record(last_tail_page.get_num_records()-1)
        tps_num = last_tail_record[RID_COLUMN]
        merged_rids = dict()
        updated_basepages = [False]*RANGE_SIZE
        break_outer = False
        for tail_page_index in reversed(range(self._tail_pages_counter)):
            index = (tail_page_index, 0)
            curr_tail_page = self.read_tailpage(index)
            for tail_num in reversed(range(curr_tail_page.num_records)):
                curr_tail_record = curr_tail_page.read_record(tail_num)
                curr_tid = curr_tail_record[RID_COLUMN]
                curr_base_rid = curr_tail_record[BASE_RID_COLUMN]
                if curr_base_rid not in merged_rids and curr_base_rid != NULL_VALUE:
                    base_page_index, base_page_slot_index = self._get_base_page_indices(curr_base_rid)
                    merged_rids[curr_base_rid] = True
                    if basepage_full[base_page_index]:
                        curr_base_page = base_pages[base_page_index]
                        curr_base_page.edit_record_values(base_page_slot_index, *curr_tail_record[METADATA_COLUMN_COUNT:])
                        curr_tps = curr_base_page.get_tps(base_page_slot_index)
                        curr_base_page.update_tps(base_page_slot_index, tps_num)
                        if curr_tid <= curr_tps and curr_tps != NULL_VALUE:
                            break_outer = True
                            break                      
                        updated_basepages[base_page_index] = True
            if break_outer:
                break
        for base_page_index in range(len(updated_basepages)):
            if updated_basepages[base_page_index]:
                curr_version = self._version_nums[base_page_index] + 1
                index = (base_page_index, curr_version)
                curr_basepage = base_pages[base_page_index]
                self.write_basepage(index, curr_basepage)

    def __str__(self):
        s = ""
        for i in range(self._base_pages_counter):
            base = self._get_base(i)
            s += str(base)
        s += ("_" * (4 * (self.num_columns + 4) + 3) + "\n")
        for i in range(self._tail_pages_counter):
            tail = self._get_tail(i)
            s += str(tail)
        s += ("_" * (4 * (self.num_columns + 4) + 6) + "\n")
        return s


class InvalidRIDException(Exception):
    pass


def set_bufferpool(bp):
    global bufferpool
    bufferpool = bp
