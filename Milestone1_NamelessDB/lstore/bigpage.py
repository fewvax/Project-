
from time import time
from typing import Tuple, Sequence, Iterator

from lstore.config import *
from lstore.page import Page

# BigPage is a collection of physical pages
class BigPage:
    def __init__(self):
        self._pages = []
        for i in range(PAGES_PER_BIG_PAGE):
            self._pages.append(Page())
        self._current_page_index = 0

    def __getitem__(self, key):
        return self._pages[key]

    def __setitem__(self, key, value):
        self._pages[key] = value

    def is_full(self):
        return not self._pages[-1].has_capacity()

    # Returns the page number of the current physical page
    # Logic: Pages fill one at a time, so if a page is empty, it means that the previous page was or is being filled
    def find_offset_index(self):
        for i in range(1, PAGES_PER_BIG_PAGE):
            if self._pages[i].get_num_records() == 0:
                return i - 1
        return PAGES_PER_BIG_PAGE - 1

    # Returns a physical page
    def find_offset_data(self):
        index = self.find_offset_index()
        return self._pages[index]

    def _get_current_page(self) -> Page:
        return self._pages[self._current_page_index]

    def _advance_current_page(self) -> None:
        self._current_page_index += 1

    def _get_page_indices(self, slot_index: int) -> Tuple[int, int]:
        assert slot_index in range(RECORDS_PER_BIG_PAGE), \
                "slot_index out of range: " + str(slot_index)
        page_index = slot_index // MAX_RECORDS
        page_slot_index = slot_index % MAX_RECORDS
        return page_index, page_slot_index

    def read_slot(self, slot_index: int) -> int:
        page_index, page_slot_index = self._get_page_indices(slot_index)
        page = self._pages[page_index]
        return page.read_int(page_slot_index)

    def write_slot(self, slot_index: int, value: int) -> None:
        page_index, page_slot_index = self._get_page_indices(slot_index)
        page = self._pages[page_index]
        return page.write_to_pos(value, page_slot_index)

    def append_value(self, value: int) -> None:
        assert not self.is_full(), "BigPage is full"
        if not self._get_current_page().has_capacity():
            self._advance_current_page()
        self._get_current_page().write(value)

    # Adds a new value to a big (base or tail) page
    def add_value(self, value):
        if self.is_full():
            raise Exception("BigPage.add_value: Attempted to add value to full page")
        if not self._get_current_page().has_capacity():
            self._advance_current_page()
        self._get_current_page().write(value)

    # returns a value in the page
    def read_bytes(self, page_num, offset):
        if page_num >= PAGES_PER_BIG_PAGE or page_num < 0:
            raise Exception("BigPage.read_bytes: Index out of range")
        if page_num >= self.find_offset_index():
            raise Exception("BigPage.read_bytes: Index beyond written pages")
        return self._pages[page_num].read_bytes(offset)

    def __iter__(self):
        for page in self._pages:
            yield from page
            if page.has_capacity():
                return

# Refers to a row of big pages, spans across multiple columns in the database
# + 4 is done to account for the 4 metadata columns in lines 5 to 8
class BasePage:
    def __init__(self, num_columns):
        self._big_pages = []
        for i in range(num_columns + METADATA_COLUMN_COUNT):
            self._big_pages.append(BigPage())

    def __getitem__(self, key):
        return self._big_pages[key]

    def __setitem__(self, key, value):
        self._big_pages[key] = value

    def is_full(self):
        return self._big_pages[0].is_full()

    # Returns the physical page number by checking the first big page of the row
    def find_offset_index(self):
        return self._big_pages[0].find_offset_index()

    # Returns the physical page
    def find_offset_data(self):
        return self._big_pages[0].find_offset_data()

    def _get_time_milliseconds(self):
        return int(time() * 1000)

    def _determine_schema_encoding(self, column_values: [int]) -> int:
        schema = 0
        for i in range(0, len(column_values)):
            if column_values[i] is not None:
                schema += 2 ** (len(column_values) - i - 1)
        return schema
    
    def read_record(self, slot_index: int) -> Sequence[int]:
        record = []
        for big_page in self._big_pages:
            record.append(big_page.read_slot(slot_index))
        return record

    def append_record(self, rid: int, *column_values: int) -> None:
        expected_column_count = len(self._big_pages) - METADATA_COLUMN_COUNT
        assert len(column_values) == expected_column_count, \
                "The number of columns do not match.\n" \
                "Expected " + str(expected_column_count) + \
                " Received " + str(len(column_values))
        self._big_pages[INDIRECTION_COLUMN].append_value(rid)
        self._big_pages[RID_COLUMN].append_value(rid)
        schema_encoding = self._determine_schema_encoding(column_values)
        self._big_pages[SCHEMA_ENCODING_COLUMN].append_value(schema_encoding)
        self._big_pages[TIMESTAMP_COLUMN].append_value(self._get_time_milliseconds())
        for i in range(expected_column_count):
            self._big_pages[i + METADATA_COLUMN_COUNT].append_value(column_values[i])

    def invalidate_record(self, slot_index: int) -> None:
        rid_column = self._big_pages[RID_COLUMN]
        schema_encoding_column = self._big_pages[SCHEMA_ENCODING_COLUMN]
        rid_column.write_slot(slot_index, NULL_VALUE)
        schema_encoding_column.write_slot(slot_index, NULL_VALUE)

    def update_indirection_pointer(self, slot_index: int, indirection_pointer: int) -> None:
        indirection_column = self._big_pages[INDIRECTION_COLUMN]
        indirection_column.write_slot(slot_index, indirection_pointer)

    def get_column(self, column_index: int) -> Iterator[int]:
        assert column_index in range(len(self._big_pages)), \
                "column_index out of range: " + str(column_index)
        return iter(self._big_pages[column_index])

    def get_field(self, column_index: int, slot_index: int) -> int:
        assert column_index in range(len(self._big_pages)), \
                "column_index out of range: " + str(column_index)
        column = self._big_pages[column_index]
        return column.read_slot(slot_index)

    def get_indirection_pointer(self, slot_index: int) -> int:
        return self.get_field(INDIRECTION_COLUMN, slot_index)

    def get_rid(self, slot_index: int) -> int:
        return self.get_field(RID_COLUMN, slot_index)

    def __iter__(self):
        for i in range(RECORDS_PER_BIG_PAGE):
            try:
                yield self.read_record(i)
            except Exception as e:
                return
