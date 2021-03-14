
from time import time
from typing import Tuple, Sequence, Iterator

from lstore.config import *
from lstore.page import Page

class BasePage:
    def __init__(self, num_columns):
        self._pages = []
        self.num_columns = num_columns + METADATA_COLUMN_COUNT
        for i in range(self.num_columns):
            self._pages.append(Page())
        self.is_dirty = False

    def get_num_records(self):
        return self._pages[0].get_num_records()

    def __getitem__(self, item):
        return self._pages[item]

    def __setitem__(self, key, value):
        self._pages[key] = value

    def is_full(self):
        return not self._pages[0].has_capacity()

    def _get_time_milliseconds(self):
        return int(time() * 1000)

    def read_record(self, slot_index: int) -> Sequence[int]:
        record = []
        for page in self._pages:
            record.append(page[slot_index])
        return record

    def append_record(self, rid: int, *column_values: int, schema_encoding: int = 0) -> None:
        expected_column_count = len(self._pages) - METADATA_COLUMN_COUNT
        assert len(column_values) == expected_column_count, \
                "The number of columns do not match.\n" \
                "Expected " + str(expected_column_count) + \
                " Received " + str(len(column_values))
        self._pages[RID_COLUMN].write(rid)
        self._pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding)
        self._pages[TIMESTAMP_COLUMN].write(self._get_time_milliseconds())
        self._pages[BASE_RID_COLUMN].write(0)
        self._pages[TPS_COLUMN].write(NULL_VALUE)
        for i in range(expected_column_count):
            self._pages[i + METADATA_COLUMN_COUNT].write(column_values[i])
        self._pages[INDIRECTION_COLUMN].write(rid)
        self.is_dirty = True

    def edit_record_values(self, slot_index: int, *column_values: int):
        expected_column_count = len(self._pages) - METADATA_COLUMN_COUNT
        assert len(column_values) == expected_column_count, \
                "The number of columns do not match.\n" \
                "Expected " + str(expected_column_count) + \
                " Received " + str(len(column_values))
        for i in range(len(self._pages)-METADATA_COLUMN_COUNT):
            self._pages[i + METADATA_COLUMN_COUNT].write_to_pos(column_values[i], slot_index)

    def invalidate_record(self, slot_index: int) -> None:
        self.update_indirection_pointer(slot_index, NULL_VALUE)

    def update_indirection_pointer(self, slot_index: int, indirection_pointer: int) -> None:
        indirection_column = self._pages[INDIRECTION_COLUMN]
        indirection_column.write_to_pos(indirection_pointer, slot_index)
        self.is_dirty = True

    def update_base_rid(self, slot_index: int, base_rid: int) -> None:
        base_rid_column = self._pages[BASE_RID_COLUMN]
        base_rid_column.write_to_pos(base_rid, slot_index)

    def update_tps(self, slot_index: int, tps: int) -> None:
        tps_column = self._pages[TPS_COLUMN]
        tps_column.write_to_pos(tps, slot_index)

    def update_schema_encoding(self, slot_index: int, schema_encoding: int) -> None:
        schema_column = self._pages[SCHEMA_ENCODING_COLUMN]
        schema_column.write_to_pos(schema_encoding, slot_index)
        self.is_dirty = True

    def get_page(self, column_index: int) -> Iterator[int]:
        assert column_index in range(len(self._pages)), \
                "column_index out of range: " + str(column_index)
        return self._pages[column_index]

    def get_field(self, column_index: int, slot_index: int) -> int:
        assert column_index in range(len(self._pages)), \
                "column_index out of range: " + str(column_index)
        page = self._pages[column_index]
        return page[slot_index]

    def get_indirection_pointer(self, slot_index: int) -> int:
        return self.get_field(INDIRECTION_COLUMN, slot_index)

    def get_schema_encoding(self, slot_index: int) -> int:
        return self.get_field(SCHEMA_ENCODING_COLUMN, slot_index)

    def get_rid(self, slot_index: int) -> int:
        return self.get_field(RID_COLUMN, slot_index)

    def get_base_rid(self, slot_index: int) -> int:
        return self.get_field(BASE_RID_COLUMN, slot_index)

    def get_tps(self, slot_index: int) -> int:
        return self.get_field(TPS_COLUMN, slot_index)

    def __iter__(self):
        for i in range(MAX_RECORDS):
            try:
                yield self.read_record(i)
            except Exception as e:
                return

    def __str__(self):
        s = ""
        for j in range(0, MAX_RECORDS):
            for k in range(0, len(self._pages)):
                if j >= self._pages[k].get_num_records():
                    s += "0\t"
                else:
                    if self[k][j] != NULL_VALUE:
                        s += (str(self[k][j]) + "\t")
                    else:
                        s += "N\t"
            s += "\n"
        s += ("_" * (4 * len(self._pages) - 3) + "\n")
        return s

    def to_metadata_bytes(self) -> bytes:
        byte_lst = bytearray()
        for page in self._pages[:METADATA_COLUMN_COUNT]:
            byte_lst += bytes(page)
        return bytes(byte_lst)

    def to_data_bytes(self) -> bytes:
        byte_lst = bytearray()
        for page in self._pages[METADATA_COLUMN_COUNT:]:
            byte_lst += bytes(page)
        return bytes(byte_lst)

    @classmethod
    def to_data_bytes_merge(cls, base_page) -> bytes:
        byte_lst = bytearray()
        for page in base_page._pages[METADATA_COLUMN_COUNT:]:
            byte_lst += bytes(page)
        return bytes(byte_lst)

    @classmethod
    def _convert_bytes_to_page(cls, columns, metadata_bytes, data_bytes):
        lst_page = []
        for i in range(columns):
            if i < METADATA_COLUMN_COUNT:
                bytes_of_page = cls._bytes_of_pages(i, metadata_bytes)
            else:
                bytes_of_page = cls._bytes_of_pages(i-METADATA_COLUMN_COUNT, data_bytes)
            page = Page.new_page_from_bytes(bytes_of_page)
            lst_page.append(page)
        return lst_page

    @classmethod
    def _bytes_of_pages(cls, num: int, bytes_pages):
        page_offset = num * PAGE_SIZE
        next_page_offset = (num + 1) * PAGE_SIZE
        bytes_of_page = bytes_pages[page_offset:next_page_offset]
        return bytes_of_page

    @classmethod
    def new_base_page_from_byte(cls, metadata_bytes: bytes, data_bytes: bytes):
        length_column = len(metadata_bytes)+len(data_bytes)
        assert len(metadata_bytes)//PAGE_SIZE == METADATA_COLUMN_COUNT, "Error Metadata column is incorrect" + str(len(metadata_bytes)//PAGE_SIZE)
        assert (length_column) % PAGE_SIZE == 0, "Error, page size is incorrect" + str(length_column)
        columns = (length_column) // PAGE_SIZE
        lst_page = cls._convert_bytes_to_page(columns, metadata_bytes, data_bytes)
        base_page = BasePage(0)
        base_page._pages = lst_page
        base_page.num_columns = columns
        base_page.num_records = lst_page[0].get_num_records()
        return base_page

