from pathlib import Path
from threading import Lock
from typing import Sequence, Iterator, List
import os
import queue, threading

from lstore.config import *
from lstore.pagerange import PageRange
from lstore.index import Index
from lstore.pagedirectory import PageDirectory


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        return str(self.columns)


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, path, next_rid=START_RID):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.index = Index(self)
        self._next_rid = next_rid
        self.path = Path(path)
        self.page_directory = PageDirectory()
        self._latch = Lock()
        self._page_ranges = []
        self.merge_queue = queue.Queue()
        self.ranges_inside_queue = set()
        self._merge_thread = threading.Thread(target=self._merge, daemon=True)
        self._merge_thread.start()

    def create_directory(self, path):
        new_path = path + "/" + self.name
        try:
            os.makedirs(new_path)
        except FileExistsError:
            pass
        filename = new_path + "/metadata.txt"
        file = open(filename, "w")
        file.write(self.name + "\n" + str(self.num_columns) + "\n" + str(self.key) + "\n" + str(self._next_rid))

    def get_next_rid(self):
        with self._latch:
            return self._next_rid

    def update_next_rid(self, rid):
        self._next_rid = rid

    def _create_new_page_range(self):
        next_range_index = len(self._page_ranges)
        range_base_rid = next_range_index * RECORDS_PER_PAGE_RANGE + START_RID
        range_dir_name = str(next_range_index)
        range_path = self.path / range_dir_name
        range_path.mkdir()
        subdirectory_paths = [range_path / BASE_META_DIRECTORY_NAME, range_path / BASE_DATA_DIRECTORY_NAME,
                              range_path / TAIL_META_DIRECTORY_NAME, range_path / TAIL_DATA_DIRECTORY_NAME]
        for path in subdirectory_paths:
            path.mkdir()
        self.page_directory[next_range_index] = [0] * RANGE_SIZE
        version_nums = self.page_directory[next_range_index]
        new_page_range = PageRange(self.num_columns, range_path, version_nums, base_rid=range_base_rid)
        self._page_ranges.append(new_page_range)

    def _get_current_page_range(self):
        return self._page_ranges[-1]

    def append_record(self, *column_values: int) -> None:
        with self._latch:
            if self._get_current_page_range().is_full():
                self._create_new_page_range()
            self._get_current_page_range().append_record(self._next_rid, *column_values)
            self._next_rid += 1
            return self._next_rid - 1

    def _get_page_range_index_of_rid(self, rid: int) -> int:
        assert rid >= 0, \
            "RID out of range: " + str(rid)
        page_range_index = (rid - START_RID) // RECORDS_PER_PAGE_RANGE
        assert page_range_index in range(len(self.page_directory)), \
            "RID out of range: " + str(rid)
        return page_range_index

    def _get_page_range_of_rid(self, rid: int) -> PageRange:
        page_range_index = self._get_page_range_index_of_rid(rid)
        return self._page_ranges[page_range_index]

    def read_raw_record(self, rid: int) -> Sequence[int]:
        page_range = self._get_page_range_of_rid(rid)
        return page_range.read_record(rid)

    def read_record(self, rid: int) -> Record:
        raw_record = self.read_raw_record(rid)
        # check if record has been deleted?
        columns = raw_record[METADATA_COLUMN_COUNT:]
        return Record(rid, self.key, columns)

    def delete_record(self, rid: int) -> None:
        page_range = self._get_page_range_of_rid(rid)
        page_range.delete_record(rid)

    def update_record(self, rid: int, *column_values: int) -> None:
        page_range = self._get_page_range_of_rid(rid)
        page_range_index = self._get_page_range_index_of_rid(rid)
        tail_full = page_range.update_record(rid, *column_values)
        if page_range_index not in self.ranges_inside_queue:
            if tail_full:
                #print("PAGE_RANGE_INDEX IN UPDATE: ", page_range_index)
                self.ranges_inside_queue.add(page_range_index)
                self.merge_queue.put(page_range_index)

    def update_versions(self, page_range_index: int, updated_base_page_indices: List[bool]):
        for base_page_num in range(RANGE_SIZE):
            if updated_base_page_indices[base_page_num]:
                self.page_directory.update_version_number(page_range_index, base_page_num)

    def undo_update(self, rid: int) -> None:
        page_range = self._get_page_range_of_rid(rid)
        page_range.undo_update(rid)

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

    def __iter__(self):
        for page_range in self._page_ranges:
            yield from page_range

    @classmethod
    def read_table_from_dir(cls, path):
        metadata_file_path = path / "metadata.txt"
        with metadata_file_path.open() as metadata_file:
            metadata = metadata_file.readline().split()
            name = path.name
            num_columns = int(metadata[0])
            key = int(metadata[1])
            next_rid = int(metadata[2])
        table = Table(name, num_columns, key, path, next_rid)
        table.page_directory = PageDirectory.read_from_dir(path)
        table._create_page_ranges_from_files()
        table.index.create_index(key)
        return table

    def _get_table_metadata_str(self):
        return str(self.num_columns) + " " + str(self.key) + " " + str(self._next_rid)

    @classmethod
    def create_new_table(cls, num_columns, key, path):
        path = Path(path)
        path.mkdir()
        metadata_file_path = path / "metadata.txt"
        next_rid = START_RID
        with metadata_file_path.open(mode="w") as metadata_file:
            metadata_str = str(num_columns) + " " + str(key) + " " + str(next_rid)
            metadata_file.write(metadata_str)
            metadata_file.write("\n")
        name = path.name
        table = Table(name, num_columns, key, path, next_rid)
        table._create_new_page_range()
        table.index.create_index(key)
        return table

    def _create_page_ranges_from_files(self):

        metadata_file_path = self.path / "metadata.txt"
        with metadata_file_path.open() as metadata_file:
            lines = metadata_file.readlines()
            if len(lines) > 1:
                for i in range(1, len(lines)):
                    range_counter = i - 1
                    # range_path = paths_dict[range_counter]
                    range_path = self.path / str(range_counter)
                    range_metadata = lines[i].split()
                    num_base_pages = int(range_metadata[0])
                    num_tail_records = int(range_metadata[1])
                    range_base_rid = range_counter * RECORDS_PER_PAGE_RANGE + START_RID
                    version_nums = self.page_directory[range_counter]
                    page_range = PageRange(self.num_columns, range_path, version_nums, num_base_pages, num_tail_records,
                                           range_base_rid)
                    # self.page_directory[range_counter] = page_range
                    # TODO: pass version list to page range
                    self._page_ranges.append(page_range)

    def close(self):
        metadata_file_path = self.path / "metadata.txt"
        with metadata_file_path.open(mode="w") as metadata_file:
            metadata_file.write(self._get_table_metadata_str())
            metadata_file.write("\n")
            for page_range in self._page_ranges:
                range_metadata = page_range.get_metadata()
                range_metadata_str = " ".join(map(str, range_metadata))
                metadata_file.write(range_metadata_str)
                metadata_file.write("\n")
        self.page_directory.write_to_dir(self.path)

    def _merge(self):
        while True:
            range_index = self.merge_queue.get()
            self.ranges_inside_queue.remove(range_index)
            self._page_ranges[range_index].merge()

    def __str__(self):
        s = "I\tR\tTimestamp\tSE\t"
        for i in range(0, self.num_columns):
            if self.key == i:
                s += "K"
            s += ("C" + str(i + 1) + "\t")
        s += "\n"
        s += ("_" * (4 * (self.num_columns + 4) + 6) + "\n")
        for key in list(self.page_directory.keys()):
            r = self.page_directory[key]
            s += str(r)
        return s
