from pathlib import Path
from threading import Lock
from typing import List, Optional, Tuple

from lstore.config import *
from lstore.basepage import BasePage

class PinnableBasePage():

    def __init__(self, base_page):
        self._base_page = base_page
        self._pin = 0
        self._lock = Lock()

    def __enter__(self):
        with self._lock:
            self._pin += 1
            return self._base_page

    def __exit__(self, type, value, traceback):
        with self._lock:
            self._pin -= 1

    def is_pinned(self):
        return self._pin > 0

    def is_dirty(self):
        return self._base_page.is_dirty

    def __bytes__(self):
        return bytes(self._base_page)

    def __str__(self):
        return str(self._base_page)

class BufferPool:

    def __init__(self):
        self.bufferpool = BUFFERPOOL_EVICTION_STRATEGY(BUFFERPOOL_SIZE)

    @classmethod
    def _create_path(cls,page_range_data_path):
        path = Path(page_range_data_path)
        assert path.exists(), "Does not exist" + str(page_range_data_path)
        return path

    @classmethod
    def _index_to_filename(cls, index: Tuple[int, int]):
        return str(index[0]) + "-" + str(index[1]) + ".bin"

    @classmethod
    def _write_file(cls, path, data: bytes):
        with path.open('wb') as data_file:
            data_file.write(data)

    @classmethod
    def _write_file_to_dir(cls, page_range_data_path, directory: str, index: Tuple[int, int], data: bytes):
        path = cls._create_path(page_range_data_path)
        filename = cls._index_to_filename(index)
        cls._write_file(path / directory / filename, data)

    def _write_base_page_metadata(self, index: Tuple[int, int], page_range_data_path, data: bytes):
        adjusted_index = (index[0], 0)
        BufferPool._write_file_to_dir(page_range_data_path, BASE_META_DIRECTORY_NAME, adjusted_index, data)

    @classmethod
    def _write_base_page_data(cls, index: Tuple[int, int], page_range_data_path, data: bytes):
        cls._write_file_to_dir(page_range_data_path, BASE_DATA_DIRECTORY_NAME, index, data)

    def _write_tail_page_metadata(self, index: Tuple[int, int], page_range_data_path, data: bytes):
        assert index[1] == 0
        BufferPool._write_file_to_dir(page_range_data_path, TAIL_META_DIRECTORY_NAME, index, data)

    def _write_tail_page_data(self, index: Tuple[int, int], page_range_data_path, data: bytes):
        assert index[1] == 0
        BufferPool._write_file_to_dir(page_range_data_path, TAIL_DATA_DIRECTORY_NAME, index, data)

    @classmethod
    def _read_file(cls, path) -> bytes:
        with path.open('rb') as data_file:
            return data_file.read()

    @classmethod
    def _read_file_from_dir(cls, page_range_data_path, directory: str, index: Tuple[int, int]):
        path = cls._create_path(page_range_data_path)
        filename = cls._index_to_filename(index)
        return cls._read_file(path / directory / filename)

    @classmethod
    def _get_base_page_metadata(cls, index: Tuple[int, int], page_range_data_path) -> bytes:
        adjusted_index = (index[0], 0)
        return cls._read_file_from_dir(page_range_data_path, BASE_META_DIRECTORY_NAME, adjusted_index)

    @classmethod
    def _get_base_page_data(cls, index: Tuple[int, int], page_range_data_path) -> bytes:
        return cls._read_file_from_dir(page_range_data_path, BASE_DATA_DIRECTORY_NAME, index)

    @classmethod
    def _get_tail_page_metadata(cls, index: Tuple[int, int], page_range_data_path) -> bytes:
        assert index[1] == 0
        return cls._read_file_from_dir(page_range_data_path, TAIL_META_DIRECTORY_NAME, index)

    @classmethod
    def _get_tail_page_data(cls, index: Tuple[int, int], page_range_data_path) -> bytes:
        assert index[1] == 0
        return cls._read_file_from_dir(page_range_data_path, TAIL_DATA_DIRECTORY_NAME, index)

    @classmethod
    def load_base_page(cls, index: Tuple[int,int], page_range_data_path) -> BasePage:
        metadata_bytes = cls._get_base_page_metadata(index, page_range_data_path)
        data_bytes = cls._get_base_page_data(index, page_range_data_path)
        return BasePage.new_base_page_from_byte(metadata_bytes, data_bytes)

    @classmethod
    def load_tail_page(cls, index: Tuple[int,int], page_range_data_path) -> BasePage:
        metadata_bytes = cls._get_tail_page_metadata(index, page_range_data_path)
        data_bytes = cls._get_tail_page_data(index, page_range_data_path)
        return BasePage.new_base_page_from_byte(metadata_bytes, data_bytes)

    def _add_frame_to_buffer_pool(self, base_page: PinnableBasePage, index: Tuple[int,int], page_range_data_path, is_base: bool):
        if self.bufferpool.is_full():
            frame = self.bufferpool.evict_frame()
            if frame.base_page.is_dirty():
                self.commit(frame)
        return self.bufferpool.add_frame(base_page, index, page_range_data_path, is_base)

    def _commit_base_page(self, frame):
        assert frame.is_base
        with frame.base_page as base_page:
            metadata_bytes = base_page.to_metadata_bytes()
            data_bytes = base_page.to_data_bytes()
            self._write_base_page_metadata(frame.index, frame.page_range_data_path, metadata_bytes)
            self._write_base_page_data(frame.index, frame.page_range_data_path, data_bytes)

    def _commit_tail_page(self, frame):
        assert not frame.is_base
        with frame.base_page as tail_page:
            metadata_bytes = tail_page.to_metadata_bytes()
            data_bytes = tail_page.to_data_bytes()
            self._write_tail_page_metadata(frame.index, frame.page_range_data_path, metadata_bytes)
            self._write_tail_page_data(frame.index, frame.page_range_data_path, data_bytes)

    def commit(self, frame):
        if frame.is_base:
            self._commit_base_page(frame)
        else:
            self._commit_tail_page(frame)

    def _new_base_page(self, index: Tuple[int,int], page_range_data_path, num_columns: int, is_base: bool) -> PinnableBasePage:
        base_page = BasePage(num_columns-METADATA_COLUMN_COUNT)
        pinnable_base_page = PinnableBasePage(base_page)
        frame = self._add_frame_to_buffer_pool(pinnable_base_page, index, page_range_data_path, is_base)
        self.commit(frame)
        return pinnable_base_page

    def new_base_page(self, index: Tuple[int,int], page_range_data_path, num_columns: int) -> PinnableBasePage:
        assert index[0] in range(RANGE_SIZE), "Invalid base page index: " + str(index)
        return self._new_base_page(index, page_range_data_path, num_columns, is_base=True)

    def new_tail_page(self, index: Tuple[int,int], page_range_data_path, num_columns: int) -> PinnableBasePage:
        assert index[0] >= 0, "Invalid tail page index: " + str(index)
        return self._new_base_page(index, page_range_data_path, num_columns, is_base=False)

    def _get_from_bufferpool(self, index: Tuple[int,int], page_range_data_path, is_base: bool) -> PinnableBasePage:
        return self.bufferpool.get_frame(index, page_range_data_path, is_base)

    def _get_base_page(self, index: Tuple[int,int], page_range_data_path, is_base: bool) -> PinnableBasePage:
        pinnable_base_page = self._get_from_bufferpool(index, page_range_data_path, is_base)
        if pinnable_base_page is None:
            if is_base:
                base_page = self.load_base_page(index, page_range_data_path)
            else:
                base_page = self.load_tail_page(index, page_range_data_path)
            pinnable_base_page = PinnableBasePage(base_page)
            self._add_frame_to_buffer_pool(pinnable_base_page, index, page_range_data_path, is_base)
        return pinnable_base_page

    def get_base_page(self, index: Tuple[int,int], page_range_data_path) -> PinnableBasePage:
        assert index[0] in range(RANGE_SIZE), "Invalid base page index" + str(index)
        return self._get_base_page(index, page_range_data_path, is_base=True)

    def get_tail_page(self, index: Tuple[int,int], page_range_data_path) -> PinnableBasePage:
        assert index[0] >=0, "Invalid tail page index" + str(index)
        return self._get_base_page(index, page_range_data_path, is_base=False)

    def flush(self):
        # todo check for pins and dirty
        for frame in self.bufferpool:
            self.commit(frame)
        self.bufferpool = BUFFERPOOL_EVICTION_STRATEGY(BUFFERPOOL_SIZE)

    def flush_before_merge(self):
        frames = list(self.bufferpool._pool.keys())
        for frame in frames:
            self.commit(frame)

bufferpool = BufferPool()
