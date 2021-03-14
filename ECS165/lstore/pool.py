from abc import ABC, abstractmethod
from collections import namedtuple
from time import time
from threading import Lock
from typing import Tuple

from lstore.heapdict import heapdict

Frame = namedtuple("Frame", ["base_page", "index", "page_range_data_path", "is_base"])

class Pool(ABC):

    @abstractmethod
    def add_frame(self, pinnable_base_page, index: int, page_range_data_path, is_base: bool):
        ...

    @abstractmethod
    def get_frame(self, index: int, page_range_data_path):
        ...

    @abstractmethod
    def evict_frame(self):
        ...

    @abstractmethod
    def is_full(self):
        ...

class PrioritizedPool(Pool, ABC):

    def __init__(self, max_size):
        self._pool = heapdict()
        self._max_size = max_size
        self._latch = Lock()

    def add_frame(self, pinnable_base_page, index: Tuple[int,int], page_range_data_path, is_base: bool):
        with self._latch:
            assert not self.is_full()
            frame = Frame(pinnable_base_page, index, page_range_data_path, is_base)
            self._pool[frame] = self.get_new_frame_priority()
            return frame

    def get_frame(self, index: Tuple[int,int], page_range_data_path, is_base: bool):
        with self._latch:
            for frame in self._pool:
                if frame.index == index and frame.page_range_data_path == page_range_data_path and frame.is_base == is_base:
                    self._pool[frame] = self.get_frame_priority(self._pool[frame])
                    return frame.base_page
            return None

    def evict_frame(self):
        with self._latch:
            assert self.is_full()
            pinned_frames = []
            for i in range(len(self._pool)):
                frame, priority = self._pool.popitem()
                if not frame.base_page.is_pinned():
                    break
                pinned_frames.append((frame, priority))
            assert not frame.base_page.is_pinned(), "Increase BufferPool Size!"
            for frame, priority in pinned_frames:
                self._pool[frame] = priority
            return frame

    def is_full(self):
        return len(self._pool) >= self._max_size

    def __iter__(self):
        with self._latch:
            return iter(self._pool)

    @abstractmethod
    def get_new_frame_priority(self):
        ...

    @abstractmethod
    def get_frame_priority(self, old_priority):
        ...

class LRUPool(PrioritizedPool):

    def __init__(self, max_size):
        super().__init__(max_size)

    def get_new_frame_priority(self):
        return time()

    def get_frame_priority(self, old_priority):
        return time()

class MRUPool(PrioritizedPool):

    def __init__(self, max_size):
        super().__init__(max_size)

    def get_new_frame_priority(self):
        return -time()

    def get_frame_priority(self, old_priority):
        return -time()

class FIFOPool(PrioritizedPool):

    def __init__(self, max_size):
        super().__init__(max_size)

    def get_new_frame_priority(self):
        return time()

    def get_frame_priority(self, old_priority):
        return old_priority

class LIFOPool(PrioritizedPool):

    def __init__(self, max_size):
        super().__init__(max_size)

    def get_new_frame_priority(self):
        return -time()

    def get_frame_priority(self, old_priority):
        return old_priority
