from lstore.config import *
from lstore.pagerange import *
import threading


class PageDirectory:
    def __init__(self):
        self._dic = {}
        self.lock = threading.Lock()

    def __getitem__(self, key):
        return self._dic[key]

    def __setitem__(self, key, value):
        self._dic[key] = value

    def __len__(self):
        return len(self._dic)

    def keys(self):
        return self._dic.keys()

    def get_last_item(self):
        key = list(self._dic.keys())[-1]
        return self._dic[key]

    def write_to_dir(self, dir_path):
        file_path = dir_path / "pd.txt"
        self._write_to_file(file_path)

    def _write_to_file(self, file_path):
        with file_path.open("w") as f:
            for key in self._dic:
                f.write(str(key) + " ")
                for version_num in self._dic[key]:
                    f.write(str(version_num) + " ")
                f.write("\n")

    @classmethod
    def read_from_dir(cls, dir_path):
        file_path = dir_path / "pd.txt"
        return cls._get_dir_from_file(file_path)

    @classmethod
    def _get_dir_from_file(cls, file_path):
        paths_dict = {}
        with file_path.open("r") as f:
            lines = f.readlines()
        for line in lines:
            data = line.split()
            versions = []
            for i in data[1:]:
                versions.append(int(i))
            paths_dict[int(data[0])] = versions
        page_directory = PageDirectory()
        page_directory._dic = paths_dict
        return page_directory

    # def add_new_page_range(self, index):
        # self._dic[index] = [0] * RANGE_SIZE

    # def add_new_base_page(self, index):
        # self._dic[index].insert(0)

    def update_version_number(self, range_num, base_page_num):
        with self.lock:
            self._dic[range_num][base_page_num] += 1

    @classmethod
    def _read_from_file(cls, file_path):
        paths_dict = {}
        with file_path.open("r") as f:
            lines = f.readlines()
        for line in lines:
            data = line.split()
            paths_dict[int(data[0])] = []
            for i in range(1, len(data)):
                paths_dict[int(data[0])].append(int(data[i]))
        page_directory = PageDirectory()
        page_directory._dic = paths_dict
        return page_directory
