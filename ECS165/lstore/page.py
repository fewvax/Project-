from lstore.config import *


class Page:

    def __init__(self):
        self.data = bytearray(PAGE_SIZE)
        # num_records is located in last data slot, bytearray()initializes all bytes to 0

    def set_data(self, data):
        self.data = data
        
    # read last data slot in page to get num_records
    def get_num_records(self):
        return int.from_bytes(self.data[PAGE_SIZE-TYPE_SIZE:], "big")
      
    def _inc_num_records(self):
        num_records = self.get_num_records()
        num_records += 1
        self.data[PAGE_SIZE-TYPE_SIZE:] = num_records.to_bytes(TYPE_SIZE, "big")
        
    def __getitem__(self, key):
        return self.read_int(key)

    # one less slot available due to num_records
    def has_capacity(self):
        return (self.get_num_records() + 1) * TYPE_SIZE < PAGE_SIZE

    def write(self, value):
        # This accounts for offsets by starting at 8 * num_records and ending at 8 * (num_records + 1)
        # It also updates num_records
        # TYPE_SIZE is a constant which is equal to 8, it is the size of the datatype
        # The bytes are stored in big endian form
        if not self.has_capacity():
            raise Exception("page.write: No space left in this page")

        self.data[self.get_num_records() * TYPE_SIZE: (self.get_num_records() + 1) * TYPE_SIZE] = value.to_bytes(TYPE_SIZE, "big")
        self._inc_num_records()

    def write_to_pos(self, value, position):
        if self.get_num_records() == 0:
            raise Exception("page.write_to_pos: No data to read in this page")
        if position >= MAX_RECORDS or position < 0:
            raise Exception("page.write_to_pos: Index out of range")

        if position > self.get_num_records() - 1:
            raise Exception("page.write_to_pos: Index " + str(position) + " beyond number of written records in page")
        self.data[position * TYPE_SIZE: (position + 1) * TYPE_SIZE] = value.to_bytes(TYPE_SIZE, "big")

    # returns TYPE_SIZE bytes at a given record index in the page
    def _read_bytes(self, index):
        if self.get_num_records() == 0:
            raise Exception("page._read_bytes: No data to read in this page")
        if index >= MAX_RECORDS or index < 0:
            raise Exception("page._read_bytes: Index out of range")

        if index > self.get_num_records() - 1:
            raise Exception("page._read_bytes: Index " + str(index) + " beyond number of written records in page")
        return self.data[index * TYPE_SIZE: index * TYPE_SIZE + TYPE_SIZE]

    # returns an int at a given record index in the page
    def read_int(self, index):
        return int.from_bytes(self._read_bytes(index), "big")

    def __iter__(self):
        for i in range(self.get_num_records()):
            yield self.read_int(i)

    def __bytes__(self):
        return bytes(self.data)

    @classmethod
    def new_page_from_bytes(cls, bytes_of_pages):
        assert len(bytes_of_pages) == PAGE_SIZE, "Unequal page size" + str(len(bytes_of_pages))
        page = Page()
        page.set_data(bytearray(bytes_of_pages))
        return page
