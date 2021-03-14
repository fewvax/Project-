
from lstore.pool import LRUPool

# Global Setting for the Database
# PageSize, StartRID, etc..
#
PAGE_SIZE = 4096
START_RID = 0
TYPE_SIZE = 8
RANGE_SIZE = 16

# PAGE_SIZE = 24
# START_RID = 0
# TYPE_SIZE = 8
# RANGE_SIZE = 4

MAX_RECORDS = PAGE_SIZE // TYPE_SIZE - 1
MAX_UNSIGNED_INT = (2 ** (8 * TYPE_SIZE)) - 1
BUFFERPOOL_SIZE = 10000
RECORDS_PER_PAGE_RANGE = MAX_RECORDS * RANGE_SIZE

NULL_VALUE = MAX_UNSIGNED_INT

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4
TPS_COLUMN = 5

METADATA_COLUMN_COUNT = 6

BUFFERPOOL_EVICTION_STRATEGY = LRUPool

BASE_META_DIRECTORY_NAME = "BasePageMetadata"
BASE_DATA_DIRECTORY_NAME = "BasePageData"
TAIL_META_DIRECTORY_NAME = "TailPageMetadata"
TAIL_DATA_DIRECTORY_NAME = "TailPageData"

# required function
def init():
    pass


# Makes sure a given input is a 64 bit integer or a None value
def validate(val):
    if val is None:
        return True
    try:
        val.to_bytes(8, "big")
    except (OverflowError, AttributeError):
        return False
    return True


# Makes sure a given list of columns is only 64 bit integers or None values
def list_validate(val):
    try:
        for v in val:
            if not validate(v):
                return False
        return True
    except TypeError:
        return False
