# Tester for incomplete segments of code

# Config was changed while running this
# PAGE_SIZE = 16
# START_RID = 0
# TYPE_SIZE = 8
# RANGE_SIZE = 4
# PAGES_PER_BIG_PAGE = 1

from lstore.query import *
import random

table = Table("table", 3, 0)
query = Query(table)
rnd = []
nums_5 = []
nums_10 = []
for i in range(0, 5):
    nums_5.append(i)
    nums_10.append(5 + i)
random.shuffle(nums_5)
random.shuffle(nums_10)

for i in nums_5:
    query.insert(i, 4, 4)
    print("insert " + str(i))
for i in range(0, 3):
    rnd.append(random.randint(0, 4))
    query.update(rnd[-1], None, 1, None)
    print("update " + str(rnd[-1]))
for i in nums_10:
    query.insert(i, 4, 4)
    print("insert " + str(i))
for i in range(0, 3):
    rnd.append(random.randint(3, 9))
    query.update(rnd[-1], None, None, 0)
    print("update " + str(rnd[-1]))

query.delete(6)
query.delete(3)

query.insert(6, 4, 4)
query.insert(3, 6, 1)
query.insert(4, 7, 2)

print(table)
print(table.key_rid_directory)
print(table.ordered_keys)

pass
