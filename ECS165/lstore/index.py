from threading import RLock

from lstore.config import *
from lstore.table import *
from threading import RLock


"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.table = table
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):

        if self.indices[column] is None:
            self.create_index(column)
        index = self.indices[column]
        node_index = index.search_tree(value)
        if node_index is None:
            return False
        #list of rids
        return node_index[1].keys[node_index[0]][1]

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            self.create_index(column)
        i = begin
        lst = []
        while i <= end:
            temp = self.locate(column, i)
            
            if temp is False:
                lst = lst 
            else:
                lst = lst + temp
            i = i + 1
        return lst

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        index = BTree()
        column_data = self.table.get_column(column_number)
        rids = self.table.get_metadata_column(RID_COLUMN)
        rids_list = []
        for i in rids:
            i = [i]
            rids_list.append(i)
        keys = zip(column_data, rids_list)
        for key in keys:
            index.insert_to_tree(list(key))
        self.indices[column_number] = index
        # index.print_tree(index.root)


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None


class BTree_Node:

    def __init__(self, leaf=False):
        self.keys = []
        self.children = []
        self.leaf = leaf

    def length(self):
        return len(self.keys)


class BTree:

    def __init__(self):
        self.order = 100
        self.root = BTree_Node(True)
        self._latch = RLock()


    def print_tree(self, x, l=0):
        print("Level ", l, " ", len(x.keys), end = ":")
        for i in x.keys:
            print(i,end=" ")
        print()
        l+=1
        if len(x.children)>0:
            for i in x.children:
                self.print_tree(i,l)
                

    #should return index, node
    def search_tree(self, key, node=None):
        with self._latch:
            if node is not None:

                i = 0
                while i < node.length() and key > node.keys[i][0]:
                    i = i + 1

                if i < node.length() and key == node.keys[i][0]:
                    return(i,node)

                elif node.leaf:
                    return None
                else:
                    return self.search_tree(key, node.children[i])

            else:
                return self.search_tree(key, self.root)

        
    def compress_tree(self, key, node=None):
        with self._latch:
            if node is not None:

                i = 0
                while i < node.length() and key[0] > node.keys[i][0]:
                    i = i + 1

                if i < node.length() and key[0] == node.keys[i][0]:
                    node.keys[i][1].append(key[1][0])
                    return True

                elif node.leaf:
                    return False
                else:
                    return self.compress_tree(key, node.children[i])

            else:
                return self.compress_tree(key, self.root)




    def insert_to_tree(self, key):
        with self._latch:
            if self.compress_tree(key) is True:
                return

            root = self.root

            if root.length() == (2 * self.order) - 1:
                test_node = BTree_Node()
                self.root = test_node
                test_node.children.insert(0, root)
                self.split_child(test_node, 0)
                self.insert_space(test_node, key)
            else:
                self.insert_space(root, key)

    def insert_space(self, node, key):
        with self._latch:
            i = node.length() - 1

            if node.leaf:
                node.keys.append([None, None])
                while i >= 0 and key[0] < node.keys[i][0]:
                    node.keys[i + 1] = node.keys[i]
                    i = i - 1
                node.keys[i + 1] = key
            else:
                while i >= 0 and key[0] < node.keys[i][0]:
                    i = i - 1
                i = i + 1
                if node.children[i].length() == (2 * self.order) - 1:
                    self.split_child(node, i)
                    if key[0] > node.keys[i][0]:
                        i = i + 1
                self.insert_space(node.children[i], key)

    def split_child(self, node, i):
        with self._latch:
            order = self.order
            next_node = node.children[i]
            temp = BTree_Node(next_node.leaf)
            node.children.insert(i + 1, temp)
            node.keys.insert(i, next_node.keys[order - 1])
            temp.keys = next_node.keys[order: 2 * order - 1]
            next_node.keys = next_node.keys[0:order - 1]
            if not next_node.leaf:
                temp.children = next_node.children[order:order * 2]
                next_node.children = next_node.children[0:order - 1]

    def delete_in_tree(self, node, key):
        with self._latch:
            order = self.order
            i = 0
            while i < node.length() and key[0] > node.keys[i][0]:
                i = i + 1
            if node.leaf:
                if i < node.length() and key[0] == node.keys[i][0]:
                    node.keys.pop(i)
                    return
                return
            if i < node.length() and key[0] == node.keys[i][0]:
                return self.delete_inside(node, key, i)
            elif node.children[i].length() >= order:
                self.delete_in_tree(node.chilren[i], key)
            else:
                if i != 0 and i + 2 < len(node.children):
                    if node.children[i - 1].length() >= order:
                        self.delete_sibling(node, i, i - 1)
                    elif node.chilren[i + 1].length() >= order:
                        self.delete_sibling(node, i, i + 1)
                    else:
                        self.delete_merge(node, i, i + 1)

                elif i == 0:
                    if node.children[i + 1].length() >= order:
                        self.delete_sibling(node, i, i + 1)
                    else:
                        self.delete_merge(node, i, i + 1)
                elif i + 1 == len(node.children):
                    if node.children[i - 1].length() >= order:
                        self.delete_sibling(node, i, i - 1)
                    else:
                        self.delete_merge(node, i, i - 1)
                self.delete_in_tree(node.children[i], key)

    def delete_inside(self, node, key, i):
        with self._latch:
            order = self.order

            if node.leaf:
                if key[0] == node.keys[i][0]:
                    node.keys.pop(i)
                    return
                return

            if node.children[i].length() >= order:
                node.keys[i] = self.delete_pred(node.children[i])
                return
            elif node.children[i + 1].length() >= order:
                node.keys[i] = self.delete_suc(node.children[i + 1])
                return
            else:
                self.delete_merge(node, i, i + 1)
                self.delete_inside(node.children[i], key, self.order - 1)

    def delete_pred(self, node):
        with self._latch:
            if node.leaf:
                return node.keys.pop()
            count = node.length() - 1
            if node.children[count].length() >= self.order:
                self.delete_sibling(node, count + 1, count)
            else:
                self.delete_merge(node, count, count + 1)
            self.delete_pred(node.children[count])

    def delete_suc(self, node):
        with self._latch:
            if node.leaf:
                return node.keys.pop(0)
            if node.children[1].length() >= self.order:
                self.delete_sibling(node, 0, 1)
            else:
                self.delete_merge(node, 0, 1)
            self.delete_suc(node.children[0])

    def delete_merge(self, node, i, j):
        with self._latch:
            child = node.children[i]
            if j > i:
                right = node.children[j]
                child.keys.append(node.keys[i])

                for item in range(right.length()):
                    child.keys.append(right.keys[item])
                    if len(right.children) > 0:
                        child.children.append(right.children(item))
                if len(right.children) > 0:
                    child.children.append(right.children.pop())
                new = child
                node.keys.pop(i)
                node.children.pop(j)

            else:
                left = node.children[j]
                left.keys.append(node.keys[j])
                for i in range(child.length()):
                    left.keys.append(child.keys[i])
                    if len(left.children) > 0:
                        left.children.append(child.children[i])
                if len(left.children) > 0:
                    left.children.append(child.children.pop())
                new = left
                node.keys.pop(j)
                node.children.pop(i)

            if node == self.root and node.length() == 0:
                self.root = new

    def delete_sibling(self, node, i, j):
        with self._latch:
            child = node.child[i]
            if i < j:
                right = node.child[j]
                child.keys.append(node.keys[i])
                node.keys[i] = right.keys[0]
                if len(right.children) > 0:
                    child.children.append(right.children[0])
                    right.children.pop(0)
                right.keys.pop(0)
            else:
                left = node.children[j]
                child.keys.insert(0, node.keys[i - 1])
                node.keys[i - 1] = left.keys.pop()
                if len(left.children) > 0:
                    child.children.insert(0, left.children.pop())

    def update_tree(self, key, rid, update, node=None):
        with self._latch:
            test = [key, rid]

            if node != None:
                i = 0
                while i < node.length() and test != node.keys[i]:
                    i = i + 1
                if i < node.length() and test == node.keys[i]:
                    for j in node.keys[i][1]:
                        if i == rid:
                            node.keys[i][1].pop(j)
                            self.insert_to_tree([update,[rid]])
                    return
                elif node.leaf:
                    return None
                else:
                    return self.update_tree(key, rid, update, node.children[i])

            else:
                return self.update_tree(key, rid, update, self.root)

    def mini_delete(self, key, rid, node=None):
        with self._latch:
            if node != None:
                i = 0
                while i < node.length() and key > node.keys[i][0]:
                    i = i + 1
                if i < node.length() and key == node.keys[i][0]:
                    for j in node.keys[i][1]:
                        if j == rid:
                            node.keys.pop(j)
                    return
                elif node.leaf:
                    return None
                else:
                    return self.mini_delete(key, rid,  node.children[i])

            else:
                return self.mini_delete(key, rid, self.root)
