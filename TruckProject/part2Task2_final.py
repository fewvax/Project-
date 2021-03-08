from collections import defaultdict
from queue import Empty, Queue



def bfs(map, office):
    dict1, res = defaultdict(list), {}

    visited = set()
    visited.add(office)

    q1 = Queue()
    q1.put((office, [office]))

    for i in map:
        dict1[i[0]].append(i[1])
        dict1[i[1]].append(i[0])

    while not q1.empty():
        curr, c_path = q1.get()
        res[curr] = c_path

        for i in sorted(dict1[curr]):
            if i not in visited:
                q1.put((i, c_path + [i]))
                visited.add(i)
    return res

def dfs(map, office):
    path = {}
    stack = [(office, [office])]
    visited = set()
    adj_lst = defaultdict(list)
    for i in map:
        adj_lst[i[0]].append(i[1])
        adj_lst[i[1]].append(i[0])
    while stack:
        (v, p) = stack.pop()
        if v not in visited:
            path[v] = p
        visited.add(v)
        for i in sorted(adj_lst[v]):
            stack.append((i, p + [i]))
        del adj_lst[v]
    return path

def dijkstra(map, office):
    # make an empty dictionary for the answers
    paths = {}
    # make dictionary for distances
    distances = {}
    for road in map:
        start = road[0]
        end = road[1]
        length = road[2]

        if start not in distances:
            if start == office:
                distances[start] = 0
            else:
                distances[start] = float('inf')
        if end not in distances:
            if end == office:
                distances[end] = 0
            else:
                distances[end] = float('inf')

    # make adjacency list dictionary
    adj = {}
    for road in map:
        start = road[0]
        end = road[1]
        if start in adj:
            adj[start].append(end)
        else:
            adj[start] = [end]
        if end in adj:
            adj[end].append(start)
        else:
            adj[end] = [start]
    # make weights dictionary of dictionaries
    weights = {}
    for road in map:
        start = road[0]
        end = road[1]
        length = road[2]

        if start not in weights:
            weights[start] = {}
            weights[start][end] = length
        if start in weights:
            if end not in weights[start]:
                weights[start][end] = length

        if end not in weights:
            weights[end] = {}
            weights[end][start] = length
        if end in weights:
            if start not in weights[end]:
                weights[end][start] = length

    # make predecessors dictionary
    pre = {}

    for key in distances:
        pre[key] = None

    # make empty list
    Q = []

    # insert key value pair (d[v],v) into
    # list for each vertex
    for vertex in distances:
        Q.append((distances[vertex], vertex))
    # sort the list in decreasing order
    Q.sort(reverse=True)

    # iterate as long as Q isn't empty
    while Q:
        (dist, u) = Q.pop(-1)
        for v in adj[u]:
            if ((distances[v] > (distances[u] + weights[v][u])) and ((distances[v], v) in Q)):  #
                Q.remove((distances[v], v))
                distances[v] = distances[u] + weights[v][u]
                Q.append((distances[v], v))
                Q.sort(reverse=True)

                pre[v] = u

    # now add answers to the dictionary
    for key in distances:
        k = key
        path = []
        while k != office:
            path.append(k)
            k = pre[k]
        path.append(office)
        path.reverse()

        paths[key] = path

    return paths
class Package:
    def __init__(self, id):
        self.id = id
        self.address = ""
        self.office = ""
        self.ownerName = ""
        self.collected = False
        self.delivered = False


class Truck:
    def __init__(self, id, n, loc):
        self.id = id
        self.size = n
        self.location = loc
        self.packages = {}

    def collectPackage(self, pk):
        if self.location == pk.office and len(self.packages) < self.size:
            # Add package to packages dictionary
            self.packages[pk.id] = pk

            # Set package pickedUp to true
            pk.collected = True

    def deliverPackage(self, pk):
        # Find delivery location
        # drive to required location
        if pk.id in self.packages:
            # remove the package from truck
            del self.packages[pk.id]

            # set delivery status to true for package
            pk.delivered = True

    def deliverPackageByAddress(self, addr):
        # drive to address
        # find all the packages we need to deliver
        # loop through all needed packages
        delivered_pack = []
        for key in self.packages:
            if self.packages[key].address == addr:
                # set package status to delivered
                self.packages[key].delivered = True
                delivered_pack.append(key)
        for key in delivered_pack:
            if self.packages[key].delivered == True:
                # remove package from truck
                del self.packages[key]

    def removePackage(self, pk, office):
        # drive to post office
        # set picked up back to false
        if self.location == office and pk.id in self.packages:
            if self.packages[pk.id].office != office:
                self.packages[pk.id].office = office
            self.packages[pk.id].collected = False
            # remove package from truck (don't set delivery status to true)
            del self.packages[pk.id]

    def driveTo(self, loc1, loc2):
        # set truck's location to location 2
        if self.location == loc1 and loc1 != loc2:
            self.location = loc2

    def getPackagesIds(self):
        # intialize empty python list
        Id_list = []
        # for package on truck:
        for package in self.packages:
            # get ID
            # add ID to list
            Id_list.append(package)
        # return output list
        return Id_list


"""
deliveryService
"""
def deliveryService(map, truck, packages):
    deliveredTo = {}
    stops = []
    #store all the packages info to make a while loop
    pk_dic = {}
    for package in packages:
        if package.office in pk_dic:
            pk_dic[package.office] = pk_dic[package.office] + [package]
        else:
            pk_dic[package.office] = [package]
    for keys in pk_dic:
        pk_dic[keys].sort(key = lambda x: x.address)
    # keep running until all packages are gone
    while pk_dic:
        # check to see if we are at an office
        if truck.location in pk_dic:
            #counter to remove from pk_dic
            s_space = len(truck.packages)
            for pk in pk_dic[truck.location]:
                if pk.delivered != True:
                    truck.collectPackage(pk)
            #if counter is greater than 0
            new_space = len(truck.packages) - s_space
            # we will reduce diction
            if new_space > 0:
                pk_dic[truck.location] = pk_dic[truck.location][new_space:]
            if pk_dic[truck.location] == []:
                del pk_dic[truck.location]
        #check for path of eath packages and drive there and delivers
        q = Queue()
        for key in truck.packages:
            q.put([truck.packages[key],truck.packages[key].address])
        while not q.empty():
            path = dijkstra(map, truck.location)
            pk, loc = q.get()
            for i in path[loc][1:]:
                truck.driveTo(truck.location, i)
                stops.append(i)
            truck.deliverPackage(pk)
            deliveredTo[pk.id] = truck.location

        #drive to new UPS stop in pk_dic
        if pk_dic != {}:
            loc = list(pk_dic.keys())[0]
            path = dijkstra(map, truck.location)
            for i in path[loc][1:]:
                truck.driveTo(truck.location, i)
                stops.append(i)
    return (deliveredTo, stops)
