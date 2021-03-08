from queue import Queue

"""
Dijkstra's #Preet 
"""
def dijkstra(map, office,target):
    # make an empty dictionary for the answers
    paths = {}
    # make adjacency list dictionary
    adj = {}
    # make dictionary for distances
    distances = {}
    # make weights dictionary of dictionaries
    weights = {}
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
        if start in adj:
            adj[start].append(end)
        else:
            adj[start] = [end]
        if end in adj:
            adj[end].append(start)
        else:
            adj[end] = [start]

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
        if key == target:
            break
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


    def driveTo(self, loc1, loc2):
        # set truck's location to location 2
        if self.location == loc1 and loc1 != loc2:
            self.location = loc2




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
            pk, loc = q.get()
            path = dijkstra(map, truck.location,loc)
            for i in path[loc][1:]:
                truck.driveTo(truck.location, i)
                stops.append(i)
            truck.deliverPackage(pk)
            deliveredTo[pk.id] = truck.location

        #drive to new UPS stop in pk_dic
        if pk_dic != {}:
            loc = list(pk_dic.keys())[0]
            path = dijkstra(map, truck.location,loc)
            for i in path[loc][1:]:
                truck.driveTo(truck.location, i)
                stops.append(i)
    return (deliveredTo, stops)
