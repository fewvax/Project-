from collections import defaultdict
from queue import Empty, Queue

"""
FS #Milap
"""


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


"""
DFS #Dave
"""


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


"""
Dijkstra's #Preet 
"""


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


