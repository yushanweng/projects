import sys
from pyspark import SparkConf, SparkContext, SQLContext
from collections import defaultdict
from queue import *
import itertools


def get_pairs(x):
    business_id = x[0]
    user_id = x[1]
    pairs = itertools.combinations(user_id, 2)
    return [((sorted(i)[0], sorted(i)[1]), [business_id]) for i in pairs]

def find_edges(x, edges):
    res = set()
    for e in edges:
        if e[0] == x:
            res.add(e[1])
        elif e[1] == x:
            res.add(e[0])
    return list(res)

def cal_bet(root, adj_v, n_vertices):
    queue = Queue(maxsize=n_vertices)
    queue.put(root)
    visited = [root]
    levels = {}
    parents = {}
    weights = {}
    nodes = {}
    betweenness = {}

    levels[root] = 0
    weights[root] = 1
    while not queue.empty():
        node = queue.get()
        children = adj_v[node]
        for i in children:
            if i not in visited:
                queue.put(i)
                parents[i] = [node]
                weights[i] = weights[node]
                visited.append(i)
                levels[i] = levels[node] + 1
            else:
                if i != root:
                    parents[i].append(node)
                    if levels[node] == levels[i]-1:
                        weights[i] += weights[node]
    order = [(visited[i], i) for i in range(len(visited))]
    reverse_order = sorted(order, key=(lambda x: x[1]), reverse=True)
    for i in reverse_order:
        nodes[i[0]]= 1
    reverse_order = [i[0] for i in reverse_order]

    for source in reverse_order:
        if source != root:
            total_weights= 0
            for i in parents[source]:
                if levels[i] == levels[source]-1:
                    total_weights += weights[i]
            for dest in parents[source]:
                if levels[dest] == levels[source]-1:
                    if source < dest:
                        pair = tuple((source,dest))
                    else:
                        pair = tuple((dest,source))
                    if pair not in betweenness.keys():
                        betweenness[pair] = float(nodes[source]*weights[dest]/total_weights)
                    else:
                        betweenness[pair] += float(nodes[source]*weights[dest]/total_weights)
                    nodes[dest] += float(nodes[source]*weights[dest]/total_weights)
    return [[k, v] for k, v in betweenness.items()]

def bfs(root, adj_v, n_vertices):
    visited =[root]
    edges = [] 
    queue= Queue(maxsize=n_vertices)
    queue.put(root)
    while not queue.empty():
        node = queue.get()
        children = adj_v[node]
        for i in children:
            if i not in visited:
                queue.put(i)
                visited.append(i)
            pair = sorted((node,i))
            if pair not in edges:
                edges.append(pair)
    return (visited, edges)

def get_connected_components(adj_v):
    CCs = []
    g = adj_v
    while not isEmpty(g):
        vertices = set()
        for k, v in g.items():
            vertices.add(k)
        vertices = list(vertices)
        root = vertices[0]
        cg = bfs(root, adj_v, len(vertices))
        CCs.append(cg)
        g = remove_comp(g, cg)
    return CCs

def remove_comp(g, comp):
    vertices = comp[0]
    edges = comp[1]
    for v in vertices:
        del g[v]
    for i in g.keys():
        adj_list = g[i]
        for v in vertices:
            if v in adj_list:
                adj_list.remove(i[1])
        g[i] = adj_list
    return g

def remove_edge(adjX, edge):
    if edge[0] in adjX.keys():
        if edge[1] in adjX[edge[0]]:
            adjX[edge[0]].remove(edge[1])
    if edge[1] in adjX.keys():
        if edge[0] in adjX[edge[1]]:
             adjX[edge[1]].remove(edge[0])
    return adjX

def isEmpty(adj_v):
    if len(adj_v) == 0:
        return True
    else:
        for i in adj_v.keys():
            adj_list = adj_v[i]
            if len(adj_list)!=0:
                return False
            else:
                pass
        return True


def cal_mod(adj_v, CCs, count):
    mod = 0
    for c in CCs:
        a_ij=0
        for i in c[0]:
            for j in c[0]:
                a_ij = 0
                adj_list = adj_v[str(i)]
                if j in adj_list:
                    a_ij = 1
                mod += a_ij-(len(adj_v[i])*len(adj_v[j]))/(2*count)  
    return mod/(2*count) 

def build_adjX(CCs):
    res= {}
    for c in CCs:
        for i in c[1]:
            if i[0] in res.keys():
                res[i[0]].append(i[1])
            else:
                res[i[0]] = [i[1]]
            if i[1] in res.keys():
                res[i[1]].append(i[0])
            else:
                res[i[1]] = [i[0]]
    return res




# sparl conf
# sc = SparkContext()
# sqlContext = SQLContext(sc)

# input
threshold= int(sys.argv[1])
inputFilePath = sys.argv[2]
outputFilePathB = sys.argv[3]
outputFilePathC = sys.argv[4]

# read input
input_data = sc.textFile(inputFilePath)
input_rdd = input_data.map(lambda x : x.split(',')).filter(lambda x: x[0]!= "user_id").persist()
business_user = input_rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y)
pair = business_user.flatMap(lambda x: get_pairs(x)).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1])>=threshold).map(lambda x: x[0])
rdd_vertices = pair.flatMap(lambda x: [(x[0]),(x[1])]).distinct()
rdd_edges = pair.map(lambda x: (x[0], x[1])).map(lambda x: (x[0], x[1]))
list_vertices = rdd_vertices.collect()
n_vertices = len(list_vertices)
list_edges = rdd_edges.collect()
adj_v = rdd_vertices.map(lambda x: (x, find_edges(x, list_edges))).collectAsMap()
betweenness_rdd = rdd_vertices.flatMap(lambda x: cal_bet(x, adj_v, n_vertices))\
.reduceByKey(lambda x,y: (x+y)).map(lambda x: (x[0], float(x[1]/2))).sortByKey().map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0]))
first_edge = betweenness_rdd.take(1)[0][0]
adjX = adj_v.copy()
ccs = get_connected_components(adjX)
mod = cal_mod(adj_v, ccs, rdd_edges.count())
adjX = adj_v.copy()
LMOD = -1
communities = []

# calculation
for _ in range(50):
    adjX = remove_edge(adjX, first_edge)
    ccs = get_connected_components(adjX)
    mod = cal_mod(adj_v, ccs, rdd_edges.count())
    adjX = build_adjX(ccs)
    tmp = set()
    for i in adjX.keys():
        tmp.add(i)
    temp = list(tmp)
    v_rdd = sc.parallelize(temp)
    betweenness_tmp = v_rdd.flatMap(lambda x: cal_bet(x, adjX, n_vertices))\
    .reduceByKey(lambda x,y: (x+y)).map(lambda x: (x[0], float(x[1]/2))).sortByKey().map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0]))
    first_edge = betweenness_tmp.take(1)[0][0]
    if mod >= LMOD:
        LMOD = mod
        communities= ccs


# output
outputB = '\n'.join([str(i[0]) + ', ' + f"{i[1]:.8f}".rstrip('0').rstrip('.') for i in betweenness_rdd.collect()])
with open(outputFilePathB, 'w') as fd:
    fd.write(outputB)
    fd.close()

sorted_communities = [(sorted(i[0]), len(sorted(i[0]))) for i in communities]
sorted_communities.sort()
sorted_communities.sort(key=lambda x:x[1])
outputC = '\n'.join([str(i[0]).replace('[','').replace(']','') for i in sorted_communities])
with open(outputFilePathC, 'w') as fd:
    fd.write(outputC)
    fd.close()
