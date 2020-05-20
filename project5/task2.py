import pyspark
from pyspark import SparkContext
import os
import re
import json
import sys
import time
import logging
import collections
from itertools import combinations
from itertools import product
import math
import random
from random import choice

from functools import reduce
from pyspark.sql.functions import col, lit, when

from pyspark.sql import SQLContext
import copy

start = time.time()

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

#pip install graphframes
# os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

scConf = pyspark.SparkConf() \
    .setAppName('hw4') \
    .setMaster('local[3]')
sc = SparkContext(conf = scConf)

#sc = SparkContext('local[*]', 'task1')

# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
sc.setLogLevel('ERROR')

N=7
#N=int(sys.argv[1])

input_file_path = '../../PycharmProjects/553hw4/ub_sample_data.csv'
#input_file_path = sys.argv[2]
textRDD = sc.textFile(input_file_path).persist()

output_file_path = '../../PycharmProjects/553hw4/betweenness.txt'
#output_file_path = sys.argv[3]

output_file_path2= '../../PycharmProjects/553hw4/community.txt'
#output_file_path2 = sys.argv[4]

user_id_list=textRDD.map(lambda line: line.split(",")).filter(lambda x:x[0] != 'user_id' and x[1] != 'business_id').map(lambda x: x[0]).distinct().collect()
a=textRDD.map(lambda line: line.split(",")).filter(lambda x:x[0] != 'user_id' and x[1] != 'business_id').map(lambda x: (x[0],[x[1]])).reduceByKey(lambda a,b: a+b).collect()
#print(a)
comb=combinations(sorted(user_id_list),2)
# print(comb)
dict1={}  #key:user_id, value:[business_id]
for i in a:
    dict1.update({i[0]:i[1]})

# vertices=[("E",),("D",),("F",), ("G",),("B",),("A",),("C",)]
# edges=[("E", "D"),("E", "F"),("D", "G"),("F", "G"),("B", "A"), ("B", "C"),("A", "C"),("D", "F"),("B", "D")]

vertices = set()
edges=[]
for j in comb:  # j:(user_id,user_id)
    inter_set=set(dict1[j[0]]).intersection(set(dict1[j[1]]))  # dict1[j[0]]:[business_id], dict1[j[1]]:[business_id]
    if len(inter_set)>=N:
        vertices.add(j[0])
        edges.append((j[1], j[0]))
        vertices.add(j[1])
        edges.append((j[0],j[1]))

m=len(edges)

edges_dict={}
for i in edges:
       if i[0] not in edges_dict:
        edges_dict.update({i[0]:[i[1]]})
       else:
           edges_dict[i[0]]+= [i[1]]
       if i[1] not in edges_dict:
        edges_dict.update({i[1]:[i[0]]})
       else:
           edges_dict[i[1]]+= [i[0]]

#print(edges_dict,'edges_dict')

d2 = copy.deepcopy(edges_dict)
def community_num(edges_dict): #input:edges_dict

    community_list=[]
    num=1
    edges_dict_list=list(d2.keys())
    for i in edges_dict_list:
        index=0
        node=i  #current root
        visited_node = []
        visited_node.append(i)
        while(index<len(visited_node)):
            node=visited_node[index] #current root
            for j in edges_dict[node]:
                if j not in visited_node:
                    visited_node.append(j)
            index+=1
        if (set(visited_node) not in community_list):
            community_list.append(set(visited_node))
    return community_list

def community_num2(edges_dict): #input:edges_dict

    community_list=[]
    num=1
    edges_dict_list=list(edges_dict.keys())
    for i in edges_dict_list:
        index=0
        node=i  #current root
        visited_node = []
        visited_node.append(i)
        while(index<len(visited_node)):
            node=visited_node[index] #current root
            for j in edges_dict[node]:
                if j not in visited_node:
                    visited_node.append(j)
            index+=1
        if (set(visited_node) not in community_list):
            community_list.append(set(visited_node))
    return community_list

def bfs(x): # x: a vertice
    visited_list = []
    vertices_num = {}  # the number of shortest path for each node # key: vertice , value:i_number
    visited_list.append(x)
    #print(x,',x')
    index=0
    l = 0
    level_dict={}  # key: vertice, value:level
    level_dict.update({visited_list[index]:0})

    level_reverse={} # key:level, value:[vertice]
    level_reverse.update({0:[visited_list[index]]})

    child_dict = {}
    vertices_credit_dict={}
    while (index<len(visited_list)):
        #print(len(visited_list),'len visited list')
        #print(len(vertices), 'len vertices')
        #print(index,'index')
        visited_list[index]  # current vertice
        vertices_credit_dict.update({visited_list[index]: 1})

        for j in edges_dict[visited_list[index]]: #child of current vertice

            vertices_credit_dict.update({j: 1})
            if j not in visited_list:
                visited_list.append(j)
                vertices_num.update({j:1})
                level_dict.update({j: level_dict[visited_list[index]] + 1})  # current level = parent level +1
                if level_dict[visited_list[index]] + 1 in level_reverse:
                    level_reverse[level_dict[visited_list[index]] + 1]+=[j]
                else:
                    level_reverse.update({level_dict[visited_list[index]] + 1:[j]})
                if visited_list[index] in child_dict:
                    child_dict[visited_list[index]] += [j]
                else:
                    child_dict.update({visited_list[index]: [j]})
            else:
                if level_dict[j] == level_dict[visited_list[index]]+1:
                    vertices_num[j]+=1
                    if visited_list[index] in child_dict:
                        child_dict[visited_list[index]] += [j]
                    else:
                        child_dict.update({visited_list[index]: [j]})

        index+=1
    #print(level_reverse,'level_reverse')
    #print(vertices_num,'vertices_num')
    #print(child_dict,'child_dict')
    #print(vertices_credit_dict,'vertices_credit_dict')
    # if (len(visited_list) ==len(vertices)):
    #     print(level_dict[visited_list[len(visited_list)-1]])
    #print(max(level_reverse, key=int)) #max level
    level_reverse_max_level=max(level_reverse, key=int)
    level_reverse[level_reverse_max_level] #[vertice]

    credit=1

    edges_credit_dict={}
    edge_credit_list = []
    while(level_reverse_max_level!=0):
        for k in level_reverse[level_reverse_max_level]:  # [vertice] A,C
            #print(k)
            level_reverse[level_reverse_max_level-1] # level-1 nodes G,B
            # for l in level_reverse[level_reverse_max_level-1]:
            #     edges_dict[l] # 'G': ['D', 'F'], 'B': ['D', 'A', 'C']
            #     if k in edges_dict[l]: # A,C in ['D', 'F'], ['D', 'A', 'C]
            #         edge_credit_list.append(((k, l), credit))
            #         print(edges_dict[l],(k, l)) #k: current vertice ,l: its parent
            parent_node=set(level_reverse[level_reverse_max_level - 1]).intersection(set(edges_dict[k]))  # B
            parent_node_list=list(parent_node)
            if (len(parent_node_list)==1):
                vertices_credit_dict[parent_node_list[0]]+=vertices_credit_dict[k] #parent node credit+= child node credit
                edge_credit_list.append((tuple(sorted((k,parent_node_list[0]))),vertices_credit_dict[k]))  # (edge, credit)

            else:
                parent_vertices_num_sum = 0
                for h in parent_node_list:
                    parent_vertices_num_sum+=vertices_num[h]
                for h in parent_node_list:
                    weight_sum=vertices_num[h]/parent_vertices_num_sum #parent edge credit
                    # print(vertices_num[h],parent_vertices_num_sum,k,h)
                    vertices_num[k] #child node credit
                    vertices_credit_dict[h]+=weight_sum*vertices_credit_dict[k] #parent node credit= child node credit*weight
                    edge_credit_list.append((tuple(sorted((k,h))),weight_sum*vertices_credit_dict[k]))
                # edge: k,h

        level_reverse_max_level-=1
    return(edge_credit_list)
def q(community_list):
    q=0
    for n in community_list:
        for i in n:
            ki=len(d2[i]) #number of connected nodes
            for j in n:
                kj = len(d2[j])
                if j in d2[i]:
                    aij=1
                else:
                    aij=0
                q+=aij-(ki*kj/2*m)
    return q/(2*m)

def update_edge_dict_f(betweenness_dict): #update edge dict after delete an betweenness
    max_btw=max(list(betweenness_dict.keys()))
    for i in betweenness_dict[max_btw]:
        edges_dict[i[0]].remove(i[1])
        edges_dict[i[1]].remove(i[0])
    return edges_dict

vertices_rdd=sc.parallelize(vertices)\
                .map(bfs)\
                .flatMap(lambda x:x)\
                .reduceByKey(lambda a,b: a+b)\
                .map(lambda x:(x[0],round((x[1]/2),8)))\
                .sortBy(lambda x:x[0])\
                .sortBy(lambda x:x[1], ascending=False)\
                .collect()
#print(vertices_rdd)

f = open(output_file_path, 'w')
for i in vertices_rdd:
    #print(i)
    ii=str(i)
    f.write(ii[1:-1])
    # dict_result = {}
    # dict_result.update({"user_id": i[0][0]})
    # dict_result.update({"business_id": i[0][1]})
    # dict_result.update({"stars": i[1]})
    # json_string = json.dumps(dict_result)
    # f.write(str(json_string))
    f.write('\n')


#print(vertices_rdd)  # How to know the betweenness is correct? ask for answer
#print(community_num(edges_dict))

flag=1000
betweenness_dict_list=[]
q_list=[]

while(flag!=0):
    betweenness_dict={}
    for i in vertices_rdd:
        if i[1] in betweenness_dict:
            betweenness_dict[i[1]]+=[i[0]]
        else:
            betweenness_dict.update({i[1]:[i[0]]})  # key:betweenness value ,value: edge of that betweenness
    betweenness_dict_list.append(betweenness_dict)
    community_result=community_num(edges_dict)
    q_list.append(q(community_result))
    edges_dict=update_edge_dict_f(betweenness_dict)
    new_betweenness=[]
    for i in vertices:
        for j in bfs(i):
            new_betweenness.append(j)
    flag=len(new_betweenness)
    if flag!=0:
        vertices_rdd=sc.parallelize(new_betweenness)\
                .reduceByKey(lambda a,b: a+b)\
                .map(lambda x:(x[0],round((x[1]/2),8)))\
                .sortBy(lambda x:x[0])\
                .sortBy(lambda x:x[1], ascending=False)\
                .collect()
max_q_index=q_list.index(max(q_list))
result_dict=betweenness_dict_list[max_q_index]  # key:betweenness value ,value: edge of that betweenness
result_dict_values=result_dict.values()

#after delete edge(s) with max_q
rdd2=sc.parallelize(result_dict_values)\
       .flatMap(lambda x:x)\
       .flatMap(lambda x:[(x[0],[x[1]]),(x[1],[x[0]])])\
       .reduceByKey(lambda a,b: a+b)\
       .collect()
#print(rdd2)

edges_dict2={}
for i in rdd2:
    edges_dict2.update({i[0]:i[1]})
# for i in list(d2.keys()):
#     if i not in edges_dict2:
#         edges_dict2.update({i[0]:[]})
#print(edges_dict2)
edges_dict2_community=community_num2(edges_dict2)
community_sum=set()
vertices_set=set(vertices)
for i in edges_dict2_community:
    community_sum=community_sum.union(i)
single_node=vertices_set-community_sum

for i in single_node:
    edges_dict2_community.append(set([i]))

#print(edges_dict2_community)

result_rdd = sc.parallelize(edges_dict2_community).sortBy(lambda x: x).map(lambda x:sorted(x)).collect()
result_rdd2= sorted(result_rdd, key=lambda k: (len(k), k[0]))

# test graph  write another vertice list and edges list
# vertices = sqlContext.createDataFrame([
#   ("E"),
#   ("D"),
#   ("F"),
#   ("G"),
#   ("B"),
#   ("A"),
#   ("C")], ["id"])
#
# edges = sqlContext.createDataFrame([
#   ("E", "D"),
#   ("E", "F"),
#   ("D", "G"),
#   ("F", "G"),
#   ("D", "B"),
#   ("B", "A"),
#   ("B", "C"),
# ], ["src", "dst"])

# vertices_df=sqlContext.createDataFrame(vertices1,['id'])
# edges_df=sqlContext.createDataFrame(edges1,['src','dst'])
#
# g = GraphFrame(vertices_df, edges_df)
# print(g)

# result = g.labelPropagation(maxIter=5)
# result.show()

#vertices_rdd_temp=sc.parallelize(vertices).map(bfs).flatMap(lambda x:x).reduceByKey(lambda a,b: a+b).collect()
#print(vertices_rdd_temp)  # cannot print?
# temp=sc.parallelize(vertices)\
#                 .map(bfs) \
#                 .flatMap(lambda x: x) \
#                 .reduceByKey(lambda a,b: a+b)\
#                 .collect()
#print(temp)  #why cannot print?



# bfs('E')
# bfs('A')
# bfs('B')
# bfs('C')
# bfs('D')
# bfs('F')
# bfs('G')
# sum/2 sort

# print(vertices_rdd)

# for i in vertices_rdd:
#     print(i)



f = open(output_file_path2, 'w')
for i in result_rdd2:
    a = ""
    for p in i:
        a += "\'" + str(p) + "\',"
    # print(a[:-1]) 
    # ii=','.join(i)
    f.write(a[:-1])
    f.write('\n')
end = time.time()
case_time = end - start
print('Duration:', case_time)
#Q
