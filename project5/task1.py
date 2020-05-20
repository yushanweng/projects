from pyspark import SparkContext, SparkConf
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

from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *
from pyspark.sql import SQLContext
from graphframes.examples import Graphs
from itertools import product
start = time.time()

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

#pip install graphframes

#sc = SparkContext('local[*]', 'task1')


import os
import pyspark
from pyspark import SparkContext
from graphframes import *
import random


os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

scConf = pyspark.SparkConf() \
    .setAppName('hw4') \
    .setMaster('local[3]')
sc = SparkContext(conf = scConf)

# sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
sc.setLogLevel('ERROR')

N=7
#N=int(sys.argv[1])

input_file_path = '../../PycharmProjects/553hw4/ub_sample_data.csv'
#input_file_path =sys.argv[2]
textRDD = sc.textFile(input_file_path).persist()

output_file_path = '../../PycharmProjects/553hw4/task1_1_ans.txt'
#output_file_path = sys.argv[3]

# a='user_id,business_id'
# b=a.split(",")
# print(b) # a list
temp_rdd=textRDD.map(lambda line: line.split(",")).filter(lambda x:x[0] != 'user_id' and x[1] != 'business_id')
user_id=temp_rdd.map(lambda x: x[0]).distinct()
user_id_list=user_id.collect()
#user_id_list2=user_id.collect()

a=temp_rdd.map(lambda x: (x[0],[x[1]])).reduceByKey(lambda a,b: a+b)
aa=a.collect()
#print(a.count(),'count')

#print('aa[0]',aa[0])
comb=list(combinations(sorted(user_id_list),2))

#print(comb,'comb')
dict1={}  #key:user_id, value:[business_id]
for i in aa:
    dict1.update({i[0]:i[1]})

vertices = set()
edges=[]
for j in comb:  # j:(user_id,user_id)
    inter_set=set(dict1[j[0]]).intersection(set(dict1[j[1]]))  # dict1[j[0]]:[business_id], dict1[j[1]]:[business_id]
    if len(inter_set)>=7:
        vertices.add(j[0])
        edges.append((j[1], j[0]))
        vertices.add(j[1])
        edges.append((j[0],j[1]))
#print(len(list(vertices)))
#print(len(edges))
# print(vertices)
# print(edges)
vertices_list=[]
for i in vertices:
    vertices_list.append((i,))

#print(vertices_list)
# def node():
#     nodes=set()

# def vertice_edge(user_id):
#     # print(user_id_list[0],'user_id_list')
#     # print(len(user_id))
#     # print(random.choice(user_id_list),'rrrrrrr')
#     # print(r,'r')
# #
#     edges_list=[]
#     #print(user_id_list[0])
#     #print(user_id_list[1])
#     for i in user_id_list:
#        #print(user_id_list[i],'i')
#        for j in user_id_list:
#             #print(j, 'j')
#             if i!=j and (i,j) not in edges_list:
#                 print(i,j)
#                 edges_list.append((i,j))
#                 edges_list.append((j,i))
#     return user_id_list



# edges=user_id.map(vertice_edge).collect()
# print(len(edges),'len edges')
# for i in comb:
#     print(i,'i')
#     print(i[0],'i[0]')
#     print(i[1], 'i[1]')
#print(list(product(user_id_list,user_id_list)))



vertices_df=sqlContext.createDataFrame(vertices_list,['id'])
edges_df=sqlContext.createDataFrame(edges,['src','dst'])

g = GraphFrame(vertices_df, edges_df)
#print(g)

result = g.labelPropagation(maxIter=5)
result.show()

r=result.collect()
# print(r)
#res = sorted(r, key = lambda i: (len(i), i))
# print(result.take(3))

# print(r[0][0])
# print(r[0][1])
#output=sc.parallelize(r).map(lambda x:(x[0][1],x[0][0])).reduceByKey(lambda a,b: a+b).sortBy(lambda x:len(x[1])).collect()
#print(sorted(output))
result_temp_rdd=sc.parallelize(r).map(lambda x:(x[1],[x[0]])).reduceByKey(lambda a,b: a+b).map(lambda x:x[1]).map(lambda x:sorted(x))
result_rdd = result_temp_rdd.collect()
#print(result_rdd)  # list of lists
# print(len(result_rdd[-1]))
result_rdd2= sorted(result_rdd, key=lambda k: (len(k), k[0]))

# result_rddd= sc.parallelize(r).map(lambda x:(x[1],[x[0]])).reduceByKey(lambda a,b: a+b).count()
# print(result_rddd)
# output=a.sortBy(lambda x:len(x[1])).map(lambda x: str(sorted(x))).collect()
# print(output)
# vertices = sqlContext.createDataFrame([
#   ("a", "Alice", 34),
#   ("b", "Bob", 36),
#   ("c", "Charlie", 30),
#   ("d", "David", 29),
#   ("e", "Esther", 32),
#   ("f", "Fanny", 36),
#   ("g", "Gabby", 60)], ["id", "name", "age"])

# edges = sqlContext.createDataFrame([
#   ("a", "b", "friend"),
#   ("b", "c", "follow"),
#   ("c", "b", "follow"),
#   ("f", "c", "follow"),
#   ("e", "f", "follow"),
#   ("e", "d", "friend"),
#   ("d", "a", "friend"),
#   ("a", "e", "friend")
# ], ["src", "dst", "relationship"])

#result outputfile

#a2=sc.parallelize(aa)\
         #.collect()
# .flatMap(lambda x: (x[0],[x[1]]))\
# .sortBy(lambda x:len(x[1]))\
#         .map(lambda x: sorted(x[1]))\
#print(a2,'a2')
# candidate_rdd2 = sc.parallelize(son2)\
#         .flatMap(lambda x: x)\
#         .distinct()\
#         .sortBy(lambda x:len(x),ascending=False).map(lambda x: tuple(sorted(x)))


f = open(output_file_path, 'w')
# for i in range(1,len(result_rdd[-1])+1):
#     i=result_temp_rdd.filter(lambda x:len(x)==i).collect()
#     i2=sorted(i)
#     ii = str(i2)
#     f.write(ii)
#     f.write('\n')
# f.close()
for i in result_rdd2:
    a = ""
    for p in i:
        a += "\'" + str(p) + "\',"
    # print(a[:-1]) 
    #ii=','.join(i)
    f.write(a[:-1])
    # dict_result = {}
    # dict_result.update({"user_id": i[0][0]})
    # dict_result.update({"business_id": i[0][1]})
    # dict_result.update({"stars": i[1]})
    # json_string = json.dumps(dict_result)
    # f.write(str(json_string))
    f.write('\n')
#
# for i in range(1, (len(aa) + 1)):
#     a_i = a.filter(lambda x: len(x[1])==i).collect()  # sort every tuple's first element in same length
#     #print(a_i,'a_i')
#     temp_a = str(a_i)
#     re = temp_a.replace("(","").replace(")","")
#     f.write(re)
#     f.write('\n')

end = time.time()
case_time = end - start
print('Duration:', case_time)