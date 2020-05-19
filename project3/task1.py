from pyspark import SparkContext
import os
import re
import json
import sys
import time
import logging
import collections
from itertools import combinations

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)
# spark_context = SparkContext()

# os.environ['PYSPARK_PYTHON'] = '/Users/yushan/opt/anaconda3/envs/v36_python/bin/python'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/yushan/opt/anaconda3/envs/v36_python/bin/python'

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')
def find_k_itemsets(n_size, can_single, dict_rdd): # k = number of size
    can = []
    comb=combinations(can_single,n_size)
    can_single = set()
    for i in comb:
        cnt = 1
        for eachelement in i:
            v_list = dict_rdd[eachelement]
            if cnt == 1:
                inter_set = set(v_list)
            inter_set=set(v_list).intersection(set(inter_set))  # every time new produced v_list intersections with the previous inter_set
            cnt += 1
        if len(inter_set) >= new_s:  # inter_set: both singles appears in their basket
            can.append(i)
            for j in i:  # keep those singles form candidate pairs
                can_single.add(j)


    return can, list(can_single)   # why not list in line 74 => since set will not have duplicates, and we do not want duplicates


def A_priori(x):  # ('user_id', 'business id') ('j7ISB0jFaAAphR_Jg0pg8Q', ['65eDIQ8bpMJFtgdyX7ulVA']) or ('P9H34s4RTz94HEZaxO5lEQ', ['Q3CQumXTxIPC0wecw3-C_Q', 'HDzt1uH9NY9IwW01XBR0kQ'])

    candidate_single=[]
    can = []  # frequent
    n_size=2
     # key: user_id=> unique
    for i in x:
        print(i,'i')
        if len(dict_rdd[i[0]])>= new_s:
            candidate_single.append(i[0])  # single
    candidate_single = sorted(candidate_single)
    new_single = [tuple(set((i,))) for i in candidate_single]
    can.append(new_single)

    while len(candidate_single)>=n_size:
        temp_can, candidate_single = find_k_itemsets(n_size, candidate_single, dict_rdd)
        n_size += 1
        can.append(temp_can)
    # print(can)

    # new_single = [tuple(set((i,))) for i in sorted_frequent_items]  # set will have no duplicates
    # re_candidate.append(new_single)
    print('******************')
    yield can # generate every partition's result

def phase2(i):   # local count in a partition
    temp_list=[i,0]
    # if type(i):
    #     print(i,'***********single************')
    # for i in candidate_rdd:
    for j in basket:

        if set(i).issubset(set(j)):
            temp_list[1]+=1
    return tuple(temp_list)

#put input file in spark bin
#input_file_path = '../../PycharmProjects/553hw2/small1.csv'   # Vocarum small2 for submit ???
input_file_path = sys.argv[3]

#input_file_path2 = '../../PycharmProjects/553hw2/business.json'
#input_file_path2 =sys.argv[3]

#y = 2015
#y=sys.argv[4]
#m=10
# m=int(sys.argv[5])
#n=12
# n=int(sys.argv[6])

#output_file_path = '../../PycharmProjects/553hw2/output1.txt'
output_file_path=sys.argv[4]

#start time
start = time.time()
textRDD = sc.textFile(input_file_path)
threshold = int(sys.argv[2])

if int(sys.argv[1])==1:
    f = open(output_file_path, 'w')

    temp_rdd=textRDD.repartition(1) # threshold=4, choose 2 (a factor of 4)
    num = 1
    # textRDD2 = sc.textFile(input_file_path2)

    #threshold = 4
    new_s = threshold // num # //:floor
    # case 1: user1:[business11, business12, ...]
    case1=temp_rdd.filter(lambda x: 'user_id,business_id' not in x).map(lambda x: x.split(',')).map(lambda x: (x[0],x[1])).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y:x+y).map(lambda x: (x[0],x[1]))  # filter(lambda x: 'user_id,business_id' not in x): do not read header
    # print(case1,'--------------------')

    # case1111 = temp_rdd.filter(lambda x: 'user_id,business_id' not in x).map(lambda x: x.split(',')).map(
    #     lambda x: (x[0], x[1])).distinct().map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).map(
    #     lambda x: x[0], x[1])  # filter(lambda x: 'user_id,business_id' not in x): do not read header
    # print(case1111,'--------------------')
    son1_rdd = temp_rdd.filter(lambda x: 'user_id,business_id' not in x).map(lambda x: x.split(',')).map(lambda x: (x[1],x[0])).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y:x+y) # filter(lambda x: 'user_id,business_id' not in x): do not read header
    dict_rdd = {}
    dict_temp = son1_rdd.collect()

    for i in dict_temp:  #single
        dict_rdd.update({i[0]:i[1]})
    apri = son1_rdd.mapPartitions(A_priori).collect()  # collect all re_candidate in all partitions
    son1 = apri[0] # since mapPartitions will have a extra [], we do not want a list of list, son1: a list
    basket = case1.map(lambda x: x[1]).collect()

    # list=> use parallelize , file=> use textFile
    candidate_rdd = sc.parallelize(son1) \
        .flatMap(lambda x: x) \
        .distinct() \
        .sortBy(lambda x: len(x), ascending=False).map(lambda x: tuple(sorted(x)))

    frequent_rdd = sc.parallelize(son1).flatMap(lambda x: x).distinct() \
        .map(phase2).reduceByKey(lambda x, y: x + y).sortBy(lambda x: len(x[0]), ascending=False) \
        .filter(lambda x: x[1] >= threshold).map(lambda x: x[0]).map(lambda x: tuple(sorted(x)))  # sort every element in a tuple

    candidate_rdd_len = candidate_rdd.take(1)  # max length
    frequent_rdd_len = frequent_rdd.take(1)
    f.write('Candidates:\n')
    for i in range(1, len(candidate_rdd_len[0]) + 1):
        candidate_rdd_i = candidate_rdd.filter(lambda x: len(x) == i)
        # for k in range(0,i):
        #     candidate_rdd_i=candidate_rdd_i.sortBy(lambda x: x[k])  # sort every tuple's first element in same length
        candidate_rdd_i=sorted(candidate_rdd_i.collect())
        temp_re = str(candidate_rdd_i)
        re1 = temp_re.replace(",)", ")").replace("[", "").replace("]", "").replace("), (", "),(")
        f.write(re1)
        f.write('\n')
    f.write('Frequent Itemsets:\n')
    for j in range(1, len(frequent_rdd_len[0]) + 1):
        frequent_rdd_i = frequent_rdd.filter(lambda x: len(x) == j).sortBy(lambda x: x[0]).collect()
        temp_re2 = str(frequent_rdd_i)
        re2 = temp_re2.replace(",)", ")").replace("[", "").replace("]", "").replace("), (", "),(")
        f.write(re2)
        f.write('\n')
    f.close()
    end1 = time.time()
    case1_time = end1 - start
    print('Duration:',case1_time)


if int(sys.argv[1]) == 2:
    # case 2: business1:[user11, user12, ...]
    temp_rdd= textRDD  # threshold=4, choose 2 (a factor of 4)
    num = 1
    new_s = threshold//1 # //:floor

    case2=temp_rdd.filter(lambda x: 'user_id,business_id' not in x)\
        .map(lambda x: x.split(','))\
        .map(lambda x: (x[1],x[0])).distinct()\
        .map(lambda x: (x[0],[x[1]]))\
        .reduceByKey(lambda x,y:x+y) \
        .repartition(1)

    son1_rdd = temp_rdd.filter(lambda x: 'user_id,business_id' not in x).map(lambda x: x.split(',')).map(lambda x: (x[0],x[1])).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y:x+y).repartition(1) # filter(lambda x: 'user_id,business_id' not in x): do not read header

    dict_rdd = {}
    dict_temp = son1_rdd.collect()

    for i in dict_temp:  # single
        dict_rdd.update({i[0]: i[1]})

    apri2 = son1_rdd.mapPartitions(A_priori).collect()
    # print(apri,'******result********')
    son2 = apri2[0]
    basket=case2.map(lambda x:x[1]).collect()

    candidate_rdd2 = sc.parallelize(son2)\
        .flatMap(lambda x: x)\
        .distinct()\
        .sortBy(lambda x:len(x),ascending=False).map(lambda x: tuple(sorted(x)))

    frequent_rdd2 = sc.parallelize(son2).flatMap(lambda x: x).distinct()\
        .map(phase2).reduceByKey(lambda x,y : x+y).sortBy(lambda x: len(x[0]),ascending=False)\
        .filter(lambda x: x[1]>=threshold).map(lambda x: x[0]).map(lambda x: tuple(sorted(x)))  # sort every element in a tuple


    candidate_rdd2_len=candidate_rdd2.take(1)  # max length
    frequent_rdd2_len=frequent_rdd2.take(1)

    f = open(output_file_path, 'w')
    f.write('Candidates:\n')
    for i in range(1, len(candidate_rdd2_len[0]) + 1):
        candidate_rdd2_i = candidate_rdd2.filter(lambda x: len(x) == i).sortBy(
            lambda x: x[0]).collect()  # sort every tuple's first element in same length
        temp_re2 = str(candidate_rdd2_i)
        re2 = temp_re2.replace(",)", ")").replace("[", "").replace("]", "").replace("), (", "),(")
        f.write(re2)
        f.write('\n')
    f.write('Frequent Itemsets:\n')
    for j in range(1, len(frequent_rdd2_len[0]) + 1):
        frequent_rdd2_i = frequent_rdd2.filter(lambda x: len(x) == j).sortBy(lambda x: x[0]).collect()
        temp_re2 = str(frequent_rdd2_i)
        re2 = temp_re2.replace(",)", ")").replace("[", "").replace("]", "").replace("), (", "),(")
        f.write(re2)
        f.write('\n')
    f.close()
    end2 = time.time()
    case2_time = end2 - start
    print('Duration:', case2_time)

