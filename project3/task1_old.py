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

def find_k_itemsets(k, all_basket,sorted_frequent_items): # k = number of size

    dict1 = {}
    produce_pair = combinations(sorted_frequent_items, k)
    count = 0
    for i in produce_pair:  # for every key
        for j in all_basket:  # combinations do not have duplicates
            if set(i).issubset(set(j)):
                if i not in dict1.keys():  # default: find from dict keys  dict1.values(): find from values
                    dict1.update({i: 1})
                if i in dict1.keys():
                    dict1[i] += 1  # use key to find value, then +1
    # print(dict1, '*****dict*****')
    # print(count)

    # find all frequent itemsets
    freq_itemsets = set()
    can_itemsets = []

    for k in dict1:  # dict1:candidate itemsets ??? k:dict1 key ???
        if(dict1[k]>=new_s):  # support>=ps  dict1[k]:count
            can_itemsets.append(k)  # append key
            for n in k:
                freq_itemsets.add(n)  # put key in freq_itemsets
    # print(can_itemsets,'8***********')


    return list(freq_itemsets), can_itemsets   # why not list in line 74 => since set will not have duplicates, and we do not want duplicates


def A_priori(x):  # ('user_id', 'business id') ('j7ISB0jFaAAphR_Jg0pg8Q', ['65eDIQ8bpMJFtgdyX7ulVA']) or ('P9H34s4RTz94HEZaxO5lEQ', ['Q3CQumXTxIPC0wecw3-C_Q', 'HDzt1uH9NY9IwW01XBR0kQ'])
    re_candidate = []
    all_items_single= []  # single candidates
    freq_items_single=[]  # single frequent itemsets
    all_basket = []
    for i in x: # i:('user_id', 'business id')
        # print(i)
        all_items_single+=i[1]  # 'business id'
        all_basket.append(i[1])
    c = collections.Counter(all_items_single)  # every single item's count
    # print(c)
    for k in c:  # every item in c
        # print(k)
        if c[k]>=(new_s): # c[k]: item k's count
           freq_items_single.append(k)
    # print(freq_items_single)


    #print(c.most_common(5))
    # print('********')

    sorted_frequent_items=sorted(freq_items_single)
    new_single = [tuple(set((i,))) for i in sorted_frequent_items]  # set will have no duplicates
    re_candidate.append(new_single)
    # print(sorted_frequent_items_single)
    # print(len(sorted_frequent_items))
    n_size = 2 # start from pairs, then triples, ...
    while len(sorted_frequent_items) >= n_size:
        # print(n_size,'n_size')

        frequent_new, temp_candidate = find_k_itemsets(n_size,all_basket,sorted_frequent_items)
        n_size +=1
        sorted_frequent_items = frequent_new
        if len(temp_candidate) != 0:
            re_candidate.append(temp_candidate)


    yield re_candidate # generate every partition's result

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
    temp_rdd=textRDD.repartition(2) # threshold=4, choose 2 (a factor of 4)
    num = 2
    #print(num,'********************')
    # textRDD2 = sc.textFile(input_file_path2)

    #threshold = 4
    new_s = threshold//num  # //:floor
    # case 1: user1:[business11, business12, ...]
    case1=temp_rdd.filter(lambda x: 'user_id,business_id' not in x).map(lambda x: x.split(',')).map(lambda x: (x[0],x[1])).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y:x+y) # filter(lambda x: 'user_id,business_id' not in x): do not read header
    # print(case1,'--------------------')

    # print(combinations(sorted_frequent_items_single, 2))
    # A_priori_rdd=sc.parallelize(A_priori)
    apri = case1.mapPartitions(A_priori).collect()  # collect all re_candidate in all partitions
    # print(apri,'******result********')
    son1 = apri[0] + apri[1]  # since mapPartitions will have a extra [], we do not want a list of list, son1: a list
    basket = case1.map(lambda x: x[1]).collect()
    # print(basket,'*****')
    # basket = list(basket)
    # print(son1,'son1')

    # list=> use parallelize , file=> use textFile
    candidate_rdd = sc.parallelize(son1) \
        .flatMap(lambda x: x) \
        .distinct() \
        .sortBy(lambda x: len(x), ascending=False).map(lambda x: tuple(sorted(x)))

    frequent_rdd = sc.parallelize(son1).flatMap(lambda x: x).distinct() \
        .map(phase2).reduceByKey(lambda x, y: x + y).sortBy(lambda x: len(x[0]), ascending=False) \
        .filter(lambda x: x[1] > 4).map(lambda x: x[0]).map(lambda x: tuple(sorted(x)))  # sort every element in a tuple

    candidate_rdd_len = candidate_rdd.take(1)  # max length
    frequent_rdd_len = frequent_rdd.take(1)

    f = open(output_file_path, 'w')
    f.write('Candidates:\n')
    for i in range(1, len(candidate_rdd_len[0]) + 1):
        candidate_rdd_i = candidate_rdd.filter(lambda x: len(x) == i).sortBy(
            lambda x: x[0]).collect()  # sort every tuple's first element in same length
        temp_re = str(candidate_rdd_i)
        re1 = temp_re.replace(",)", ")").replace("[", "").replace("]", "")
        f.write(re1)
        f.write('\n')
    f.write('Frequent Itemsets:\n')
    for j in range(1, len(frequent_rdd_len[0]) + 1):
        frequent_rdd_i = frequent_rdd.filter(lambda x: len(x) == j).sortBy(lambda x: x[0]).collect()
        temp_re2 = str(frequent_rdd_i)
        re2 = temp_re2.replace(",)", ")").replace("[", "").replace("]", "")
        f.write(re2)
        f.write('\n')
    f.close()
    end1 = time.time()
    case1_time = end1 - start
    print('Duration:',case1_time)


if int(sys.argv[1]) == 2:
    # case 2: business1:[user11, user12, ...]
    temp_rdd= textRDD  # threshold=4, choose 2 (a factor of 4)
    # num = temp_rdd.getNumPartitions()
    new_s = threshold//6  # //:floor

    case2=temp_rdd.filter(lambda x: 'user_id,business_id' not in x)\
        .map(lambda x: x.split(','))\
        .map(lambda x: (x[1],x[0])).distinct()\
        .map(lambda x: (x[0],[x[1]]))\
        .reduceByKey(lambda x,y:x+y) \
        .repartition(9)
      #  .partitionBy(6, lambda x: hash(x[0]))
    # print(case2,'++++++++++++++++++++++++++++++')
    # case2=textRDD.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],x['user_id'])).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y:x+y).take(5)

    apri2 = case2.mapPartitions(A_priori).collect()
    # print(apri,'******result********')
    son2 = apri2[0] + apri2[1] + apri2[2] + apri2[3] + apri2[4] + apri2[5]+apri2[6]+apri2[7]+apri2[8]
    basket2=case2.map(lambda x:x[1]).collect()

    candidate_rdd2 = sc.parallelize(son2)\
        .flatMap(lambda x: x)\
        .distinct()\
        .sortBy(lambda x:len(x),ascending=False).map(lambda x: tuple(sorted(x)))

    frequent_rdd2 = sc.parallelize(son2).flatMap(lambda x: x).distinct()\
        .map(phase2).reduceByKey(lambda x,y : x+y).sortBy(lambda x: len(x[0]),ascending=False)\
        .filter(lambda x: x[1]>4).map(lambda x: x[0]).map(lambda x: tuple(sorted(x)))  # sort every element in a tuple


    candidate_rdd2_len=candidate_rdd2.take(1)  # max length
    frequent_rdd2_len=frequent_rdd2.take(1)

    f = open(output_file_path, 'w')
    f.write('Candidates:\n')
    for i in range(1, len(candidate_rdd2_len[0]) + 1):
        candidate_rdd2_i = candidate_rdd2.filter(lambda x: len(x) == i).sortBy(lambda x: x[0]).collect()  # sort every tuple's first element in same length
        temp_re2 = str(candidate_rdd2_i)
        re2 = temp_re2.replace(",)", ")").replace("[", "").replace("]", "")
        f.write(re2)
        f.write('\n')
    f.write('Frequent Itemsets:\n')
    for j in range(1, len(frequent_rdd2_len[0]) + 1):
        frequent_rdd2_i = frequent_rdd2.filter(lambda x: len(x) == j).sortBy(lambda x: x[0]).collect()
        temp_re2 = str(frequent_rdd2_i)
        re2 = temp_re2.replace(",)", ")").replace("[", "").replace("]", "")
        f.write(re2)
        f.write('\n')
    f.close()
    end2 = time.time()
    case2_time = end2 - start
    print('Duration:', case2_time)

