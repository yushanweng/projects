from pyspark import SparkContext
import os
import re
import json
import sys
import time
import logging
import collections
from itertools import combinations
import csv

sc = SparkContext('local[*]', 'wordcount')
sc.setLogLevel('ERROR')

#input_file_path = '../../PycharmProjects/553hw2/review.json'
#input_file_path = sys.argv[1]

#input_file_path2 = '../../PycharmProjects/553hw2/business.json'
#nput_file_path2 =sys.argv[3]

#output_file_path = '../../PycharmProjects/553hw2/task2.txt'
output_file_path=sys.argv[4]

#output_file_path2 = '../../PycharmProjects/553hw2/user_business.csv'
#output_file_path=sys.argv[2]

# textRDD = sc.textFile(input_file_path)
# temp_rdd=textRDD.repartition(2) # threshold=4, choose 2 (a factor of 4)
# num = temp_rdd.getNumPartitions()
# #print(num,'********************')
#
# textRDD2 = sc.textFile(input_file_path2)
#
# nv_bs=textRDD2.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],x['state']))\
#     .filter(lambda x: x[1] == 'NV')\
#     .map(lambda x: (x[0],x[0]))
#     # .sortByKey(True).collect()
#
# us_bs=textRDD.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],x['review_id']))
#
# two = nv_bs.join(us_bs).map(lambda x: x[1]).collect()
# # print(two)
#k=70
k=int(sys.argv[1])
#threshold = 50
threshold=int(sys.argv[2])
#
# # case1=sc.parallelize(nv_bs).filter(lambda x: 'user_id' not in x).map(lambda x: x.split(','))
# # print(case1)
# with open(output_file_path2, mode='w') as user_business:
#     user_business = csv.writer(user_business, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#     user_business.writerow(['user_id', 'business_id'])
#     cnt = 1
#     for i in two:
#         # print(i[0])
#         user_business.writerow([i[0],i[1]])

start = time.time()

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


def A_priori(x):  #(business_id, user_id(filtered)) -- update version
    # ('user_id', 'business id') ('j7ISB0jFaAAphR_Jg0pg8Q', ['65eDIQ8bpMJFtgdyX7ulVA']) or ('P9H34s4RTz94HEZaxO5lEQ', ['Q3CQumXTxIPC0wecw3-C_Q', 'HDzt1uH9NY9IwW01XBR0kQ'])

    # build local dictionary
    # dict_rdd = {}
    # for i in x:
    #     if i[0] in dict_rdd:
    #         print('flag')
    #         dict_rdd[i[0]].append(i[1])
    #     else:
    #         dict_rdd.update({i[0]:[i[1]]})
    #         # print('flag')

    candidate_single=[]
    can = []  # frequent
    n_size=2
     # key: user_id=> unique
    for i in x:
        # print(i,'i')
        if len(dict_rdd[i[0]])>= new_s:
            candidate_single.append(i[0])  # single
    candidate_single = sorted(candidate_single)
    new_single = [tuple(set((i,))) for i in candidate_single]
    can.append(new_single)
    #print(new_single,'sing')

    while len(candidate_single)>=n_size:
        temp_can, candidate_single = find_k_itemsets(n_size, candidate_single, dict_rdd)
        n_size += 1
        # if n_size == 2:
        #     print(temp_can,'temp_can')
        can.append(temp_can)
   # print(can)

    # new_single = [tuple(set((i,))) for i in sorted_frequent_items]  # set will have no duplicates
    # re_candidate.append(new_single)
    #print('******************')
    # print(dict_rdd)
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

#input_file_path = '../../PycharmProjects/553hw2/user_business.csv'
input_file_path = sys.argv[3]
textRDD = sc.textFile(input_file_path)
temp_rdd=textRDD # threshold=4, choose 2 (a factor of 4)
num = 1
new_s = threshold//num # //:floor
#print(new_s,'new_s')


# (user_id,[bus_id])
case1 =temp_rdd.distinct().filter(lambda x: 'user_id,business_id' not in x).map(lambda x: x.split(','))\
    .map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y) \
    .filter(lambda x: len(x[1]) > k)  # find out user_id that who reviewed more than k businesses

# .sortBy(lambda x: len(x[1]),ascending=False)

new_rdd=case1.map(lambda x:x[0]).collect()  # get those user_id

# new_rdd = case1.map(lambda x:len(x[1])).take(5)

son1_rdd = temp_rdd.distinct().filter(lambda x: 'user_id,business_id' not in x).map(lambda x: x.split(',')).map(lambda x: (x[0], x[1])).filter(lambda x:x[0] in new_rdd).map(lambda x: (x[1],x[0]))\
    .map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y:x+y).sortBy(lambda x: len(x[1]),ascending=False)  # (business_id:[user1,user2, ...])

#global dict

dict_rdd = {}
dict_temp = son1_rdd.collect()
for i in dict_temp:  # single
    dict_rdd.update({i[0]: i[1]})

apri = son1_rdd.repartition(1).mapPartitions(A_priori).collect()  # collect all re_candidate in all partitions

son1 = apri[0]  # since mapPartitions will have a extra [], we do not want a list of list, son1: a list
basket = case1.map(lambda x: x[1]).collect()
# list=> use parallelize , file=> use textFile
candidate_rdd = sc.parallelize(son1) \
    .flatMap(lambda x: x) \
    .distinct() \
    .sortBy(lambda x: len(x), ascending=False).map(lambda x: tuple(sorted(x)))

frequent_rdd = sc.parallelize(son1).flatMap(lambda x: x).distinct() \
    .map(phase2).reduceByKey(lambda x, y: x + y).sortBy(lambda x: len(x[0]), ascending=False) \
    .filter(lambda x: x[1] >= threshold).map(lambda x: x[0]).map(
    lambda x: tuple(sorted(x)))  # sort every element in a tuple

candidate_rdd_len = candidate_rdd.take(1)  # max length
frequent_rdd_len = frequent_rdd.take(1)

#print(candidate_rdd_len,'can__')

f = open(output_file_path, 'w')
f.write('Candidates:\n')
for i in range(1, len(candidate_rdd_len[0])+1): # +1 error? ask
    candidate_rdd_i = candidate_rdd.filter(lambda x: len(x) == i).sortBy(lambda x: x)
    candidate_rdd_i = sorted(candidate_rdd_i.collect(),key=lambda x:x)
    temp_re = str(candidate_rdd_i)
    re1 = temp_re.replace(",)", ")").replace("[", "").replace("]", "").replace("), (", "),(")
    f.write(re1)
    f.write('\n')
    f.write('\n')
f.write('Frequent Itemsets:\n')
for j in range(1, len(frequent_rdd_len[0])+1): # +1 error? ask
    frequent_rdd_i = frequent_rdd.filter(lambda x: len(x) == j).sortBy(lambda x: x).collect()
    temp_re2 = str(frequent_rdd_i)
    re2 = temp_re2.replace(",)", ")").replace("[", "").replace("]", "").replace("), (", "),(")
    f.write(re2)
    f.write('\n')
    if j != len(frequent_rdd_len[0]):
        f.write('\n')
f.close()
end1 = time.time()
case1_time = end1 - start
print('Duration:', case1_time)

#
# def find_k_itemsets(n_size, can_single, dict_rdd): # k = number of size
#     can = []
#     comb=combinations(can_single,n_size)
#     can_single = set()
#     for i in comb:
#         cnt = 1
#         for eachelement in i:
#             v_list = dict_rdd[eachelement]
#             if cnt == 1:
#                 inter_set = set(v_list)
#             inter_set=set(v_list).intersection(set(inter_set))  # every time new produced v_list intersections with the previous inter_set
#             cnt += 1
#         if len(inter_set) >= 1:  # inter_set: both singles appears in their basket
#             can.append(i)
#             for j in i:  # keep those singles form candidate pairs
#                 can_single.add(j)
#
#
#     return can, list(can_single)   # why not list in line 74 => since set will not have duplicates, and we do not want duplicates
#
#
# def A_priori(x):  # ('user_id', 'business id') ('j7ISB0jFaAAphR_Jg0pg8Q', ['65eDIQ8bpMJFtgdyX7ulVA']) or ('P9H34s4RTz94HEZaxO5lEQ', ['Q3CQumXTxIPC0wecw3-C_Q', 'HDzt1uH9NY9IwW01XBR0kQ'])
#
#     candidate_single=[]
#     can = []  # frequent
#     n_size=2
#      # key: user_id=> unique
#     for i in x:
#         print(i,'i')
#         if len(dict_rdd[i[0]])>= 1:
#             candidate_single.append(i[0])  # single
#     candidate_single = sorted(candidate_single)
#     new_single = [tuple(set((i,))) for i in candidate_single]
#     can.append(new_single)
#
#     while len(candidate_single)>=n_size:
#         temp_can, candidate_single = find_k_itemsets(n_size, candidate_single, dict_rdd)
#         n_size += 1
#         can.append(temp_can)
#     # print(can)
#
#     # new_single = [tuple(set((i,))) for i in sorted_frequent_items]  # set will have no duplicates
#     # re_candidate.append(new_single)
#     print('******************')
#     yield can # generate every partition's result
#
# def phase2(i):   # local count in a partition
#     temp_list=[i,0]
#     # if type(i):
#     #     print(i,'***********single************')
#     # for i in candidate_rdd:
#     for j in basket:
#
#         if set(i).issubset(set(j)):
#             temp_list[1]+=1
#     return tuple(temp_list)
#
# #input_file_path = '../../PycharmProjects/553hw2/small1.csv'   # Vocarum small2 for submit ???
# input_file_path = sys.argv[3]
#
# #input_file_path2 = '../../PycharmProjects/553hw2/business.json'
# #input_file_path2 =sys.argv[3]
#
# #y = 2015
# #y=sys.argv[4]
# #m=10
# # m=int(sys.argv[5])
# #n=12
# # n=int(sys.argv[6])
#
# #output_file_path = '../../PycharmProjects/553hw2/output1.txt'
# output_file_path=sys.argv[4]
#
# #start time
#
# textRDD = sc.textFile(input_file_path)
# threshold = int(sys.argv[2])
#
# f = open(output_file_path, 'w')
#
# temp_rdd=textRDD.repartition(2) # threshold=4, choose 2 (a factor of 4)
# num = 2
# # textRDD2 = sc.textFile(input_file_path2)
#
# #threshold = 4
# new_s = threshold // num # //:floor
# # case 1: user1:[business11, business12, ...]
# # print(case1,'--------------------')
# dict_rdd = {}
# dict_temp = son1_rdd.collect()
#
# for i in dict_temp:  #single
#     dict_rdd.update({i[0]:i[1]})
# apri = son1_rdd.mapPartitions(A_priori).collect()  # collect all re_candidate in all partitions
# son1 = apri[0] + apri[1] # since mapPartitions will have a extra [], we do not want a list of list, son1: a list
# basket = case1.map(lambda x: x[1]).collect()
#
# # list=> use parallelize , file=> use textFile
# candidate_rdd = sc.parallelize(son1) \
#     .flatMap(lambda x: x) \
#     .distinct() \
#     .sortBy(lambda x: len(x), ascending=False).map(lambda x: tuple(sorted(x)))
#
# frequent_rdd = sc.parallelize(son1).flatMap(lambda x: x).distinct() \
#     .map(phase2).reduceByKey(lambda x, y: x + y).sortBy(lambda x: len(x[0]), ascending=False) \
#     .filter(lambda x: x[1] > 4).map(lambda x: x[0]).map(lambda x: tuple(sorted(x)))  # sort every element in a tuple
#
# candidate_rdd_len = candidate_rdd.take(1)  # max length
# frequent_rdd_len = frequent_rdd.take(1)
# f.write('Candidates:\n')
# for i in range(1, len(candidate_rdd_len[0]) + 1):
#     candidate_rdd_i = candidate_rdd.filter(lambda x: len(x) == i).sortBy(
#         lambda x: x[0]).collect()  # sort every tuple's first element in same length
#     temp_re = str(candidate_rdd_i)
#     re1 = temp_re.replace(",)", ")").replace("[", "").replace("]", "")
#     f.write(re1)
#     f.write('\n')
# f.write('Frequent Itemsets:\n')
# for j in range(1, len(frequent_rdd_len[0]) + 1):
#     frequent_rdd_i = frequent_rdd.filter(lambda x: len(x) == j).sortBy(lambda x: x[0]).collect()
#     temp_re2 = str(frequent_rdd_i)
#     re2 = temp_re2.replace(",)", ")").replace("[", "").replace("]", "")
#     f.write(re2)
#     f.write('\n')
# f.close()
# end1 = time.time()
# case1_time = end1 - start
# print('Duration:',case1_time)
#
#
#
#
#
#
#
#
