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
import random

start = time.time()

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')

#input_file_path = '../../PycharmProjects/553hw3/train_review.json'
input_file_path= sys.argv[1]

textRDD = sc.textFile(input_file_path).persist() # 20: number of partition
#output_file_path = '../../PycharmProjects/553hw3/output1.txt'
output_file_path= sys.argv[2]

# test=textRDD.map(lambda x: json.loads(x))\
#     .map(lambda x: (x['user_id'],x['business_id'],x['stars']))\
#     .distinct()\
#     .count()
# print(test)

b = 300
r = 1
n = b*r
m=102001
a_list=[7, 43, 89,163,167,311,449,617,619,757,1033,2447,6113,7417,11887,20011]  # 21 prime
b_list=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,997,9973,13441]

#a_list=[3,7, 43, 89,101,311,449,757,941,11887,20011,29989,35839,39929,39989,49999]
#b_list=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]



# a_list = [2609,2617,2621,2633,2647,2657,2659,2663,2671,2677,2683,2687,11131,11833, 11783, 12011, 12323, 13163, 10459, 10949, 9883, 13411]
# b_list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
def isprime(n):
    '''check if integer n is a prime'''
    # make sure n is a positive integer
    n = abs(int(n))
    # 0 and 1 are not primes
    if n < 2:
        return False
    # 2 is the only even prime number
    if n == 2:
        return True
    # all other even numbers are not primes
    if not n & 1:
        return False
    # range starts with 3 and only needs to go up the squareroot of n
    # for all odd numbers
    for x in range(3, int(n**0.5)+1, 2):
        if n % x == 0:
            return False
    return True

#primes = [i for i in range(0,100) if isprime(i)]
#ll = random.sample(range(1,3000), 30)
#print(primes)
#n = random.choice(primes)
#random.shuffle(primes)  # shuffle: random sort
#b_list=[7,19,23,5,29]
#a_b_product=list(product(primes[:200], b_list))

#b_primes = [i for i in range(1,100000) if isprime(i)]
#random.shuffle(b_primes)
#n = random.randint(1,10000)
#l = random.sample(range(1,31), 30)
a_b_product=list(product(a_list,b_list))
#print(a_b_product)
#print(ll[:30])
#print(l[:30])
#random.shuffle(a_b_product)
#x:(business_id,[user_id])

# def minhash(x):
#     return_list = []
#     for i in range(0,b):  # b=15
#         hash_4v_list=[]
#         b_num=[]
#         b_num.append(i)
#         for j in range(0,r):  # r=4
#             hash_v_list = []
#             for k in x[1]:
#                 user_id_index=user_id.index(k)
#                 #print(user_id_index)
#                 hash_v = (a_b_product[r*i+j][0]*user_id_index+a_b_product[r*i+j][1])%m   # self-defined hash funciton: ax+b x=user_id_index   4*i+j=>0~59
#                 hash_v_list.append(hash_v)
#             min(hash_v_list)      # min in the list as signature
#             hash_4v_list.append(min(hash_v_list))
#         return_list.append(((i,tuple(hash_4v_list)),[x[0]]))  #key: (band_no,minhash signatures) value: business_id
#     return return_list
def minhash(x):
    return_list = []
    for i in range(0,b):  # b=15
        hash_4v_list=[]
        for j in range(0,r):
          # r=4
            hash_v_list=[]
            for k in x[1]:
                user_id_index=user_id_dict[k]
                #print(user_id_index)
                hash_v = (a_b_product[r*i+j][0]*user_id_index+a_b_product[r*i+j][1])%m   # self-defined hash funciton: ax+b x=user_id_index   4*i+j=>0~59
                hash_v_list.append(hash_v)
            hash_4v_list.append(min(hash_v_list))
        return_list.append(((i,tuple(hash_4v_list)),[x[0]]))  #key: (minhash signatures) value: business_id
    return return_list
# def minhash2(x):
#     return_list = []
#     for i in range(0,n):
#         hash_4v_list = []
#         for k in x[1]:
#             user_id_index = user_id.index(k)
#             hash_v = (a_b_product[n][0] * user_id_index + a_b_product[n][1]) % m
#             hash_v_list = []
#             hash_v_list.append(hash_v)
#         hash_4v_list.append(min(hash_v_list))# min in the list as signature
#         return_list.append(((i, tuple(hash_4v_list)), [x[0]]))  # key: (band_no,minhash signatures) value: business_id
#     return return_list


# new=textRDD.map(lambda x: json.loads(x))\
#     .map(lambda x: (x['user_id'],x['business_id']))\
#     .distinct().collect()
#print(new)



# print(a_b_product)

pre_min=textRDD.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],[x['user_id']])).reduceByKey(lambda a,b: a+b)
# print(pre_min)

dict_pre_min={}
pre=pre_min.collect()
for i in pre:
    dict_pre_min.update({i[0]:i[1]})


user_id=textRDD.map(lambda x: json.loads(x)).map(lambda x: (x['user_id'])).distinct().collect()
#print(len(user_id)) #26184

user_id_dict = {}
for i in user_id:
    user_id_dict.update({i:user_id.index(i)})



def Jac_func(x): # x = (bus1,bus2)
    user_id_v1 =dict_pre_min[x[0]]
    user_id_v2 = dict_pre_min[x[1]]
    inter_set = set(user_id_v1).intersection(set(user_id_v2))
    union_set= set(user_id_v1).union(set(user_id_v2))
    sim=float(len(inter_set))/float(len(union_set))
    return (x,sim)

#filter(lambda x: len(x[1])>1): at least two business_id have the same key will be similar x[1]:business_id
# minhash_rdd = pre_min.map(minhash)\
#     .flatMap(lambda x:x)\
#     .reduceByKey(lambda a,b: a+b)\
#     .filter(lambda x: len(x[1])>1)\
#     .map(lambda x: list(combinations(sorted(x[1]),2)))\
#     .flatMap(lambda x: x)\
#     .distinct()\
#     .map(Jac_func)\
#     .filter(lambda x: x[1]>=0.05)\
#     .collect()
#.sortBy(lambda x:x[1])\

# 1.flatMap(lambda x:x): x=>key: (band_num, minhash signatures) value: business_id  ,flatMap: want to delete []
# map(lambda x: list(combinations(sorted(x[1]),2))): x[1]:business_id ,with the same key:(band_num, minhash signatures)=> similar , in a list. generate candidate pairs from that list
temp1 = pre_min.map(minhash)\
    .flatMap(lambda x:x)\
    .reduceByKey(lambda a,b: a+b)\
    .filter(lambda x: len(x[1])>1)\
    .map(lambda x: list(combinations(sorted(x[1]),2)))\
    .flatMap(lambda x: x)\
    .distinct()
print(temp1.count(),'count')
temp_min_hash = temp1.map(Jac_func)\
    .filter(lambda x: x[1]>=0.05)
    #.sortBy(lambda x:x[1])

minhash_rdd2 = temp_min_hash.collect()
print(temp_min_hash.count())


# print(minhash_rdd)

f = open(output_file_path, 'w')
for i in minhash_rdd2:
    # f.write('"b1":\n')
    dict_result={}
    dict_result.update({"b1":i[0][0]})
    dict_result.update({"b2": i[0][1]})
    dict_result.update({"sim":i[1]})
    json_string = json.dumps(dict_result)
    f.write(str(json_string))
    f.write('\n')

end= time.time()
case_time = end - start
print('Duration:',case_time)


# ask how to set good b r ???