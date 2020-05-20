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

start = time.time()

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

conf = (
    SparkConf()
    .setAppName("your app name")
    .set("spark.driver.memory", "4g")
    .set("spark.executor.memory", "4g")
)
sc = SparkContext(conf=conf)
#sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')

#input_file_path = '../../PycharmProjects/553hw3/train_review.json'
input_file_path= sys.argv[1]
textRDD = sc.textFile(input_file_path).persist()

#output_file_path = '../../PycharmProjects/553hw3/task3_model.txt'
output_file_path= sys.argv[2]

case=sys.argv[3]

a = textRDD.map(lambda x: json.loads(x)) \
        .map(lambda x: (x['business_id'],(x['user_id'],x['stars'])))

def corated(x):  #x:(bus_id,bus_id) pairs
    inter_set=set(dict_corated[x[0]]).intersection(set(dict_corated[x[1]]))
    if len(inter_set)>=3:
        return True
    else:
        return False

def Pearson_tr(x): #x:(bus_id,bus_id) pairs
    inter_set = list(set(dict_corated[x[0]]).intersection(set(dict_corated[x[1]])))  # list:(user_id)intersection i:x[0] j:x[1]
    r_i_avg=sum(dict_bs_ratings[x[0]])/len(dict_bs_ratings[x[0]])
    r_j_avg=sum(dict_bs_ratings[x[1]])/len(dict_bs_ratings[x[1]])
    up=0
    down1=0
    down2=0
    for u in inter_set: # u: user_id
        # dict_bs_ratings[x[0]]:ratings list
        i_index=dict_corated[x[0]].index(u) # dict_corated[x[0]]:user_id list ; index of u
        r_u_i=dict_bs_ratings[x[0]][i_index] # ratings of user u on item i
        j_index = dict_corated[x[1]].index(u)
        r_u_j = dict_bs_ratings[x[1]][j_index]

        r1=r_u_i-r_i_avg
        r2=r_u_j-r_j_avg
        up+=r1*r2

        down1+=r1*r1
        down2+=r2*r2

    down=math.sqrt(down1)*math.sqrt(down2)
    if down!=0:
        weight=up/down
    else:
        return (x,'no')

    return (x,weight)

def Pearson_tr2(x): #x:(user_id,user_id) pairs

    #Jaccard sim
    inter_set = list(set(dict_corated[x[0]]).intersection(set(dict_corated[x[1]])))  # list:(business_id)intersection i:x[0] j:x[1]
    union_set = list(set(dict_corated[x[0]]).union(set(dict_corated[x[1]])))
    sim = len(inter_set) / len(union_set)
    if sim<0.01 or len(inter_set)<3:
        return (x, 'no')
    else:
        r_u_avg=sum(dict_bs_ratings[x[0]])/len(dict_bs_ratings[x[0]])
        r_v_avg=sum(dict_bs_ratings[x[1]])/len(dict_bs_ratings[x[1]])
        up=0
        down1=0
        down2=0
        # user-based: dict_corated2 => key:business_id, value:[user_id]
        # user-based: dict_corated => key:user_id, value:[business_id]
        for u in inter_set: # u: business_id
            # dict_bs_ratings[x[0]]:ratings list
            u_index=dict_corated[x[0]].index(u) # dict_corated[x[0]]:business_id list ; index of u
            r_u_i=dict_bs_ratings[x[0]][u_index] # ratings of user u on item i
            v_index = dict_corated[x[1]].index(u) #dict_corated[x[1]]:business_id list ; index of u
            r_v_i = dict_bs_ratings[x[1]][v_index] # ratings of user v on item i

            r1=r_u_i-r_u_avg
            r2=r_v_i-r_v_avg
            up+=r1*r2

            down1+=r1*r1
            down2+=r2*r2

        down=math.sqrt(down1)*math.sqrt(down2)
        if down!=0:
            weight=up/down
        else:
            return (x,'no')

        return (x,weight)

# b = 20
# r = 4
# n = b*r
m=102001
# a_list=[2003,2011,2017,2027,2029,2039,2053,2063,2069,2081,2083,2087,2089,2099,3001,3011,3019,3023,3037,3041,3049,3061,3067,3079,3083,3089,3109,3119,3121,3137,3163,3167,3169,3181,3187,3191]  # 36
# b_list=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]

b = 250  #250
r = 1
n = b*r
#m=11273
a_list=[7, 43, 89,163,167,311,449,617,619,757,1033,2447,6113,7417,11887,20011]  # 21 prime
b_list=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,997,9973,13441]

a_b_product=list(product(a_list, b_list))

def minhash(x):
    return_list = []
    for i in range(0,b):  # b=15
        hash_4v_list=[]
        for j in range(0,r):
          # r=4
            hash_v_list=[]
            for k in x[1]:
                bus_id_index=bus_id_dict[k]    #key error ask!!!
                #print(user_id_index)
                hash_v = (a_b_product[r*i+j][0]*bus_id_index+a_b_product[r*i+j][1])%m   # self-defined hash funciton: ax+b x=user_id_index   4*i+j=>0~59
                hash_v_list.append(hash_v)
            hash_4v_list.append(min(hash_v_list))
        return_list.append(((i,tuple(hash_4v_list)),[x[0]]))  #key: (minhash signatures) value: business_id
    return return_list

if (case=='item_based'):

    #.filter(lambda x:len(x[1])>=3)=> in order to reduce the number of combinations
    a2=a.map(lambda x:(x[0],([x[1][0]],[x[1][1]])))\
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))\
        .collect()

    a3=a.map(lambda x:(x[1][0],[x[0]])) \
        .reduceByKey(lambda a, b: a + b)\
        .collect()

    dict_corated = {}
    for i in a2:
        dict_corated.update({i[0]:i[1][0]})  # key:business_id ,value:[user_id]

    dict_corated2 = {}
    for i in a3:
        dict_corated2.update({i[0]: i[1]}) # key:user_id ,value:[business_id]

    dict_bs_ratings={}
    for i in a2:
        dict_bs_ratings.update({i[0]: i[1][1]}) # key:business_id ,value:[stars]

    # bus_comb=list(combinations(list(dict_corated.keys()),2))
    # print(len(bus_comb))
    bus_rdd_comb_rdd=sc.parallelize(list(dict_corated2.keys())).map(lambda x: combinations(sorted(dict_corated2[x]),2)).flatMap(lambda x:x).distinct().filter(corated).map(Pearson_tr).filter(lambda x:x[1] !='no').filter(lambda x:x[1]>0).collect()  # (business_id,business_id) pairs have at least one corated user
    # ((bus_id,bus_id),weight)

    print(len(bus_rdd_comb_rdd),'count')

    f = open(output_file_path, 'w')
    for i in bus_rdd_comb_rdd:
        # f.write('"b1":\n')
        dict_result = {}
        # dict_result.update({"flag": 'item_based'})
        dict_result.update({"b1": i[0][0]})
        dict_result.update({"b2": i[0][1]})
        dict_result.update({"sim": round(i[1],2)})
        json_string = json.dumps(dict_result)
        f.write(str(json_string))
        f.write('\n')

if (case == 'user_based'):
   pre_min = textRDD.map(lambda x: json.loads(x)) \
       .map(lambda x: (x['user_id'],[x['business_id']])) \
       .reduceByKey(lambda a, b: a + b)

   dict_pre_min = {}
   pre = pre_min.collect()
   for i in pre:
       dict_pre_min.update({i[0]: i[1]})

   bus_id = textRDD.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'])).distinct().collect()
   bus_id_dict = {}
   for i in bus_id:
       bus_id_dict.update({i: bus_id.index(i)})

   pre_min2=textRDD.map(lambda x: json.loads(x)) \
       .map(lambda x: (x['user_id'],(x['business_id'],x['stars']))) \

   a2_2= pre_min2.map(lambda x: (x[0], ([x[1][0]], [x[1][1]]))) \
       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
       .collect()

   a3_2= pre_min2.map(lambda x: (x[1][0], [x[0]])) \
       .reduceByKey(lambda a, b: a + b) \
       .collect()

   dict_corated = {}
   for i in a2_2:
       dict_corated.update({i[0]: i[1][0]})  # key:user_id, value:[business_id]

   dict_corated2 = {}
   for i in a3_2:
       dict_corated2.update({i[0]: i[1]})  # key:business_id, value:[user_id]

   dict_bs_ratings = {}
   for i in a2_2:
       dict_bs_ratings.update({i[0]: i[1][1]})  # key:user_id ,value:[stars]

   # minhash_rdd = pre_min.persist().map(minhash) \
   #     .flatMap(lambda x: x) \
   #     .reduceByKey(lambda a, b: a + b) \
   #     .filter(lambda x: len(x[1]) > 1) \
   #     .map(lambda x: list(combinations(sorted(x[1]), 2))) \
   #     .flatMap(lambda x: x) \
   #     .distinct() \
   #     .map(Pearson_tr2) \
   #     .filter(lambda x: x[1]!='no') \
   #     .filter(lambda x: x[1]>0)\
   #     .collect()

   minhash_rdd_2 = pre_min.persist().map(minhash) \
       .flatMap(lambda x: x) \
       .reduceByKey(lambda a, b: a + b) \
       .filter(lambda x: len(x[1]) > 1) \
       .map(lambda x: list(combinations(sorted(x[1]), 2))) \
       .flatMap(lambda x: x) \
       .distinct() \
       .map(Pearson_tr2) \
       .filter(lambda x: x[1] != 'no') \
       .filter(lambda x: x[1] > 0)
   print(minhash_rdd_2.count(), 'count')

   minhash_rdd=minhash_rdd_2.collect()
   #print(minhash_rdd)
   f = open(output_file_path, 'w')
   for i in minhash_rdd:
       # f.write('"b1":\n')
       dict_result = {}
       # dict_result.update({"flag": 'user_based'})
       dict_result.update({"u1": i[0][0]})
       dict_result.update({"u2": i[0][1]})
       dict_result.update({"sim": i[1]})
       json_string = json.dumps(dict_result)
       f.write(str(json_string))
       f.write('\n')

end= time.time()
case_time = end - start
print('Duration:',case_time)
#i: 132.15
#change model into both cases!!! ask!!!!

# task3.model delete flag!!!!!

#line 131 key error ask!!!!!!!!!!!!!!!!!!!!!!