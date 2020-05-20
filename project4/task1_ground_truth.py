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
s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')

input_file_path = '../../PycharmProjects/553hw3/train_review.json'
textRDD = sc.textFile(input_file_path,20) # 20: number of partition

output_file_path = '../../PycharmProjects/553hw3/task1truth.txt'
#output_file_path=sys.argv[4]

start = time.time()
#
def Jac_func(x): # x = (bus1,bus2)
    user_id_v1 =dict_pre_min[x[0]]
    user_id_v2 = dict_pre_min[x[1]]
    inter_set = set(user_id_v1).intersection(set(user_id_v2))
    union_set= set(user_id_v1).union(set(user_id_v2))
    sim=len(inter_set)/len(union_set)
    return (x,sim)
# ground truth no minhash (no bands)
pre_min=textRDD.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],[x['user_id']])).reduceByKey(lambda a,b: a+b)
unique_bs_id=pre_min.map(lambda x:x[0]).collect()
pair_list = [unique_bs_id]
# print(pre_min)

dict_pre_min={}
pre=pre_min.collect()
for i in pre:
    dict_pre_min.update({i[0]:i[1]})

unique_bs_id_comb=sc.parallelize(pair_list)\
    .map(lambda x: list(combinations(sorted(x),2)))\
    .flatMap(lambda x: x)\
    .map(Jac_func)\
    .filter(lambda x: x[1]>=0.05)\
    .sortBy(lambda x:x[1])\
    .collect()

f = open(output_file_path, 'w')
for i in unique_bs_id_comb:
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