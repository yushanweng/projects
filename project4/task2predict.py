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
s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')

#input_file_path = '../../PycharmProjects/553hw3/task2_model.txt'
input_file_path= sys.argv[2]
textRDD = sc.textFile(input_file_path)

#input_file_path2 = '../../PycharmProjects/553hw3/test_review.json'
input_file_path2= sys.argv[1]
textRDD2 = sc.textFile(input_file_path2)

#output_file_path = '../../PycharmProjects/553hw3/output_task2predict.txt'
output_file_path= sys.argv[3]

start = time.time()

model_bus_rdd=textRDD.map(lambda x: json.loads(x))\
            .filter(lambda x: x['flag']=='bus')\
            .map(lambda x: (x['id'], x['boolean']))\
            .collect()

dict_bus_boolean = {}
for i in model_bus_rdd:
    # f.write('"b1":\n')
    dict_bus_boolean.update({i[0]:i[1]})
#print(model_bus_rdd)

model_usr_rdd = textRDD.map(lambda x: json.loads(x)) \
    .filter(lambda x: x['flag'] == 'usr') \
    .map(lambda x: (x['id'], x['boolean'])) \
    .collect()

dict_usr_boolean = {}
for i in model_usr_rdd:
    # f.write('"b1":\n')
    dict_usr_boolean.update({i[0]:i[1]})
#print(model_usr_rdd)

def cos_sim_func(x):
    if x[0] not in dict_usr_boolean or x[1] not in dict_bus_boolean:
        return (x,0)
    else:
        products = sum([a * b for a, b in zip(dict_usr_boolean[x[0]], dict_bus_boolean[x[1]])])  # dict_usr_boolean[x[0]]=> use usr_id in test_review to find boolean vectors in  dict_usr_boolean;  dict_bus_boolean[x[1]]=> use bus_id in test_review to find boolean vectors in  dict_bus_boolean
        temp_1 = math.sqrt(sum(dict_usr_boolean[x[0]]))
        temp_2 = math.sqrt(sum(dict_bus_boolean[x[1]]))
        de = temp_1 * temp_2
        if de==0:
            return (x,0)
    return (x,products/de)


test_review_rdd=textRDD2.map(lambda x: json.loads(x))\
    .map(lambda x: (x['user_id'], x['business_id']))\
    .map(cos_sim_func)\
    .filter(lambda x: x[1]>=0.01)\
    .sortBy(lambda x: x[1], ascending= False)\
    .collect()
#print(test_review_rdd)


# write output file

f = open(output_file_path, 'w')
for i in test_review_rdd:
    # f.write('"b1":\n')
    dict_predict={}
    dict_predict.update({"user_id":i[0][0]})
    dict_predict.update({"business_id":i[0][1]})
    dict_predict.update({"sim":i[1]})
    json_string = json.dumps(dict_predict)
    f.write(str(json_string))
    f.write('\n')

end= time.time()
case_time = end - start
print('Duration:',case_time)