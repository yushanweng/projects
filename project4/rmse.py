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

input_file_path = '../../PycharmProjects/553hw3/test_review_ratings.json'
textRDD = sc.textFile(input_file_path)

input_file_path2 = '../../PycharmProjects/553hw3/task3item.predict'
textRDD2 = sc.textFile(input_file_path2)

output_file_path = '../../PycharmProjects/553hw3/task3_groundtruth.txt'

a = textRDD.map(lambda x: json.loads(x)) \
   .map(lambda x: ((x['user_id'],x['business_id']),[x['stars']])).count()
a2=textRDD.map(lambda x: json.loads(x)) \
   .map(lambda x: ((x['user_id'],x['business_id']),[x['stars']])).collect()

predict=textRDD2.map(lambda x: json.loads(x)).map(lambda x: ((x['user_id'],x['business_id']),[x['stars']])).collect()

ground_predict=a2+predict

b=sc.parallelize(ground_predict).reduceByKey(lambda a,b:a+b).map(lambda x:(x[1][0]-x[1][1])**2).collect()
rmse=math.sqrt(sum(b)/a)
print(rmse)


# f = open(output_file_path, 'w')
# for i in b:
#    # f.write('"b1":\n')
#       dict_result = {}
#       dict_result.update({"user_id": i[0][0]})
#       dict_result.update({"business_id": i[0][1]})
#       dict_result.update({"stars": i[1]})
#       json_string = json.dumps(dict_result)
#       f.write(str(json_string))
#       f.write('\n')