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

input_file_path = '../../PycharmProjects/553hw2/review.json'
#input_file_path = sys.argv[1]

input_file_path2 = '../../PycharmProjects/553hw2/business.json'
#nput_file_path2 =sys.argv[3]

output_file_path = '../../PycharmProjects/553hw2/task2.txt'
#output_file_path=sys.argv[2]

output_file_path2 = '../../PycharmProjects/553hw2/user_business.csv'
#output_file_path=sys.argv[2]

textRDD = sc.textFile(input_file_path)
temp_rdd=textRDD.repartition(2) # threshold=4, choose 2 (a factor of 4)
num = temp_rdd.getNumPartitions()
#print(num,'********************')

textRDD2 = sc.textFile(input_file_path2)

nv_bs=textRDD2.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],x['state']))\
    .filter(lambda x: x[1] == 'NV')\
    .map(lambda x: (x[0],x[0]))
    # .sortByKey(True).collect()

us_bs=textRDD.map(lambda x: json.loads(x)).map(lambda x: (x['business_id'],x['user_id']))

two = nv_bs.join(us_bs).map(lambda x: x[1]).collect()
# print(two)
k=70
#threshold=sys.argv[1]
threshold = 50
#threshold=sys.argv[2]
new_s = threshold//num  # //:floor

# case1=sc.parallelize(nv_bs).filter(lambda x: 'user_id' not in x).map(lambda x: x.split(','))
# print(case1)
with open(output_file_path2, mode='w') as user_business:
    user_business = csv.writer(user_business, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    user_business.writerow(['user_id', 'business_id'])
    cnt = 1
    for i in two:
        # print(i[0])
        user_business.writerow([i[1],i[0]])
