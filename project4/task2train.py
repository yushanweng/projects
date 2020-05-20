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

start = time.time()

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')

#input_file_path = '../../PycharmProjects/553hw3/train_review.json'
input_file_path= sys.argv[1]
textRDD = sc.textFile(input_file_path)

#input_file_path2 = '../../PycharmProjects/553hw3/stopwords'
input_file_path2= sys.argv[3]
textRDD2 = sc.textFile(input_file_path2)

#output_file_path = '../../PycharmProjects/553hw3/output_task2train.txt'

#output_file_path2 = '../../PycharmProjects/553hw3/task2_model.txt'
output_file_path2= sys.argv[2]

f = open(input_file_path2, "r")
# print(f.read())

stopwords=[]
for word in f:
    # print(word)
    stopwords.append(word.strip())

# remove stopwords # remove punctuations  # remove numbers and space
a=textRDD.map(lambda x: json.loads(x))\
    .map(lambda x: x['text'])\
    .map(lambda x: x.split(' '))\
    .flatMap(lambda x: x)\
    .map(lambda x: x.strip())\
    .map(lambda s: re.sub(r'[^\w\s]','',s))\
    .map(lambda s: re.sub(r'([^a-zA-Z\s]+?)','',s))


b = a.count()
c = a.map(lambda x: (x,1))\
    .reduceByKey(lambda a,b: a+b)\
    .filter(lambda x: x[1]/b > 0.000001)\
    .filter(lambda x: x[0] not in stopwords)\
    .filter(lambda x:x[0]!= '')\
    .sortBy(lambda x: x[1], ascending= False)\
    .map(lambda x: x[0])\
    .take(200)
#print(c)

# TF-IDF

# uniqueWords = textRDD.\
#     map(lambda x: json.loads(x)) \
#     .map(lambda x: x['text']) \
#     .map(lambda x: x).take(3)
# print(uniqueWords)
# for i in textRDD:
#     dict_words={}
#     if i not in dict_words:
#         dict_words.update({i:1})
#     else:
#         dict_words[i] = dict_words[i]+1
# print(dict_words)
#
def boolean_200(x):
    boolean_vector=[]
    for i in c:  # we want the length of boolean_vector=200
        if i in x[1]:
            boolean_vector.append(1)
        else:
            boolean_vector.append(0)
    return (x[0],boolean_vector)



bus_rdd=textRDD.map(lambda x: json.loads(x))\
        .map(lambda x: (x['business_id'], x['text']))\
        .map(lambda x: (x[0],x[1].split(' ')))\
        .reduceByKey(lambda a,b: a+b)\
        .map(boolean_200)\
        .collect()

# print(bus_rdd)

f = open(output_file_path2, 'w')
for i in bus_rdd:
    # f.write('"b1":\n')
    dict_bus_boolean={}
    dict_bus_boolean.update({"flag":'bus'})
    dict_bus_boolean.update({"id":i[0]})
    dict_bus_boolean.update({"boolean":i[1]})
    json_string = json.dumps(dict_bus_boolean)
    f.write(str(json_string))
    f.write('\n')


usr_rdd=textRDD.map(lambda x: json.loads(x))\
        .map(lambda x: (x['user_id'], x['text']))\
        .map(lambda x: (x[0],x[1].split(' ')))\
        .reduceByKey(lambda a,b: a+b)\
        .map(boolean_200)\
        .collect()

# print(usr_rdd)

f = open(output_file_path2, 'a')  # a:append
for i in usr_rdd:
    dict_usr_boolean={}
    dict_usr_boolean.update({"flag": 'usr'})
    dict_usr_boolean.update({"id":i[0]})
    dict_usr_boolean.update({"boolean":i[1]})
    json_string = json.dumps(dict_usr_boolean)
    f.write(str(json_string))
    f.write('\n')

end= time.time()
case_time = end - start
print('Duration:',case_time)  #299.1530532836914


