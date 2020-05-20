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
    .setAppName("task3predict.py")
    .set("spark.driver.memory", "4g")
    .set("spark.executor.memory", "4g")
)
sc = SparkContext(conf=conf)
#sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')

# input_file_path = '../../PycharmProjects/553hw3/task3_model.txt'
input_file_path = sys.argv[3]
textRDD = sc.textFile(input_file_path)

# input_file_path2 = '../../PycharmProjects/553hw3/test_review.json'  # only target pairs?
input_file_path2 = sys.argv[2]
textRDD2 = sc.textFile(input_file_path2)

# input_file_path3 = '../../PycharmProjects/553hw3/train_review.json'
input_file_path3 = sys.argv[1]
textRDD3 = sc.textFile(input_file_path3)

output_file_path = '../../PycharmProjects/553hw3/output_task3predict.txt'
output_file_path = sys.argv[4]

case = sys.argv[5]




a = textRDD3.map(lambda x: json.loads(x)) \
    .map(lambda x: (x['business_id'], (x['user_id'], x['stars'])))


def top_n(x):  # x:user_id,business_id
    N = 2300
    if x[1] not in dict_b1_2:  # x[1]:bus_id
        return (x,'no')
    # dict_b1_2[x[1]]: sim list
    else:
        if len(dict_b1_2[x[1]]) < N:
            topN_index = range(len(dict_b1_2[x[1]]))
        else:
            topN_index = sorted(range(len(dict_b1_2[x[1]])), key=lambda i: dict_b1_2[x[1]][i])[-N:]  # top n sim index
        # dict_b1[x[1]]: b2 list

        up = 0
        down = 0

        for i in topN_index:
            N_bus = dict_b1[x[1]][i]  # every sim correspond b2 in topN
            if x[0] in dict_corated[N_bus]:  # x[0]:user_id ,dict_corated[N_bus]:[user_id]
                r_u_n = dict_bs_ratings[N_bus][dict_corated[N_bus].index(x[0])]
                sim_index = dict_b1[N_bus].index(x[1])
                w_i_n = dict_b1_2[N_bus][sim_index]  # sim
                up += r_u_n * w_i_n
                down += abs(w_i_n)
        if down != 0:
            p = up / down
            return (x, p)
        else:
            return (x, 'no')


def atleast_n(x):  # x:user_id,business_id
    N = 3
    if x[0] not in dict_u1_2:  # x[0]:user_id   # dict_u1_2=> key:u1, value:[sim]
        return (x, 'no')
    # dict_b1_2[x[1]]: sim list
    else:
        if len(dict_u1_2[x[0]]) < N:        #ask!!!!!!!!!!!!!!!!!!!!!!!! why not just return 4.0??
            topN_index = range(len(dict_u1_2[x[0]]))  # dict_u1_2[x[0]]:[sim]
        else:
            topN_index = sorted(range(len(dict_u1_2[x[0]])), key=lambda i: dict_u1_2[x[0]][i])[-N:]  # top n sim index

        up = 0
        down = 0
        # dict_u1 =>   key:u1, value:[u2]
        for i in topN_index:
            N_bus = dict_u1[x[0]][i]  # every sim correspond u2 in topN
            if x[1] in dict_corated_userbased[
                N_bus]:  # x[1]:business_id ,dict_corated_userbased[N_bus]:[business_id]   # dict_corated_userbased=> key:user_id ,value:[business_id]
                r_a_avg = sum(dict_bs_ratings_userbased[x[0]]) / len(dict_bs_ratings_userbased[x[0]])
                r_u_i = dict_bs_ratings_userbased[N_bus][dict_corated_userbased[N_bus].index(
                    x[1])]  # dict_bs_ratings_userbased => key:user_id ,value:[stars]
                r_u_avg = sum(dict_bs_ratings_userbased[N_bus]) / len(dict_bs_ratings_userbased[N_bus])
                sim_index = dict_u1[N_bus].index(x[0])
                w_a_u = dict_u1_2[N_bus][sim_index]  # sim
                up += (r_u_i - r_u_avg) * w_a_u
                down += abs(w_a_u)
        if down != 0:
            p = r_a_avg + (up / down)
            return (x, p)
        else:
            return (x, 'no')


if (case == 'item_based'):

    b_rdd = textRDD.map(lambda x: json.loads(x))

    b1_rdd = b_rdd \
        .map(lambda x: (x['b1'], ([x['b2']], [x['sim']]))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()

    b2_rdd = b_rdd \
        .map(lambda x: (x['b2'], ([x['b1']], [x['sim']]))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()
    dict_b1 = {}  # key:b1, value:[b2]
    dict_b1_2 = {}  # key:b1, value:[sim]
    for i in b1_rdd:
        dict_b1.update({i[0]: i[1][0]})
        dict_b1_2.update({i[0]: i[1][1]})
    for i in b2_rdd:
        if i[0] not in dict_b1:
            dict_b1.update({i[0]: i[1][0]})
        else:
            dict_b1[i[0]] += i[1][0]
        if i[0] not in dict_b1_2:
            dict_b1_2.update({i[0]: i[1][1]})
        else:
            dict_b1_2[i[0]] += i[1][1]

    # print(list(dict_b1.values())[:10])
    # print(list(dict_b1_2.values())[:10])

    a2 = a.map(lambda x: (x[0], ([x[1][0]], [x[1][1]]))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()

    dict_corated = {}
    for i in a2:
        dict_corated.update({i[0]: i[1][0]})  # key:business_id ,value:[user_id]

    dict_bs_ratings = {}
    for i in a2:
        dict_bs_ratings.update({i[0]: i[1][1]})  # key:business_id ,value:[stars]

    test_review_rdd = textRDD2.map(lambda x: json.loads(x)) \
        .map(lambda x: (x['user_id'], x['business_id'])) \
        .map(top_n) \
        .filter(lambda x: x[1] != 'no') \
        .filter(lambda x: x[1] >= 1 and x[1] <= 5) \
        .collect()

    f = open(output_file_path, 'w')
    for i in test_review_rdd:
        # f.write('"b1":\n')
        dict_result = {}
        dict_result.update({"user_id": i[0][0]})
        dict_result.update({"business_id": i[0][1]})
        dict_result.update({"stars": i[1]})
        json_string = json.dumps(dict_result)
        f.write(str(json_string))
        f.write('\n')

if (case == 'user_based'):
    # .filter(lambda x: x['flag'] == 'user_based') \
    start1 = time.time()

    u1_rdd = textRDD.map(lambda x: json.loads(x)) \
        .map(lambda x: (x['u1'], ([x['u2']], [x['sim']]))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()

    u2_rdd = textRDD.map(lambda x: json.loads(x)) \
        .map(lambda x: (x['u2'], ([x['u1']], [x['sim']]))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()


    # u1_rdd = textRDD.filter(lambda x: '"u1":' in x).map(lambda x: json.loads(x)) \
    #     .map(lambda x: [(x['u1'], {x['u2']: x['sim']}), (x['u2'], {x['u1']: x['sim']})]) \
    #     .flatMap(lambda x: x) \
    #     .reduceByKey(lambda a, b: merge_two_dict(a, b)) \
    #     .collect()

    # sim_dict = {item[0]: item[1] for item in u1_rdd}

    dict_u1 = {}  # key:u1, value:[u2]
    dict_u1_2 = {}  # key:u1, value:[sim]
    for i in u1_rdd:
        dict_u1.update({i[0]: i[1][0]})
        dict_u1_2.update({i[0]: i[1][1]})
    for i in u2_rdd:
        if i[0] not in dict_u1:
            dict_u1.update({i[0]: i[1][0]})
        else:
            dict_u1[i[0]] += i[1][0]
        if i[0] not in dict_u1_2:
            dict_u1_2.update({i[0]: i[1][1]})
        else:
            dict_u1_2[i[0]] += i[1][1]

    end1 = time.time()
    case_time1 = end1 - start1
    #print('Duration:', case_time1)
    # dict_u1 = {}  # key:u1, value:[u2]
    # dict_u1_2 = {}  # key:u1, value:[sim]
    # for i in u1_rdd:
    #     dict_u1.update({i[0]: i[1][0]})
    #     dict_u1_2.update({i[0]: i[1][1]})

    # a = textRDD3.map(lambda x: json.loads(x)) \
    # .map(lambda x: (x['business_id'], (x['user_id'], x['stars'])))

    # key:user_id, value:([business_id],[stars])
    a2 = a.map(lambda x: (x[1][0], ([x[0]], [x[1][1]]))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()

    dict_corated_userbased = {}
    for i in a2:
        dict_corated_userbased.update({i[0]: i[1][0]})  # key:user_id ,value:[business_id]

    dict_bs_ratings_userbased = {}
    for i in a2:
        dict_bs_ratings_userbased.update({i[0]: i[1][1]})  # key:user_id ,value:[stars]

    test_review_rdd2 = textRDD2.map(lambda x: json.loads(x)) \
        .map(lambda x: (x['user_id'], x['business_id'])) \
        .map(atleast_n) \
        .filter(lambda x:x[1] != 'no')\
        .filter(lambda x:x[1]>=1 and x[1]<=5)\
        .collect()

    f = open(output_file_path, 'w')
    for i in test_review_rdd2:
        # f.write('"b1":\n')
        dict_result = {}
        dict_result.update({"user_id": i[0][0]})
        dict_result.update({"business_id": i[0][1]})
        dict_result.update({"stars": i[1]})
        json_string = json.dumps(dict_result)
        f.write(str(json_string))
        f.write('\n')

end = time.time()
case_time = end - start
print('Duration:', case_time)
