#!/usr/bin/env python3

import sys
import requests
import subprocess
import threading
import redis
import pickle
import json
import os
import time
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from flask import Flask, request
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

app = Flask(__name__)

def start_splitdata_instance(parameter_file):
    # print("hello")
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/splitdata-function/split"
                        ,json = {"parameter_file": parameter_file}
                        ,verify=False)
    reply = reply.json()
    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    return pickle.loads(r.get(reply["mapper_list_key"])), pickle.loads(r.get(reply["reducer_list_key"])), reply["data_list_key"],  reply["num_blocks"], reply["activation_id"]

def mapper_function(unique_id, activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/mapper-function/mapper",
        json={"unique_id":str(unique_id), "activation_id":str(activation_id), "mapper_mapping_table_key": "mapper_mapping_table_key_"+str(activation_id)},
        verify=False)
    reply = reply.json()
    # print(reply)

def start_mapper_instances(list_of_ids, activation_id):
    mapper_threads = []
    # print("Mapper Functions started")
    for id in list_of_ids:
        mapper_threads.append(threading.Thread(target=mapper_function, args=[str(id), activation_id]))
    
    for mapper_thread in mapper_threads:
        mapper_thread.start()
    
    for mapper_thread in mapper_threads:
        mapper_thread.join()

    # print("Mapper Functions ended")
    # print() 

def reducer_function(unique_id, activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/reducer-function/reducer",
        json={"unique_id":str(unique_id), "activation_id":str(activation_id), "reducer_mapping_table_key": "reducer_mapping_table_key_"+str(activation_id)},
        verify=False)
    reply = reply.json()
    # print(reply)


def start_reducer_instances(list_of_ids, activation_id):
    reducer_threads = []
    # print("Reducer Functions started")
    for id in list_of_ids:
        reducer_threads.append(threading.Thread(target=reducer_function, args=[str(id), activation_id]))
    
    for reducer_thread in reducer_threads:
        reducer_thread.start()
    
    for reducer_thread in reducer_threads:
        reducer_thread.join()

    # print("Reducer Functions ended")
    # print()

def start_aggregator_instances(data_list_key, activation_id):
    # print("Aggregator Functions started")
    ids = {}
    ids["activation_id"] = activation_id
    ids["data_list_key"] = data_list_key
    # print(ids)
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/aggregator-function/aggregate",
        json=ids,
        verify=False)
    reply = reply.json()
    # print(reply)  

    # print("Aggrregator Functions ended")
    # print()
    return reply

def clear_db():
    reply = subprocess.check_output(["redis-cli -h 10.129.28.219 -n 1 flushdb"], shell=True)
    # print(reply.decode('utf-8'))
    pass

def input_db():
    filename = "input.txt"
    file = open(filename)
    file_contents = file.read()
    r = redis.Redis(host='10.129.28.219', port=6379, db=1)
    pickled_object = pickle.dumps(file_contents)
    r.set(filename, pickled_object)
    file_contents = pickle.loads(r.get(filename))
    # print(file_contents)
    # print()

def output_db(splitdata_activation_id):
    filename = "final-output-"+splitdata_activation_id
    # print("Output stored in Redis with key",filename)
    r = redis.Redis(host='10.129.28.219', port=6379, db=1)
    file_contents = pickle.loads(r.get(filename))
    # print(file_contents)
    # print()

@app.route('/driver-function/', methods=['GET', 'POST'])
def main():
    print("Driver Server Function:")
    parameter_file = request.json
    log_data = ""

    start = time.time()
    mapper_list, reducer_list, data_list_key, num_blocks, activation_id = start_splitdata_instance(parameter_file)
    end = time.time()
    split_time = end-start
    print("split_time", split_time)

    start = time.time()
    start_mapper_instances(mapper_list, activation_id)
    end = time.time()
    mapper_time = end-start
    print("mapper_time", mapper_time)

    start = time.time()
    start_reducer_instances(reducer_list, activation_id)
    end = time.time()
    reducer_time = end-start
    print("reducer_time", reducer_time)

    start = time.time()
    agg_reply = start_aggregator_instances(data_list_key, activation_id)
    end = time.time()
    aggre_time = end-start
    print("aggre_time", aggre_time)

    time_stat = {
        "split_time":split_time
        ,"mapper_time":mapper_time
        ,"reducer_time":reducer_time
        ,"aggre_time":aggre_time
    }

    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    r.set("driver_time_stat", pickle.dumps(time_stat))

    return (json.dumps({
        "input":{
            # "input_files": str(input_filename)
            # ,"split_file": str(split_file)
            # "mapper_parallel_instances": str(mapper_parallel_instances)
            # ,"reducer_parallel_instances": str(reducer_parallel_instances)
            "activation_id":str(activation_id)
            ,"num_blocks":num_blocks
            # ,"mapper_split_percentage_list":str(mapper_split_percentage_list)
            # ,"reducer_split_percentage_list":str(reducer_split_percentage_list)
        }
        ,"output":{
            "output_filename": str(agg_reply["output_filename"])
            ,"number_of_mapper_instances": str(len(mapper_list))
            ,"number_of_reducer_instances": str(len(reducer_list))
            # ,"output": str(agg_reply["aggregator-output"])
            ,"split_time": str(split_time)
            ,"mapper_time": str(mapper_time)
            ,"reducer_time": str(reducer_time)
            ,"aggre_time": str(aggre_time)
            # ,"remaining_time":str(remaining_time)
            
        }
    }))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
