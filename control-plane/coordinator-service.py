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

import math
import textwrap
import time
import psutil
import rediscluster

app = Flask(__name__)


# event_obj_list = {}
sema_list = {}
startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]


@app.route('/wake-up/', methods=['GET', 'POST'])
def wake_up():
    request_values = request.json
    function_type = request_values["function_type"]
    unique_id = request_values["unique_id"]
    activation_id = request_values["activation_id"]
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    # print(request_values)
    # print(event_obj_list)
    # event_obj_list[unique_id].set()
    sema_list[wake_up_obj].release()
    response = {}
    response[wake_up_obj] = "Woken"
    output_json = json.dumps(response)
    return output_json


def mapper_function(unique_id, activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/mapper-function/mapper",
        json={"unique_id":str(unique_id), "activation_id":str(activation_id), "mapper_mapping_table_key": "mapper_mapping_table_key_"+str(activation_id)},
        verify=False)
    # event_obj_list[unique_id].wait()
    function_type="mapper"
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    print(wake_up_obj," waiting")
    sema_list[wake_up_obj].acquire()
    print(wake_up_obj," passed")
    print(reply)
    print(reply.status_code)
    print(reply.text)
    print(reply.reason)


def reducer_function(unique_id, activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/reducer-function/reducer",
        json={"unique_id":str(unique_id), "activation_id":str(activation_id), "reducer_mapping_table_key": "reducer_mapping_table_key_"+str(activation_id)},
        verify=False)
    # event_obj_list[unique_id].wait()
    function_type="reducer"
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    print(wake_up_obj," waiting")
    sema_list[wake_up_obj].acquire()
    print(wake_up_obj," passed")
    print(reply)
    print(reply.status_code)
    print(reply.text)
    print(reply.reason)
    # reply = reply.json()
    # print(reply)


def start_splitdata_instance(parameter_file):
    # print("hello")
    reply = splitdata(parameter_file)
    # reply = reply.json()
    print("Hello")
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    print("nadesh")
    return pickle.loads(r.get(reply["mapper_list_key"])), pickle.loads(r.get(reply["reducer_list_key"])), reply["data_list_key"],  reply["num_blocks"], reply["activation_id"]



def start_mapper_instances(list_of_ids, activation_id):
    mapper_threads = []
    function_type="mapper"
    for unique_id in list_of_ids:  

        wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
        # event_obj_list[id] = threading.Event()
        sema_list[wake_up_obj] = threading.Semaphore(0)
        # event_obj_list.append(threading.Event())
    # print("Mapper Functions started")
    for id in list_of_ids:
        mapper_threads.append(threading.Thread(target=mapper_function, args=[str(id), activation_id]))
    
    for mapper_thread in mapper_threads:
        mapper_thread.start()
    
    for mapper_thread in mapper_threads:
        mapper_thread.join()

    # print("Mapper Functions ended")
    # print() 



def start_reducer_instances(list_of_ids, activation_id):
    reducer_threads = []
    # print("Reducer Functions started")
    function_type="reducer"
    for unique_id in list_of_ids:

        wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
        # event_obj_list[id] = threading.Event()
        sema_list[wake_up_obj] = threading.Semaphore(0)
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
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    pickled_object = pickle.dumps(file_contents)
    r.set(filename, pickled_object)
    file_contents = pickle.loads(r.get(filename))
    # print(file_contents)
    # print()

def output_db(splitdata_activation_id):
    filename = "final-output-"+splitdata_activation_id
    # print("Output stored in Redis with key",filename)
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    file_contents = pickle.loads(r.get(filename))
    # print(file_contents)
    # print()

def partition_based_on_list(split_percentage_list
                            ,data_list):
    num_of_blocks = int(len(data_list))
    # print(split_percentage_list)
    list_size = len(split_percentage_list)
    mapping_table = {}
    job_list = [str(i+1) for i in range(list_size)]
    start=0
    end=0
    for i in range(list_size):
        start=end
        if i==list_size-1:
            end=100
        else:
            end = (int(split_percentage_list[i])+end)
        # print(start, end)
        start_i = float(start/100)
        end_i = float(end/100)
        mapping_table[str(i+1)] = (data_list[int(num_of_blocks*start_i): int(num_of_blocks*end_i)])
    
    # display the list results
    # print("Mapping Table:")
    # print(mapping_table)
    return mapping_table, job_list

def equal_partition(parallel_instances
                    ,data_list):
    num_of_blocks = int(len(data_list))
    # print(int(num_of_blocks/parallel_instances))
    # size of each chunk
    size_per_instance = int(num_of_blocks/parallel_instances) + int(num_of_blocks%parallel_instances)

    # partition the list 
    # mapping_table = list(partition(size_per_instance, o_list))
    count=1
    mapping_table={}
    for i in range(0, num_of_blocks, size_per_instance):
        # print(str(count), data_list[i:i + size_per_instance])
        if count==parallel_instances:
            mapping_table[str(count)] = data_list[i:]
            count+=1
            break
        else:
            mapping_table[str(count)] = data_list[i:i + size_per_instance]
        count+=1
    
    job_list = [str(i+1) for i in range(count-1)]
    # display the list results
    # print("Mapping Table:")
    # print(mapping_table)
    return mapping_table, job_list

def splitdata(parameter_file):
    # print("hello")
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    # print("hello")
    activation_id = os.getenv("__OW_ACTIVATION_ID")
    filename = "input1234.txt"
    mapper_split_percentage_list_key=""
    reducer_split_percentage_list_key=""
    split_file=""
    mapper_distribution_using = ""
    reducer_distribution_using = ""
    mapper_parallel_instances=1
    reducer_parallel_instances=1
    mapper_split_percentage_list = []
    mapper_mapping_table={} 
    mapper_list=[]
    reducer_split_percentage_list = []
    reducer_mapping_table={} 
    reducer_list=[]
    block_size = 100 #in number of characters
    # if len(sys.argv) == 2:
    #     parameter_file = json.loads(sys.argv[1])
    params = parameter_file
    print(params)
    if "input_files" in params.keys():
        input_files = params["input_files"]
        filename = input_files[0]
    # if "activation_id" in params.keys():
    #     activation_id = params["activation_id"]
    if "mapper_split_percentage_list" in params.keys():
        mapper_split_percentage_list = params["mapper_split_percentage_list"]
    if "reducer_split_percentage_list" in params.keys():
        reducer_split_percentage_list = params["reducer_split_percentage_list"]
    if "mapper_distribution_using" in params.keys():
        mapper_distribution_using = params["mapper_distribution_using"]
    if "reducer_distribution_using" in params.keys():
        reducer_distribution_using = params["reducer_distribution_using"]
    if "mapper_parallel_instances" in params.keys():
        mapper_parallel_instances = int(params["mapper_parallel_instances"])
    if "reducer_parallel_instances" in params.keys():
        reducer_parallel_instances = int(params["reducer_parallel_instances"])
    if "split_file" in params.keys():
        split_file = params["split_file"]
    if "block_size" in params.keys():
        block_size = int(params["block_size"])
    
    print("Block Size", block_size/1024,"MB")
    print("Ram used before loading", psutil.virtual_memory()[2])
    print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
    data = pickle.loads(r.get(filename))
    # data = "adasdfasd asd"
    print("Ram used after loading", psutil.virtual_memory()[2])
    print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
    # print(block_size, block_size.type())
    blocks = textwrap.wrap(data, block_size, break_long_words=False)
    # blocks = data
    print("Ram used after wraping", psutil.virtual_memory()[2])
    print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
    print(mapper_parallel_instances)
    print(reducer_parallel_instances)
    num_of_blocks = len(blocks)
    print(num_of_blocks)

    data_list = [str(i+1) for i in range(num_of_blocks)]
    # print(data_list)

    
    # if split_file is false (creating mapper mapping table)
    if mapper_distribution_using=="percentage_list":
        mapper_mapping_table, mapper_list = partition_based_on_list(mapper_split_percentage_list, data_list)
    else:
        mapper_mapping_table, mapper_list = equal_partition(mapper_parallel_instances, data_list)

    r.set("mapper_mapping_table_key_"+str(activation_id), pickle.dumps(mapper_mapping_table))


    # if split_file is false (creating reducer mapping table)
    if reducer_distribution_using=="percentage_list":
        reducer_mapping_table, reducer_list = partition_based_on_list(reducer_split_percentage_list, data_list)
    else:
        reducer_mapping_table, reducer_list = equal_partition(reducer_parallel_instances, data_list)

    r.set("reducer_mapping_table_key_"+str(activation_id), pickle.dumps(reducer_mapping_table))

    block_count=1
    for block in blocks:
        mapper_filename = "mapper-input-"+str(activation_id)+"-"+str(block_count)
        block_count+=1
        pickled_block = pickle.dumps(block)
        r.set(mapper_filename, pickled_block)


    mapper_list_key = "mapper_list-"+str(activation_id)
    reducer_list_key = "reducer_list-"+str(activation_id)
    data_list_key = "data_list-"+str(activation_id)
    r.set(mapper_list_key, pickle.dumps(mapper_list))
    r.set(reducer_list_key, pickle.dumps(reducer_list))
    r.set(data_list_key, pickle.dumps(data_list))
    
    print("Splitdata function -", activation_id)
    print("Driver function -", activation_id)
    print(json.dumps({  "num_blocks": str(len(blocks))
                        ,"activation_id": str(activation_id)
                        ,"mapper_list_key":str(mapper_list_key)
                        ,"reducer_list_key":str(reducer_list_key)
                        ,"data_list_key":str(data_list_key)
    }))
    return {  "num_blocks": str(len(blocks))
                        ,"activation_id": str(activation_id)
                        ,"mapper_list_key":str(mapper_list_key)
                        ,"reducer_list_key":str(reducer_list_key)
                        ,"data_list_key":str(data_list_key)
    }



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

    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
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
