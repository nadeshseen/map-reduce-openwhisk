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

coordinator_ip=""
coordinator_port=""

response = {}
mapper_response = {}
reducer_response = {}
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
    response[wake_up_obj] = "Woken"
    if function_type=="mapper":
        mapper_response[wake_up_obj]="Done"
    if function_type=="reducer":
        reducer_response[wake_up_obj]="Done"
    sema_list[wake_up_obj].release()
    
    output_json = json.dumps(response)
    return output_json


def mapper_function(unique_id, activation_id, mapper_url, config_file):
    function_type="mapper"
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    mapper_response[wake_up_obj] = "Initiated"
    
    reply = requests.post(url = mapper_url,
        json={"unique_id":str(unique_id), "activation_id":str(activation_id)
        , "mapper_mapping_table_key": "mapper_mapping_table_key_"+str(activation_id)
        , "config_file":config_file}
        , verify=False)
    # event_obj_list[unique_id].wait()
    
    mapper_response[wake_up_obj] = "Executing"
    # print(wake_up_obj," waiting")
    
    sema_list[wake_up_obj].acquire()
    mapper_response[wake_up_obj] = "Done"
    # print(wake_up_obj," passed")
    # print(reply)
    # print(reply.status_code)
    # print(reply.text)
    # print(reply.reason)
    print("Mapper", unique_id)
    print(json.dumps(mapper_response, indent = 1))


def reducer_function(unique_id, activation_id, reducer_url, config_file):
    function_type="reducer"
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    reducer_response[wake_up_obj] = "Initiated"
    reply = requests.post(url = reducer_url,
        json={"unique_id":str(unique_id), "activation_id":str(activation_id)
        , "reducer_mapping_table_key": "reducer_mapping_table_key_"+str(activation_id)
        , "config_file": config_file}
        ,verify=False)
    # event_obj_list[unique_id].wait()
    reducer_response[wake_up_obj] = "Executing"
    # print(wake_up_obj," waiting")
    sema_list[wake_up_obj].acquire()

    reducer_response[wake_up_obj] = "Done"
    # print(wake_up_obj," passed")
    # print(reply)
    # print(reply.status_code)
    # print(reply.text)
    # print(reply.reason)
    print("Reducer", unique_id)
    print(json.dumps(reducer_response, indent = 1))
    # reply = reply.json()
    # print(reply)


def start_splitdata_instance(parameter_file):
    # print("hello")
    reply = splitdata(parameter_file)
    # reply = reply.json()
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    return pickle.loads(r.get(reply["mapper_list_key"])), pickle.loads(r.get(reply["reducer_list_key"])), reply["data_list_key"],  reply["num_blocks"], reply["activation_id"]



def start_mapper_instances(list_of_ids, activation_id, mapper_url, config_file):
    mapper_threads = []
    function_type="mapper"
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    
    for unique_id in list_of_ids:  
        
        wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
        # event_obj_list[id] = threading.Event()
        log_obj = wake_up_obj+"-logs"
        r.set(log_obj,pickle.dumps(""))
        sema_list[wake_up_obj] = threading.Semaphore(0)
        # event_obj_list.append(threading.Event())
    # print("Mapper Functions started")
    for id in list_of_ids:
        mapper_threads.append(threading.Thread(target=mapper_function, args=[str(id), activation_id, mapper_url, config_file]))
    
    for mapper_thread in mapper_threads:
        mapper_thread.start()
    
    for mapper_thread in mapper_threads:
        mapper_thread.join()

    # print("Mapper Functions ended")
    # print() 



def start_reducer_instances(list_of_ids, activation_id, reducer_url, config_file):
    reducer_threads = []
    # print("Reducer Functions started")
    function_type="reducer"
    for unique_id in list_of_ids:

        wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
        # event_obj_list[id] = threading.Event()
        sema_list[wake_up_obj] = threading.Semaphore(0)
    for id in list_of_ids:
        reducer_threads.append(threading.Thread(target=reducer_function, args=[str(id), activation_id, reducer_url, config_file]))
    
    for reducer_thread in reducer_threads:
        reducer_thread.start()
    
    for reducer_thread in reducer_threads:
        reducer_thread.join()

    # print("Reducer Functions ended")
    # print()

def start_aggregator_instances(data_list_key, activation_id, aggregator_url):
    # print("Aggregator Functions started")
    ids = {}
    ids["activation_id"] = activation_id
    ids["data_list_key"] = data_list_key
    # print(ids)
    reply = requests.post(url = aggregator_url,
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

def single_partitions(data_list):
    num_of_blocks = int(len(data_list))
    count=1
    mapping_table={}
    for i in range(0, num_of_blocks):
        mapping_table[str(count)] = data_list[i:i+1]
        count+=1
    
    job_list = [str(i+1) for i in range(count-1)]
    # display the list results
    # print("Mapping Table:")
    # print(mapping_table)
    return mapping_table, job_list

def multiple_partitions(data_list, blocks_per_instance, job_type, activation_id):
    num_of_blocks = int(len(data_list))
    job_count=1
    mapping_table={}
    for i in range(0, num_of_blocks, blocks_per_instance):
        mapping_table[str(job_count)] = data_list[i:i+blocks_per_instance]
        wake_up_obj = job_type+"-"+str(job_count)+"-"+str(activation_id)
        if job_type=="mapper":
            mapper_response[wake_up_obj]="x"
        else:
            reducer_response[wake_up_obj]="x"
        
        job_count+=1
    
    job_list = [str(i+1) for i in range(job_count-1)]
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
    # block_size = 100 #in number of characters
    # if len(sys.argv) == 2:
    #     parameter_file = json.loads(sys.argv[1])
    params = parameter_file
    print(json.dumps(params, indent = 1))
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
        block_size = int(params["block_size"])*1024*1024
    if "blocks_per_instance" in params.keys():
        blocks_per_instance = int(params["blocks_per_instance"])

    print("Blocks Per Instance : ", blocks_per_instance)
    # if "block_size" in params.keys():
    #     block_size = int(params["block_size"])
    
    # print("Block Size", block_size/2048,"MB")
    # print("Ram used before loading", psutil.virtual_memory()[2])
    # print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
    block_count=0
    for filename in input_files:
        print(filename)

        # block_size = 0.5*1024*1024
        data = pickle.loads(r.get(filename))
        data_size = len(data)
        print("Data size =", data_size/1024/1024,"MB")
        # block_size = data_size/mapper_parallel_instances
        print("New block size =", block_size/1024/1024,"MB")

        # data = "adasdfasd asd"
        # print("Ram used after loading", psutil.virtual_memory()[2])
        # print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
        # print(block_size, block_size.type())
        blocks = textwrap.wrap(data, block_size, break_long_words=False)
        del(data)
        for block in blocks:
            block_count+=1
            mapper_filename = "mapper-input-"+str(activation_id)+"-"+str(block_count)
            # if block_count==10:
            #     print(block)
            print(mapper_filename, len(block)/1024/1024,"MB")
            r.set(mapper_filename, pickle.dumps(block))
        # blocks = data
        # print("Ram used after wraping", psutil.virtual_memory()[2])
        # print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
        # print("mapper_parallel_instances =", mapper_parallel_instances)
        # print("reducer_parallel_instances =", reducer_parallel_instances)
        # num_of_blocks = len(blocks)
        del(blocks)
    # print("num_of_blocks =", num_of_blocks)
    print("block count = ", block_count)
    mapper_parallel_instances = block_count
    reducer_parallel_instances = block_count
    data_list = [str(i+1) for i in range(block_count)]
    # print(data_list)

    
    # if split_file is false (creating mapper mapping table)
    # if mapper_distribution_using=="percentage_list":
    #     mapper_mapping_table, mapper_list = partition_based_on_list(mapper_split_percentage_list, data_list)
    # elif mapper_distribution_using=="equal":
    # mapper_mapping_table, mapper_list = equal_partition(mapper_parallel_instances, data_list)
    # else:
    mapper_mapping_table, mapper_list = multiple_partitions(data_list, blocks_per_instance, "mapper", activation_id)

    r.set("mapper_mapping_table_key_"+str(activation_id), pickle.dumps(mapper_mapping_table))
    print(mapper_mapping_table, mapper_list)

    # if split_file is false (creating reducer mapping table)
    # if reducer_distribution_using=="percentage_list":
    #     reducer_mapping_table, reducer_list = partition_based_on_list(reducer_split_percentage_list, data_list)
    # elif reducer_distribution_using=="equal":
    # reducer_mapping_table, reducer_list = equal_partition(reducer_parallel_instances, data_list)
    # else:
    reducer_mapping_table, reducer_list = multiple_partitions(data_list, blocks_per_instance, "reducer", activation_id)
        

    r.set("reducer_mapping_table_key_"+str(activation_id), pickle.dumps(reducer_mapping_table))

    mapper_list_key = "mapper_list-"+str(activation_id)
    reducer_list_key = "reducer_list-"+str(activation_id)
    data_list_key = "data_list-"+str(activation_id)
    r.set(mapper_list_key, pickle.dumps(mapper_list))
    r.set(reducer_list_key, pickle.dumps(reducer_list))
    r.set(data_list_key, pickle.dumps(data_list))
    
    print("Splitdata function -", activation_id)
    print("Driver function -", activation_id)
    print(json.dumps({  "num_blocks": str(block_count)
                        ,"activation_id": str(activation_id)
                        ,"mapper_list_key":str(mapper_list_key)
                        ,"reducer_list_key":str(reducer_list_key)
                        ,"data_list_key":str(data_list_key)
    }))
    return {  "num_blocks": str(block_count)
                        ,"activation_id": str(activation_id)
                        ,"mapper_list_key":str(mapper_list_key)
                        ,"reducer_list_key":str(reducer_list_key)
                        ,"data_list_key":str(data_list_key)
    }



@app.route('/driver-function/', methods=['GET', 'POST'])
def main():
    response = {}
    mapper_response={}
    reducer_response={}
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    
    print("Driver Server Function:")
    parameter_file = request.json
    mapper_url = parameter_file.get("mapper_url", "NULL")
    reducer_url = parameter_file.get("reducer_url", "NULL")
    aggregator_url = parameter_file.get("aggregator_url", "NULL")
    if mapper_url=="NULL" or reducer_url=="NULL"or aggregator_url=="NULL":
        return json.dumps({"Output":"Failed"})
    log_data = ""

    config_file = {"web_server":{
                        "url": coordinator_ip
                        ,"port": coordinator_port
                    }
                    ,"storage": parameter_file.get("storage", "NULL")
    }
    # config_file.update(parameter_file)
    print(json.dumps(config_file, indent = 1))
    # web_server_spec = 

    print(time.time())
    print("Split Stage")
    start = time.time()
    mapper_list, reducer_list, data_list_key, num_blocks, activation_id = start_splitdata_instance(parameter_file)
    end = time.time()
    split_time = end-start
    print("split_time = " + str(split_time) + "s " + str(split_time/60) +"m")

    # activation_id = str(time.time())
    print("Mapper Stage")
    start = time.time()
    start_mapper_instances(mapper_list, activation_id, mapper_url, config_file)
    end = time.time()
    mapper_time = end-start
    print("mapper_time = " + str(mapper_time) + "s " + str(mapper_time/60) +"m")
    
    print("Reducer Stage")
    start = time.time()
    start_reducer_instances(reducer_list, activation_id, reducer_url, config_file)
    end = time.time()
    reducer_time = end-start
    print("reducer_time = " + str(reducer_time) + "s " + str(reducer_time/60) +"m")


    print("Aggregate Stage")
    start = time.time()
    agg_reply = start_aggregator_instances(data_list_key, activation_id, aggregator_url)
    end = time.time()
    aggre_time = end-start
    print("aggre_time = " + str(aggre_time) + "s " + str(aggre_time/60) +"m")

    time_stat = {
        "split_time":split_time
        ,"mapper_time":mapper_time
        ,"reducer_time":reducer_time
        ,"aggre_time":aggre_time
    }

    
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
    coordinator_ip=sys.argv[1]
    coordinator_port=sys.argv[2]
    print(coordinator_ip, coordinator_port)
    app.run(host=coordinator_ip, port=int(coordinator_port))
