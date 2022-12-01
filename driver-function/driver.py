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

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def start_splitdata_instance(log_data, input_filename, driver_activation_id, mapper_split_percentage_list_key, reducer_split_percentage_list_key, split_file):
    # print("Split Data")
    log_data+="Split Data"+"\n"

    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/splitdata-function/split"
    ,json = {
            "filename": str(input_filename)
            ,"driver_activation_id":str(driver_activation_id)
            ,"mapper_split_percentage_list_key":mapper_split_percentage_list_key
            ,"reducer_split_percentage_list_key":reducer_split_percentage_list_key
            ,"split_file": split_file
            }
    ,verify=False)
    reply = reply.json()

    # print(reply)
    log_data+=str(reply)+"\n"

    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    return log_data, pickle.loads(r.get(reply["mapper_list_key"])), pickle.loads(r.get(reply["reducer_list_key"])), reply["data_list_key"]

def mapper_function(unique_id, driver_activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/mapper-function/mapper",
        json={"unique_id":str(unique_id), "driver_activation_id":str(driver_activation_id), "mapper_mapping_table_key": "mapper_mapping_table_key_"+str(driver_activation_id)},
        verify=False)
    reply = reply.json()
    # print(reply)

def start_mapper_instances(list_of_ids, driver_activation_id):
    mapper_threads = []
    # print("Mapper Functions started")
    for id in list_of_ids:
        mapper_threads.append(threading.Thread(target=mapper_function, args=[str(id), driver_activation_id]))
    
    for mapper_thread in mapper_threads:
        mapper_thread.start()
    
    for mapper_thread in mapper_threads:
        mapper_thread.join()

    # print("Mapper Functions ended")
    # print() 

def reducer_function(unique_id, driver_activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/reducer-function/reducer",
        json={"unique_id":str(unique_id), "driver_activation_id":str(driver_activation_id), "reducer_mapping_table_key": "reducer_mapping_table_key_"+str(driver_activation_id)},
        verify=False)
    reply = reply.json()
    # print(reply)


def start_reducer_instances(list_of_ids, driver_activation_id):
    reducer_threads = []
    # print("Reducer Functions started")
    for id in list_of_ids:
        reducer_threads.append(threading.Thread(target=reducer_function, args=[str(id), driver_activation_id]))
    
    for reducer_thread in reducer_threads:
        reducer_thread.start()
    
    for reducer_thread in reducer_threads:
        reducer_thread.join()

    # print("Reducer Functions ended")
    # print()

def start_aggregator_instances(data_list, driver_activation_id):
    # print("Aggregator Functions started")
    ids = {}
    ids["driver_activation_id"] = driver_activation_id
    ids["data_list"] = data_list
    print(ids)
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/aggregator-function/aggregate",
        json=ids,
        verify=False)
    reply = reply.json()
    # print(reply)  

    # print("Aggrregator Functions ended")
    print()
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

def main():
    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    log_data = ""
    # input_db()
    input_files = [] #default
    split_file = "true"
    parallel_instances = 1
    number_of_mapper_instances = 1
    driver_activation_id = os.getenv("__OW_ACTIVATION_ID")
    mapper_list=[]
    reducer_list=[]
    data_list=[]
    mapper_split_percentage_list=[]
    reducer_split_percentage_list=[]

    if len(sys.argv) == 2:
        params = json.loads(sys.argv[1])
        if "input_files" in params.keys():
            input_files = params["input_files"]
        if "split_file" in params.keys():
            split_file = params["split_file"]
            
        if "mapper_split_percentage_list" in params.keys():
            mapper_split_percentage_list = params["mapper_split_percentage_list"]
        if "reducer_split_percentage_list" in params.keys():
            reducer_split_percentage_list = params["reducer_split_percentage_list"]
    

    if split_file == "false":
        pass
    else:
        input_filename = input_files[0]
        
        mapper_split_percentage_list_key = "mapper_split_percentage_list_"+str(driver_activation_id)
        reducer_split_percentage_list_key = "reducer_split_percentage_list_"+str(driver_activation_id)
        r.set(mapper_split_percentage_list_key, pickle.dumps(mapper_split_percentage_list))
        r.set(reducer_split_percentage_list_key, pickle.dumps(reducer_split_percentage_list))
        start = time.time()
        log_data, mapper_list, reducer_list, data_list = \
            start_splitdata_instance(log_data, input_filename, driver_activation_id, mapper_split_percentage_list_key, reducer_split_percentage_list_key, split_file)
        end = time.time()
        split_time = end-start
        print(end-start)
        # print(mapper_split_percentage_list_key, mapper_split_percentage_list)
        # print(reducer_split_percentage_list_key, reducer_split_percentage_list)

    start = time.time()
    start_mapper_instances(mapper_list, driver_activation_id)
    end = time.time()
    print(end-start)
    mapper_time = end-start

    start = time.time()
    start_reducer_instances(reducer_list, driver_activation_id)
    end = time.time()
    print(end-start)
    reducer_time = end-start

    start = time.time()
    agg_reply = start_aggregator_instances(data_list, driver_activation_id)
    end = time.time()
    print(end-start)
    aggre_time = end-start
    
    r.set("log_data", pickle.dumps(log_data))

    print(json.dumps({
        "input":{
            "input_files": str(input_filename)
            ,"split_file": str(split_file)
            # ,"parallel_instances": str(parallel_instances)
            ,"driver_activation_id":str(driver_activation_id)
            ,"mapper_split_percentage_list":str(mapper_split_percentage_list)
            ,"reducer_split_percentage_list":str(reducer_split_percentage_list)
        }
        ,"output":{
            "output_filename": str(agg_reply["output_filename"])
            ,"number_of_mapper_instances": str(len(mapper_list))
            ,"number_of_reducer_instances": str(len(reducer_list))
            ,"output": str(agg_reply["aggregator-output"])
            ,"split_time": str(split_time)
            ,"mapper_time": str(mapper_time)
            ,"reducer_time": str(reducer_time)
            ,"aggre_time": str(aggre_time)
            
        }
    }))


if __name__=="__main__":
    main()