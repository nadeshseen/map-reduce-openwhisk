#!/usr/bin/env python3

import sys
import requests
import subprocess
import threading
import redis
import pickle
import json
import os
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def start_splitdata_instance(log_data, input_filename, driver_activation_id, split_percentage_list_key):
    # print("Split Data")
    log_data+="Split Data"+"\n"

    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/splitdata-function/split"
    ,json = {"filename": str(input_filename), "driver_activation_id":str(driver_activation_id), "split_percentage_list_key":split_percentage_list_key}
    ,verify=False)
    reply = reply.json()

    # print(reply)
    log_data+=str(reply)+"\n"

    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    return int(reply["splitdata"]), log_data, pickle.loads(r.get(reply["list_of_ids"]))

def mapper_function(unique_id, driver_activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/mapper-function/mapper",
        json={"unique_id":str(unique_id), "driver_activation_id":str(driver_activation_id)},
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
        json={"unique_id":str(unique_id), "driver_activation_id":str(driver_activation_id)},
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

def start_aggregator_instances(list_of_ids, driver_activation_id):
    # print("Aggregator Functions started")
    ids = {}
    ids["driver_activation_id"] = driver_activation_id
    ids["list_of_ids"] = list_of_ids
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
    split_in_ratio = "true"
    parallel_instances = 1
    number_of_mapper_instances = 1
    driver_activation_id = os.getenv("__OW_ACTIVATION_ID")
    list_of_ids=[]
    split_percentage_list=[]
    if len(sys.argv) == 2:
        params = json.loads(sys.argv[1])
        if "input_files" in params.keys():
            input_files = params["input_files"]
        if "split_in_ratio" in params.keys():
            split_in_ratio = params["split_in_ratio"]
        if "parallel_instances" in params.keys():
            parallel_instances = int(params["parallel_instances"])
        if "split_percentage_list" in params.keys():
            split_percentage_list = params["split_percentage_list"]
    

    if split_in_ratio == "false":
        number_of_mapper_instances = parallel_instances
    else:
        input_filename = input_files[0]

        split_percentage_list_key = "split_percentage_list_"+str(driver_activation_id)
        r.set(split_percentage_list_key, pickle.dumps(split_percentage_list))
        number_of_mapper_instances, log_data, list_of_ids = start_splitdata_instance(log_data, input_filename, driver_activation_id, split_percentage_list_key)
        print(split_percentage_list_key)

    # number_of_reducer_instances = number_of_mapper_instances
    # start_mapper_instances(list_of_ids, driver_activation_id)
    # start_reducer_instances(list_of_ids, driver_activation_id)
    # agg_reply = start_aggregator_instances(list_of_ids, driver_activation_id)


    
    r.set("log_data", pickle.dumps(log_data))

    print(json.dumps({
        "input":{
            "input_files": str(input_filename)
            ,"split_in_ratio": str(split_in_ratio)
            ,"parallel_instances": str(parallel_instances)
            ,"driver_activation_id":str(driver_activation_id)
            ,"split_percentage_list":str(split_percentage_list)
        }
        # ,"output":{
        #     "output_filename": str(agg_reply["output_filename"])
        #     ,"number_of_mapper_instances-": str(number_of_mapper_instances)
        #     ,"number_of_reducer_instances": str(number_of_reducer_instances)
        #     ,"output": str(agg_reply["aggregator-output"])
        # }
    }))


if __name__=="__main__":
    main()