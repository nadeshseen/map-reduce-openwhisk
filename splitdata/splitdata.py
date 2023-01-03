#!/usr/bin/env python3
import redis
import os
import json
import pickle
import sys
import math
import textwrap
import time
import psutil
data_list=[]

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

def main():
    # print("hello")
    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
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
    if len(sys.argv) == 2:
        parameter_file = json.loads(sys.argv[1])
        params = parameter_file["parameter_file"]
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
    
    print("Block Size", block_size)
    print("Ram used before loading", psutil.virtual_memory()[2])
    data = pickle.loads(r.get(filename))
    # data = "adasdfasd asd"
    print("Ram used after loading", psutil.virtual_memory()[2])
    # print(block_size, block_size.type())
    blocks = textwrap.wrap(data, block_size, break_long_words=False)
    # blocks = data
    print("Ram used after wraping", psutil.virtual_memory()[2])
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

if __name__ == "__main__":
    main()
