#!/usr/bin/env python3
import redis, rediscluster
import os
import json
import pickle
import sys
import math
import textwrap
import time
import psutil

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

def multiple_partitions(data_list, blocks_per_instance, job_type, activation_id, parallel_instances):
    num_of_blocks = int(len(data_list))
    job_count=1
    print(parallel_instances)
    mapping_table={}
    for i in range(0, num_of_blocks, blocks_per_instance):
        wake_up_obj = job_type+"-"+str(job_count)+"-"+str(activation_id)
        if job_type=="mapper":
            mapper_response[wake_up_obj]="x"
        else:
            reducer_response[wake_up_obj]="x"
        if job_count==parallel_instances:
            mapping_table[str(job_count)] = data_list[i:]
            job_count+=1
            break
        else:
            mapping_table[str(job_count)] = data_list[i:i+blocks_per_instance]
        job_count+=1
    
    job_list = [str(i+1) for i in range(job_count-1)]
    # display the list results
    # print("Mapping Table:")
    # print(mapping_table)
    return mapping_table, job_list

def main():
    startup_nodes = None
    redis_conn = None
    connection_details = parameter_file
    if connection_details["storage"]["type"] == "redis":
        redis_host = connection_details["storage"]["redis"]
        redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
    else:
        startup_nodes = connection_details["storage"]["redis_cluster_nodes"]
        redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # print("hello")
    activation_id = os.getenv("__OW_ACTIVATION_ID")
    # activation_id = 
    filename = ""
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
    input_data_size=None
    instance_allocation_size=None
    if "input_files" in params.keys():
        input_files = params["input_files"]
        filename = input_files[0]
    if "activation_id" in params.keys():
        activation_id = params["activation_id"]
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
    if "blocks_per_instance" in params.keys():
        blocks_per_instance = int(params["blocks_per_instance"])
    if "input_data_size" in params.keys():
        input_data_size = int(params["input_data_size"])
    if "instance_allocation_size" in params.keys():
        instance_allocation_size = int(params["instance_allocation_size"])
    if "split_required" in params.keys():
        split_required = params["split_required"]
    
    data_store = parameter_file.get("data_store", "NULL")
    # if data_store != "NULL":

    client = Minio(
        endpoint = data_store["endpoint"],
        access_key=data_store["access_key"],
        secret_key=data_store["secret_key"],
        secure=False,
    )

    number_of_blocks = math.ceil(input_data_size/block_size)
    # print("instance_allocation_size", instance_allocation_size)
    print("number_of_blocks", number_of_blocks)
    
    block_count=0
    per_instance_size = None
    if split_required == "false":

        per_instance_size = math.ceil(input_data_size/mapper_parallel_instances)
        # blocks_per_instance = math.ceil(per_instance_size/block_size)
        block_count=number_of_blocks
        # for block_count in range(0, number_of_blocks):
        #     print(block_count+1)
    else:
        pass

    # print("num_of_blocks =", num_of_blocks)
    blocks_per_instance = int(block_count/mapper_parallel_instances)
    print("Number of blocks",block_count, number_of_blocks)
    print("Total Number of blocks of size",block_size,"=", block_count)

    print("Blocks Per Instance : ", blocks_per_instance)
    # mapper_parallel_instances = block_count
    reducer_parallel_instances = mapper_parallel_instances
    data_list = [str(i+1) for i in range(block_count)]
    print(data_list)

    
    # if split_file is false (creating mapper mapping table)
    if mapper_distribution_using=="percentage_list":
        mapper_mapping_table, mapper_list = partition_based_on_list(mapper_split_percentage_list, data_list)
    elif mapper_distribution_using=="equal":
        mapper_mapping_table, mapper_list = equal_partition(mapper_parallel_instances, data_list)
    else:
        mapper_mapping_table, mapper_list = multiple_partitions(data_list, blocks_per_instance, "mapper", activation_id, mapper_parallel_instances)

    redis_conn.set("mapper_mapping_table_key_"+str(activation_id), pickle.dumps(mapper_mapping_table))
    print("mapper_mapping_table", mapper_mapping_table)
    print("mapper_list", mapper_list)

    # if split_file is false (creating reducer mapping table)
    if reducer_distribution_using=="percentage_list":
        reducer_mapping_table, reducer_list = partition_based_on_list(reducer_split_percentage_list, data_list)
    elif reducer_distribution_using=="equal":
        reducer_mapping_table, reducer_list = equal_partition(reducer_parallel_instances, data_list)
    else:
        reducer_mapping_table, reducer_list = multiple_partitions(data_list, blocks_per_instance, "reducer", activation_id, reducer_parallel_instances)
        

    redis_conn.set("reducer_mapping_table_key_"+str(activation_id), pickle.dumps(reducer_mapping_table))
    print("reducer_mapping_table", reducer_mapping_table)
    print("reducer_list", reducer_list)

    mapper_list_key = "mapper_list-"+str(activation_id)
    reducer_list_key = "reducer_list-"+str(activation_id)
    data_list_key = "data_list-"+str(activation_id)
    
    redis_conn.set(mapper_list_key, pickle.dumps(mapper_list))
    redis_conn.set(reducer_list_key, pickle.dumps(reducer_list))
    redis_conn.set(data_list_key, pickle.dumps(data_list))
    
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

if __name__ == "__main__":
    main()
