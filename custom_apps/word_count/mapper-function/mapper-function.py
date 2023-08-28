#!/usr/bin/env python3
import os
import redis as redis_lib
import json
import pickle
import sys
import time
import requests
# import rediscluster as redis_lib
from minio import Minio
# import psutil
import io
import socket

# Informs control when the execution is completed
def wake_up(function_type, unique_id, activation_id, web_server_details):
    server_ip = web_server_details["url"]
    server_port = web_server_details["port"]
    driver_url = "http://"+server_ip+":"+server_port+"/wake-up/"
    reply = requests.post(url = driver_url, json = {"function_type": str(function_type), "unique_id": str(unique_id), "activation_id": str(activation_id)})

# Asks control plane to know on which node this specific action is running
def get_worker_node_ip(web_server_details):
    server_ip = web_server_details["url"]
    server_port = web_server_details["port"]
    driver_url = "http://"+server_ip+":"+server_port+"/get_worker_node_ip/"
    reply = requests.post(url = driver_url)
    reply = reply.json()
    return reply["worker_node_ip"]
    
def mapper_logic():
    pass

def main():
    # hostname=socket.gethostname()
    # ip_addr=socket.gethostbyname(hostname)

    params = json.loads(sys.argv[1])
    # print(hostname, ip_addr)
    connection_details = params.get("config_file", "NULL")
    unique_id = params.get("unique_id")
    activation_id = params.get("activation_id")
    # Gets the worker node ip where this mapper function is running
    # This will be used to make connection between this function and the local storage of 
    # the worker node where it is running
    
    # ----------------------------------Remote Storage Details-------------------------------------
    data_store = connection_details["data_store"]
    remote_endpoint = data_store["endpoint"]
    remote_access_key = data_store["access_key"]
    remote_secret_key = data_store["secret_key"]
    remote_bucket_name = data_store["bucket_name"]
    
    remote_client = Minio(
        endpoint=remote_endpoint,
        access_key=remote_access_key,
        secret_key=remote_secret_key,
        secure=False,
    )
    found = remote_client.bucket_exists(bucket_name = remote_bucket_name)
    if not found:
        remote_client.make_bucket(bucket_name = remote_bucket_name)
    else:
        print("remote bucket '",remote_bucket_name,"' already exists")
    # ----------------------------------Remote Storage Details-------------------------------------


    # ----------------------------------Temporary Storage Details-------------------------------------
    intermediate_storage = connection_details["intermediate_storage"]
    temporary_bucket_name = intermediate_storage["bucket_name"]
    temporary_access_key = intermediate_storage["access_key"]
    temporary_secret_key = intermediate_storage["secret_key"]
    temporary_endpoint_ip=None
    if intermediate_storage["location"] == "local":
        temporary_endpoint_ip = get_worker_node_ip(connection_details["web_server"])
    else:
        temporary_endpoint_ip = intermediate_storage["endpoint_ip"]
    temporary_endpoint_port = intermediate_storage["endpoint_port"]
    temporary_endpoint = temporary_endpoint_ip+":"+temporary_endpoint_port

    print("temporary_endpoint_ip = ", temporary_endpoint_ip)

    temporary_client = Minio(
        endpoint=temporary_endpoint,
        access_key=temporary_access_key,
        secret_key=temporary_secret_key,
        secure=False,
    )
    found = temporary_client.bucket_exists(bucket_name = temporary_bucket_name)
    if not found:
        temporary_client.make_bucket(bucket_name = temporary_bucket_name)
    else:
        print("Local bucket '",temporary_bucket_name,"' already exists")
    # ----------------------------------Temporary Storage Details-------------------------------------


    # ----------------------------------Redi Type Details-------------------------------------
    startup_nodes = None
    redis_host = None
    redis_conn = None
    if connection_details["storage"]["type"] == "redis":
        redis_host = connection_details["storage"]["redis"]
        redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
    else:
        startup_nodes = connection_details["storage"]["redis_cluster_nodes"]
        redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # ----------------------------------Redi Type Details-------------------------------------
    

    mapper_mapping_table_key = params.get("mapper_mapping_table_key")
    del(params)
    mapper_mapping_table = pickle.loads(redis_conn.get(mapper_mapping_table_key))

    if intermediate_storage["location"] == "local":
        local_mapping_table = {}
        local_mapping_table_flag = redis_conn.get("local_mapping_table")
        if local_mapping_table_flag != None:
            local_mapping_table = pickle.loads(local_mapping_table_flag)
        local_mapping_table[str(temporary_endpoint_ip)] = str(unique_id)
        print(local_mapping_table)
        redis_conn.set("local_mapping_table", pickle.dumps(local_mapping_table))
        

    
    print("Unique Id",unique_id)
    function_type = "mapper"
    # log_obj = function_type+"-"+unique_id+"-"+activation_id+"-logs"
    # log_data = pickle.loads(redis_conn.get(log_obj))
    
    # log_data+=log_obj+"\n"
    # redis_conn.set(log_obj, pickle.dumps(log_data))
    for data_id in mapper_mapping_table[unique_id]:
        mapper_input_param = "mapper-input-"+activation_id+"-"+data_id
        # log_data+=mapper_input_param+"\n"
        # redis_conn.set(log_obj, pickle.dumps(log_data))
        input_data = None
        
        # input_data = pickle.loads(remote_client.get_object(bucket_name = remote_bucket_name, object_name = mapper_input_param).data)
        input_data = pickle.loads(temporary_client.get_object(bucket_name = temporary_bucket_name, object_name = mapper_input_param).data)
        # log_data+="data loaded in memory"+"\n"
        # redis_conn.set(log_obj, pickle.dumps(log_data))
        





        
        ### Mapper Logic Starts ###
        tokenize_data = input_data.split()
        del(input_data)
        for i in range(len(tokenize_data)):
            tokenize_data[i] = tokenize_data[i].replace(",", "")
            tokenize_data[i] = tokenize_data[i].replace(".", "")

        ### Mapper Logic Ends ###
        # log_data+="mapper logic ended"+"\n"
        # redis_conn.set(log_obj, pickle.dumps(log_data))
        






        reducer_instance = "reducer-input-"+activation_id+"-"+data_id

        pickled_tokenize_data = pickle.dumps(tokenize_data)
        
        # log_data+="data pickled"+"\n"
        # redis_conn.set(log_obj, pickle.dumps(log_data))
        # if intermediate_storage["location"] == "local":
        temporary_client.put_object(bucket_name = temporary_bucket_name, object_name = reducer_instance, data=io.BytesIO(pickled_tokenize_data), length=len(pickled_tokenize_data))
        # else:
        #     remote_client.put_object(bucket_name = remote_bucket_name, object_name = reducer_instance, data=io.BytesIO(pickled_tokenize_data), length=len(pickled_tokenize_data))
        del(tokenize_data)
        # log_data+="data stored in redis"+"\n"
        # redis_conn.set(log_obj, pickle.dumps(log_data))
        del(pickled_tokenize_data)
        

    mapper_activation_id = os.getenv("__OW_ACTIVATION_ID")

    print("Mapper function -", mapper_activation_id)
    print("Driver function -", activation_id)
    
    wake_up(function_type, unique_id, activation_id, connection_details["web_server"])
    # result_dict = { 
    #         "mapper-output-"+unique_id: str(mapper_mapping_table[unique_id])
    #         ,"activation_id": str(mapper_activation_id)
    #         }
    # print("result_dict", result_dict)
    # print(json.dumps({"result_dict": result_dict}))
    # return({"result_dict": result_dict})

    print(json.dumps( { 
                    "mapper-output-"+unique_id: str(mapper_mapping_table[unique_id])
                    ,"activation_id": str(mapper_activation_id)
                    }))
    
    return( { 
            "mapper-output-"+unique_id: str(mapper_mapping_table[unique_id])
            ,"activation_id": str(mapper_activation_id)
            })


if __name__ == "__main__":
    main()
