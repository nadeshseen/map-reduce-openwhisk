#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
import time
import requests
import rediscluster
from minio import Minio
import psutil
import io
import socket

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
    
    # Gets the worker node ip where this mapper function is running
    # This will be used to make connection between this function and the local storage of 
    # the worker node where it is running
    
    data_store = connection_details["data_store"]
    intermediate_storage = connection_details["intermediate_storage"]
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

    
    local_bucket_name = intermediate_storage["bucket_name"]
    local_client = None
    local_endpoint_ip = None
    # #testing
    # local_endpoint_ip = get_worker_node_ip(connection_details["web_server"])

    if intermediate_storage["location"] == "local":
        
        local_endpoint_ip = get_worker_node_ip(connection_details["web_server"])
        local_access_key = intermediate_storage["access_key"]
        local_secret_key = intermediate_storage["secret_key"]
        local_endpoint_port = intermediate_storage["endpoint_port"]
        
        local_endpoint = local_endpoint_ip+":"+local_endpoint_port
        print("local_endpoint_ip = ", local_endpoint_ip)

        local_client = Minio(
            endpoint=local_endpoint,
            access_key=local_access_key,
            secret_key=local_secret_key,
            secure=False,
        )
        found = local_client.bucket_exists(bucket_name = local_bucket_name)
        if not found:
            local_client.make_bucket(bucket_name = local_bucket_name)
        else:
            print("Local bucket '",local_bucket_name,"' already exists")


    startup_nodes = connection_details["storage"]["startup_nodes"]
    rediscluster_conn = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    unique_id = params.get("unique_id")
    activation_id = params.get("activation_id")
    mapper_mapping_table_key = params.get("mapper_mapping_table_key")
    del(params)
    mapper_mapping_table = pickle.loads(rediscluster_conn.get(mapper_mapping_table_key))
    if local_endpoint_ip != None:
        local_mapping_table = {}
        local_mapping_table_flag = rediscluster_conn.get("local_mapping_table")
        if local_mapping_table_flag != None:
            local_mapping_table = pickle.loads(local_mapping_table_flag)
        local_mapping_table[str(local_endpoint_ip)] = str(unique_id)
        # local_mapping_table = pickle.dumps({str(local_endpoint_ip):str(unique_id)})
        print(local_mapping_table)
        rediscluster_conn.set("local_mapping_table", pickle.dumps(local_mapping_table))
        

    
    print("Unique Id",unique_id)
    function_type = "mapper"
    # log_obj = function_type+"-"+unique_id+"-"+activation_id+"-logs"
    # log_data = pickle.loads(rediscluster_conn.get(log_obj))
    
    # log_data+=log_obj+"\n"
    # rediscluster_conn.set(log_obj, pickle.dumps(log_data))
    for data_id in mapper_mapping_table[unique_id]:
        mapper_input_param = "mapper-input-"+activation_id+"-"+data_id
        # log_data+=mapper_input_param+"\n"
        # rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        input_data = None
        
        input_data = pickle.loads(remote_client.get_object(bucket_name = remote_bucket_name, object_name = mapper_input_param).data)
        # log_data+="data loaded in memory"+"\n"
        # rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        





        
        ### Mapper Logic Starts ###
        tokenize_data = input_data.split()
        del(input_data)
        for i in range(len(tokenize_data)):
            tokenize_data[i] = tokenize_data[i].replace(",", "")
            tokenize_data[i] = tokenize_data[i].replace(".", "")

        ### Mapper Logic Ends ###
        # log_data+="mapper logic ended"+"\n"
        # rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        






        reducer_instance = "reducer-input-"+activation_id+"-"+data_id

        pickled_tokenize_data = pickle.dumps(tokenize_data)
        
        # log_data+="data pickled"+"\n"
        # rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        if intermediate_storage["location"] == "local":
            local_client.put_object(bucket_name = local_bucket_name, object_name = reducer_instance, data=io.BytesIO(pickled_tokenize_data), length=len(pickled_tokenize_data))
        else:
            remote_client.put_object(bucket_name = remote_bucket_name, object_name = reducer_instance, data=io.BytesIO(pickled_tokenize_data), length=len(pickled_tokenize_data))
        del(tokenize_data)
        # log_data+="data stored in redis"+"\n"
        # rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        del(pickled_tokenize_data)
        

    mapper_activation_id = os.getenv("__OW_ACTIVATION_ID")

    print("Mapper function -", mapper_activation_id)
    print("Driver function -", activation_id)
    
    wake_up(function_type, unique_id, activation_id, connection_details["web_server"])
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
