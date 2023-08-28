#!/usr/bin/env python3
import os
import redis as redis_lib
import json
import pickle
import sys
import requests
# import rediscluster as redis_lib
from minio import Minio
import io

def wake_up(function_type, unique_id, activation_id, web_server_details):
    server_ip = web_server_details["url"]
    server_port = web_server_details["port"]
    driver_url = "http://"+server_ip+":"+server_port+"/wake-up/"
    reply = requests.post(url = driver_url, json = {"function_type": str(function_type), "unique_id": str(unique_id), "activation_id": str(activation_id)})

    # print(reply.json())
    
def get_worker_node_ip(web_server_details):
    server_ip = web_server_details["url"]
    server_port = web_server_details["port"]
    driver_url = "http://"+server_ip+":"+server_port+"/get_worker_node_ip/"
    reply = requests.post(url = driver_url)
    reply = reply.json()
    return reply["worker_node_ip"]
    
def main():
    params = json.loads(sys.argv[1])
    connection_details = params.get("config_file", "NULL")

    unique_id = str(params.get("unique_id"))
    activation_id = params.get("activation_id", "None")
    # print(connection_details)

    # ----------------------------------Remote Storage Details-------------------------------------
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
    found = remote_client.bucket_exists(bucket_name = remote_bucket_name)
    if not found:
        remote_client.make_bucket(bucket_name = remote_bucket_name)
    else:
        print("remote bucket '",remote_bucket_name,"' already exists")
    # ----------------------------------Remote Storage Details-------------------------------------

    
    # ----------------------------------Temporary Storage Details-------------------------------------
    
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
    
    startup_nodes = None
    redis_host = None
    redis_conn = None
    if connection_details["storage"]["type"] == "redis":
        redis_host = connection_details["storage"]["redis"]
        redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
    else:
        startup_nodes = connection_details["storage"]["redis_cluster_nodes"]
        redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    # redis_conn = rediscluster.RedisCluster(startup_nodes=startup_nodes)

    if intermediate_storage["location"] == "local":
        local_mapping_table_flag = redis_conn.get("local_mapping_table")
        if local_mapping_table_flag != None:
            local_mapping_table = pickle.loads(local_mapping_table_flag)
            print(unique_id)
            unique_id = local_mapping_table[str(temporary_endpoint_ip)]
            print(unique_id)
    
    
    
    reducer_mapping_table_key = params.get("reducer_mapping_table_key", "reducer_mapping_table_key_None")
    reducer_mapping_table = pickle.loads(redis_conn.get(reducer_mapping_table_key))

    
    print("Unique Id", unique_id)
    # print(reducer_mapping_table[unique_id])
    for reducer_id in reducer_mapping_table[unique_id]:
        
        # token_data = redis_conn.get(params)
        reducer_input_param = "reducer-input-"+activation_id+"-"+reducer_id
        # print(reducer_input_param)
        # token_data = pickle.loads(redis_conn.get(reducer_input_param))
        token_data = None
        # if intermediate_storage["location"] == "local":
        token_data = pickle.loads(temporary_client.get_object(bucket_name = temporary_bucket_name, object_name = reducer_input_param).data)
        # else:
        #     token_data = pickle.loads(remote_client.get_object(bucket_name = remote_bucket_name, object_name = reducer_input_param).data)
        
        # # print(token_data)
        return_dict={}
        for token in token_data:
            if token in return_dict:
                return_dict[token]+=1
            else:
                return_dict[token]=1
        del(token_data)
        pickled_object = pickle.dumps(return_dict)
        
        filename = "aggregator-input-"+activation_id+"-"+reducer_id
        # print(filename)
        
        # redis_conn.set(filename, pickled_object)
        # remote_client.put_object(bucket_name = remote_bucket_name, object_name = filename, data=io.BytesIO(pickled_object), length=len(pickled_object))
        temporary_client.put_object(bucket_name = temporary_bucket_name, object_name = filename, data=io.BytesIO(pickled_object), length=len(pickled_object))
        del(return_dict)
        del(pickled_object)
    reducer_activation_id = os.getenv("__OW_ACTIVATION_ID")
    # print(return_dict)
    # my_dict =

    print("Reducer function -", reducer_activation_id)
    print("Driver function -", activation_id)
    wake_up("reducer", unique_id, activation_id, connection_details["web_server"])
    print(json.dumps( {"reducer-output-"+unique_id: str(reducer_mapping_table[unique_id])
                        ,"activation_id": str(reducer_activation_id)
                        }))
    
    return ( {"reducer-output-"+unique_id: str(reducer_mapping_table[unique_id])
                        ,"activation_id": str(reducer_activation_id)
                        })

if __name__ == "__main__":
    main()
