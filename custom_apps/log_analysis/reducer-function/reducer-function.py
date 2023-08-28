#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
import requests
import rediscluster
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
    # print(connection_details)
    data_store = connection_details["data_store"]
    startup_nodes = connection_details["storage"]["startup_nodes"]
    # startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    unique_id = str(params.get("unique_id"))
    activation_id = params.get("activation_id", "None")
    
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


    if intermediate_storage["location"] == "local":
        local_endpoint_ip = get_worker_node_ip(connection_details["web_server"])
        local_mapping_table_flag = r.get("local_mapping_table")
        if local_mapping_table_flag != None:
            local_mapping_table = pickle.loads(local_mapping_table_flag)
            print(unique_id)
            unique_id = local_mapping_table[str(local_endpoint_ip)]
            print(unique_id)
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

    # web_server_details = params.get("web_server", "NULL")
    
    
    
    reducer_mapping_table_key = params.get("reducer_mapping_table_key", "reducer_mapping_table_key_None")
    reducer_mapping_table = pickle.loads(r.get(reducer_mapping_table_key))
    print("Unique Id", unique_id)
    # print(reducer_mapping_table[unique_id])
    for reducer_id in reducer_mapping_table[unique_id]:
        
        # token_data = r.get(params)
        reducer_input_param = "reducer-input-"+activation_id+"-"+reducer_id
        # print(reducer_input_param)
        # token_data = pickle.loads(r.get(reducer_input_param))
        token_data = None
        if intermediate_storage["location"] == "local":
            token_data = pickle.loads(local_client.get_object(bucket_name = local_bucket_name, object_name = reducer_input_param).data)
        else:
            token_data = pickle.loads(remote_client.get_object(bucket_name = remote_bucket_name, object_name = reducer_input_param).data)
        
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
        
        # r.set(filename, pickled_object)
        remote_client.put_object(bucket_name = remote_bucket_name, object_name = filename, data=io.BytesIO(pickled_object), length=len(pickled_object))
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
