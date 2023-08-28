#!/usr/bin/env python3
import os, json, pickle, sys, io
import redis as redis_lib
# import rediscluster as redis_lib
from minio import Minio
import requests


def wake_up(function_type, unique_id, activation_id, web_server_details):
    server_ip = web_server_details["url"]
    server_port = web_server_details["port"]
    driver_url = "http://"+server_ip+":"+server_port+"/wake-up/"
    reply = requests.post(url = driver_url, json = {"function_type": str(function_type), "unique_id": str(unique_id), "activation_id": str(activation_id)})


def main():
    params = json.loads(sys.argv[1])
    connection_details = params.get("config_file", "NULL")
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
    startup_nodes = None
    redis_host = None
    redis_conn = None
    if connection_details["storage"]["type"] == "redis":
        redis_host = connection_details["storage"]["redis"]
        redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
    else:
        startup_nodes = connection_details["storage"]["redis_cluster_nodes"]
        redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    activation_id = str(params["activation_id"])
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
    data_list = pickle.loads(redis_conn.get(params["data_list_key"]))
    # print(data_list)
    overall_word_count = {}
    for unique_id in data_list:
        # print(key)
        # temp_str+= " --- "+str(unique_id) + "\n     "
        input_path = "aggregator-input-"+str(activation_id)+"-"+str(unique_id)
        # print(input_path)
        # print(unique_id)
        # individual_word_count = pickle.loads(redis_conn.get(input_path))
        # individual_word_count = pickle.loads(remote_client.get_object(bucket_name = remote_bucket_name, object_name = input_path).data)
        individual_word_count = pickle.loads(temporary_client.get_object(bucket_name = temporary_bucket_name, object_name = input_path).data)
        
        for key, value in individual_word_count.items():
            if key in overall_word_count.keys():
                overall_word_count[key]+=value
            else:
                overall_word_count[key]=value
        del(individual_word_count)
    pickled_object = pickle.dumps(overall_word_count)
    
    length_of_overall_word_count = len(overall_word_count)
    # del(overall_word_count)
    filename = "final-output-"+activation_id

    # redis_conn.set(filename, pickled_object)
    remote_client.put_object(bucket_name = remote_bucket_name, object_name = filename, data=io.BytesIO(pickled_object), length=len(pickled_object))
    del(pickled_object)
    print(filename)
    wake_up("aggregator", "1", activation_id, connection_details["web_server"])
    aggregator_activation_id = os.getenv("__OW_ACTIVATION_ID")
    print("Aggregator function -", aggregator_activation_id)
    print("Driver function -", activation_id)
    print(json.dumps( {
        "aggregator-output": str(length_of_overall_word_count)
        ,"output_filename": str(filename)
        }))    
    return( {
        "aggregator-output": str(length_of_overall_word_count)
        ,"output_filename": str(filename)
        })

if __name__ == "__main__":
    main()
