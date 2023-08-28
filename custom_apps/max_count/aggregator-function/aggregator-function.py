#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
import rediscluster
from minio import Minio
import io

def main():
    params = json.loads(sys.argv[1])
    connection_details = params.get("config_file", "NULL")
    data_store = connection_details["data_store"]
    client = Minio(
        endpoint = data_store["endpoint"],
        access_key=data_store["access_key"],
        secret_key=data_store["secret_key"],
        secure=False,
    )
    # print(connection_details)
    my_bucket_name = data_store["bucket_name"]
    startup_nodes = connection_details["storage"]["startup_nodes"]
    # startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    activation_id = str(params["activation_id"])
    
    data_list = pickle.loads(r.get(params["data_list_key"]))
    # print(data_list)
    overall_word_count = {}
    for unique_id in data_list:
        # print(key)
        # temp_str+= " --- "+str(unique_id) + "\n     "
        input_path = "aggregator-input-"+str(activation_id)+"-"+str(unique_id)
        # print(input_path)
        # print(unique_id)
        # individual_word_count = pickle.loads(r.get(input_path))
        individual_word_count = pickle.loads(client.get_object(bucket_name = my_bucket_name, object_name = input_path).data)
        
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
    # r.set(filename, pickled_object)
    client.put_object(bucket_name = my_bucket_name, object_name = filename, data=io.BytesIO(pickled_object), length=len(pickled_object))
    del(pickled_object)
    # print(filename)

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
