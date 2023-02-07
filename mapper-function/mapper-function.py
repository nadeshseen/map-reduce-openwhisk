#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
import time
import requests
import rediscluster

def wake_up(function_type, unique_id, activation_id):
    server_ip = "10.129.28.219"
    server_port = "5001"
    driver_url = "http://"+server_ip+":"+server_port+"/wake-up/"
    print(driver_url)
    reply = requests.post(url = driver_url, json = {"function_type": str(function_type), "unique_id": str(unique_id), "activation_id": str(activation_id)})

    print(reply.json())
    


def main():
    total_start = time.time()
    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)

    params = json.loads(sys.argv[1])
    unique_id = params.get("unique_id")
    start = time.time()
    activation_id = params.get("activation_id")
    mapper_mapping_table_key = params.get("mapper_mapping_table_key")
    end = time.time()
    metadata_access_time = end - start
    r.set("mapper-metadata-time-"+unique_id, pickle.dumps(str(metadata_access_time)))

    mapper_mapping_table = pickle.loads(r.get(mapper_mapping_table_key))
    print("Unique Id",unique_id)
    # print(mapper_mapping_table[unique_id])
    start = time.time()
    for data_id in mapper_mapping_table[unique_id]:
        mapper_input_param = "mapper-input-"+activation_id+"-"+data_id
        input_data = pickle.loads(r.get(mapper_input_param))
        
        tokenize_data = input_data.split()
        for i in range(len(tokenize_data)):
            tokenize_data[i] = tokenize_data[i].replace(",", "")
            tokenize_data[i] = tokenize_data[i].replace(".", "")
        pickled_object = pickle.dumps(tokenize_data)
        reducer_instance = "reducer-input-"+activation_id+"-"+data_id
        r.set(reducer_instance, pickled_object)
    end = time.time()
    data_access_time = end - start
    r.set("mapper-data-time-"+unique_id, pickle.dumps(str(data_access_time)))

    mapper_activation_id = os.getenv("__OW_ACTIVATION_ID")

    print("Mapper function -", mapper_activation_id)
    print("Driver function -", activation_id)
    total_end = time.time()
    total_execution_time = total_end - total_start
    r.set("mapper-total-time-"+unique_id, pickle.dumps(str(total_execution_time)))
    wake_up("mapper", unique_id, activation_id)
    print(json.dumps( { 
                        "mapper-output-"+unique_id: str(mapper_mapping_table[unique_id])
                        ,"activation_id": str(mapper_activation_id)
                        }))
    
    return( { 
                    "mapper-output-"+unique_id: str(mapper_mapping_table[unique_id])
                    ,"activation_id": str(mapper_activation_id)
                    })
    # return my_dict
# main("nadesh")
if __name__ == "__main__":
    main()
