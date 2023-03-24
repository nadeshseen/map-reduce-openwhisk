#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
import time
import requests
import rediscluster

import psutil

def wake_up(function_type, unique_id, activation_id, web_server_details):
    server_ip = web_server_details["url"]
    server_port = web_server_details["port"]
    driver_url = "http://"+server_ip+":"+server_port+"/wake-up/"
    # print(driver_url)
    reply = requests.post(url = driver_url, json = {"function_type": str(function_type), "unique_id": str(unique_id), "activation_id": str(activation_id)})

    # print(reply.json())
    
def mapper_logic():
    pass

def main():

    # print("Ram used before loading", psutil.virtual_memory()[2])
    # print('RAM Used (MB):', psutil.virtual_memory()[3]/1024/1024)
    # total_start = time.time()
    params = json.loads(sys.argv[1])


    connection_details = params.get("config_file", "NULL")
    print(connection_details)
    startup_nodes = connection_details["storage"]["startup_nodes"]
    # startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    rediscluster_conn = rediscluster.RedisCluster(startup_nodes=startup_nodes)

    # tmp_var = params.get("storage", "NULL")
    # tmp_var = tmp_var["startup_nodes"]
    # print(tmp_var)


    unique_id = params.get("unique_id")
    # start = time.time()
    activation_id = params.get("activation_id")
    mapper_mapping_table_key = params.get("mapper_mapping_table_key")
    del(params)
    # end = time.time()
    # metadata_access_time = end - start
    # rediscluster_conn.set("mapper-metadata-time-"+unique_id, pickle.dumps(str(metadata_access_time)))

    mapper_mapping_table = pickle.loads(rediscluster_conn.get(mapper_mapping_table_key))
    print("Unique Id",unique_id)
    function_type = "mapper"
    log_obj = function_type+"-"+unique_id+"-"+activation_id+"-logs"

    log_data = pickle.loads(rediscluster_conn.get(log_obj))
    
    
    log_data+=log_obj+"\n"
    rediscluster_conn.set(log_obj, pickle.dumps(log_data))
    # print(mapper_mapping_table[unique_id])
    # start = time.time()
    for data_id in mapper_mapping_table[unique_id]:
        # print("Ram used before loading", psutil.virtual_memory()[2])
        # print('RAM Used (MB):', psutil.virtual_memory()[3]/1024/1024)
        mapper_input_param = "mapper-input-"+activation_id+"-"+data_id
        log_data+=mapper_input_param+"\n"
        rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        input_data = pickle.loads(rediscluster_conn.get(mapper_input_param))
        log_data+="data loaded in memory"+"\n"
        rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        





        
        ### Mapper Logic Starts ###
        tokenize_data = input_data.split()
        del(input_data)
        for i in range(len(tokenize_data)):
            tokenize_data[i] = tokenize_data[i].replace(",", "")
            tokenize_data[i] = tokenize_data[i].replace(".", "")

        ### Mapper Logic Ends ###
        log_data+="mapper logic ended"+"\n"
        rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        






        # tokenize_data = 
        reducer_instance = "reducer-input-"+activation_id+"-"+data_id

        pickled_tokenize_data = pickle.dumps(tokenize_data)
        del(tokenize_data)
        log_data+="data pickled"+"\n"
        rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        rediscluster_conn.set(reducer_instance, pickled_tokenize_data)
        log_data+="data stored in redis"+"\n"
        rediscluster_conn.set(log_obj, pickle.dumps(log_data))
        del(pickled_tokenize_data)
        
    # end = time.time()
    # data_access_time = end - start
    # rediscluster_conn.set("mapper-data-time-"+unique_id, pickle.dumps(str(data_access_time)))

    mapper_activation_id = os.getenv("__OW_ACTIVATION_ID")

    print("Mapper function -", mapper_activation_id)
    print("Driver function -", activation_id)
    # total_end = time.time()
    # total_execution_time = total_end - total_start
    # rediscluster_conn.set("mapper-total-time-"+unique_id, pickle.dumps(str(total_execution_time)))
    wake_up(function_type, unique_id, activation_id, connection_details["web_server"])
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
