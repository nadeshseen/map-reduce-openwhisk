#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
import requests
import rediscluster

def wake_up(function_type, unique_id, activation_id, web_server_details):
    server_ip = web_server_details["url"]
    server_port = web_server_details["port"]
    driver_url = "http://"+server_ip+":"+server_port+"/wake-up/"
    reply = requests.post(url = driver_url, json = {"function_type": str(function_type), "unique_id": str(unique_id), "activation_id": str(activation_id)})

    # print(reply.json())
    

def main():
    params = json.loads(sys.argv[1])
    connection_details = params.get("config_file", "NULL")
    print(connection_details)
    startup_nodes = connection_details["storage"]["startup_nodes"]
    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    
    # web_server_details = params.get("web_server", "NULL")
    
    
    unique_id = str(params.get("unique_id"))
    activation_id = params.get("activation_id", "None")
    reducer_mapping_table_key = params.get("reducer_mapping_table_key", "reducer_mapping_table_key_None")
    reducer_mapping_table = pickle.loads(r.get(reducer_mapping_table_key))
    print("Unique Id", unique_id)
    # print(reducer_mapping_table[unique_id])
    for reducer_id in reducer_mapping_table[unique_id]:
        
        # token_data = r.get(params)
        reducer_input_param = "reducer-input-"+activation_id+"-"+reducer_id
        # print(reducer_input_param)
        token_data = pickle.loads(r.get(reducer_input_param))
        
        # # print(token_data)
        return_dict={}
        for token in token_data:
            if token in return_dict:
                return_dict[token]+=1
            else:
                return_dict[token]=1
        del(token_data)
        pickled_object = pickle.dumps(return_dict)
        del(return_dict)
        filename = "aggregator-input-"+activation_id+"-"+reducer_id
        # print(filename)
        
        r.set(filename, pickled_object)
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
