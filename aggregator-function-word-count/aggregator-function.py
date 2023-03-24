#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
import rediscluster

def main():

    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    
    params = json.loads(sys.argv[1])
    activation_id = str(params["activation_id"])
    
    data_list = pickle.loads(r.get(params["data_list_key"]))
    # print(data_list)
    overall_word_count = {}
    temp_str = ""
    for unique_id in data_list:
        # print(key)
        # temp_str+= " --- "+str(unique_id) + "\n     "
        input_path = "aggregator-input-"+str(activation_id)+"-"+str(unique_id)
        # print(input_path)
        # print(unique_id)
        individual_word_count = pickle.loads(r.get(input_path))
        
        for key, value in individual_word_count.items():
            if key in overall_word_count.keys():
                overall_word_count[key]+=value
            else:
                overall_word_count[key]=value
    
    pickled_object = pickle.dumps(overall_word_count)
    filename = "final-output-"+activation_id
    r.set(filename, pickled_object)
    # print(filename)

    aggregator_activation_id = os.getenv("__OW_ACTIVATION_ID")
    print("Aggregator function -", aggregator_activation_id)
    print("Driver function -", activation_id)
    print(json.dumps( {
        "aggregator-output": str(len(overall_word_count))
        ,"output_filename": str(filename)
        }))    
    return( {
        "aggregator-output": str(len(overall_word_count))
        ,"output_filename": str(filename)
        })

if __name__ == "__main__":
    main()
