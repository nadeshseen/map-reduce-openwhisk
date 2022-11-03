#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
def main():
    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    
    params = json.loads(sys.argv[1])
    driver_activation_id = params["driver_activation_id"]
    
    data_list = pickle.loads(r.get(params["data_list"]))
    print(data_list)
    overall_word_count = {}
    temp_str = ""
    for unique_id in data_list:
        # print(key)
        # temp_str+= " --- "+str(unique_id) + "\n     "
        input_path = "aggregator-input-"+driver_activation_id+"-"+str(unique_id)
        print(input_path)
        # print(unique_id)
        individual_word_count = pickle.loads(r.get(input_path))
        
        for key, value in individual_word_count.items():
            if key in overall_word_count.keys():
                overall_word_count[key]+=value
            else:
                overall_word_count[key]=value
    
    pickled_object = pickle.dumps(overall_word_count)
    filename = "final-output-"+driver_activation_id
    r.set(filename, pickled_object)
    # print(filename)

    aggregator_activation_id = os.getenv("__OW_ACTIVATION_ID")
    print("Aggregator function -", aggregator_activation_id)
    print("Driver function -", driver_activation_id)
    print(json.dumps( {
        "aggregator-output": str(overall_word_count)
        ,"output_filename": str(filename)
        }))

if __name__ == "__main__":
    main()
