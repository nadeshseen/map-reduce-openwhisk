#!/usr/bin/env python3
import os
import redis
import json
import pickle
import sys
def main():
    
    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    
    params = json.loads(sys.argv[1])
    unique_id = params.get("unique_id")
    driver_activation_id = params.get("driver_activation_id")
    reducer_mapping_table_key = params.get("reducer_mapping_table_key")
    reducer_mapping_table = pickle.loads(r.get(reducer_mapping_table_key))
    print("Unique Id", unique_id)
    print(reducer_mapping_table[unique_id])
    for reducer_id in reducer_mapping_table[unique_id]:
        
        # token_data = r.get(params)
        reducer_input_param = "reducer-input-"+driver_activation_id+"-"+reducer_id
        print(reducer_input_param)
        token_data = pickle.loads(r.get(reducer_input_param))
        # # print(token_data)
        return_dict={}
        for token in token_data:
            if token in return_dict:
                return_dict[token]+=1
            else:
                return_dict[token]=1

        pickled_object = pickle.dumps(return_dict)
        filename = "aggregator-input-"+driver_activation_id+"-"+reducer_id
        print(filename)
        r.set(filename, pickled_object)
    reducer_activation_id = os.getenv("__OW_ACTIVATION_ID")
    # print(return_dict)
    # my_dict =

    print("Reducer function -", reducer_activation_id)
    print("Driver function -", driver_activation_id)
    print(json.dumps( {"reducer-output-"+unique_id: str(reducer_mapping_table[unique_id])
                        ,"activation_id": str(reducer_activation_id)
                        }))

if __name__ == "__main__":
    main()
