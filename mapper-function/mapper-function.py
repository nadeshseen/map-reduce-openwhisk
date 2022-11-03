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
    mapper_mapping_table_key = params.get("mapper_mapping_table_key")
    mapper_mapping_table = pickle.loads(r.get(mapper_mapping_table_key))
    print("Unique Id",unique_id)
    print(mapper_mapping_table[unique_id])
    for mapper_id in mapper_mapping_table[unique_id]:
        


        mapper_input_param = "mapper-input-"+driver_activation_id+"-"+mapper_id

        print(mapper_input_param)

        input_data = pickle.loads(r.get(mapper_input_param))
        # input_data = input_data.decode("utf-8")
        tokenize_data = input_data.split()
        for i in range(len(tokenize_data)):
            tokenize_data[i] = tokenize_data[i].replace(",", "")
            tokenize_data[i] = tokenize_data[i].replace(".", "")
        pickled_object = pickle.dumps(tokenize_data)
        reducer_instance = "reducer-input-"+driver_activation_id+"-"+mapper_id

        r.set(reducer_instance, pickled_object)

    mapper_activation_id = os.getenv("__OW_ACTIVATION_ID")

    print("Mapper function -", mapper_activation_id)
    print("Driver function -", driver_activation_id)
    print(json.dumps( { 
                        "mapper-output-"+unique_id: str(mapper_mapping_table[unique_id])
                        ,"activation_id": str(mapper_activation_id)
                        }))
    # return my_dict
# main("nadesh")
if __name__ == "__main__":
    main()
