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
    splitdata_activation_id = params.get("activation_id")
    
    mapper_input_param = "mapper-input-"+splitdata_activation_id+"-"+unique_id


    input_data = pickle.loads(r.get(mapper_input_param))
    # input_data = input_data.decode("utf-8")
    tokenize_data = input_data.split()
    for i in range(len(tokenize_data)):
        tokenize_data[i] = tokenize_data[i].replace(",", "")

        tokenize_data[i] = tokenize_data[i].replace(".", "")
    pickled_object = pickle.dumps(tokenize_data)
    reducer_instance = "reducer-input-"+splitdata_activation_id+"-"+unique_id

    r.set(reducer_instance, pickled_object)

    mapper_activation_id = os.getenv("__OW_ACTIVATION_ID")
    print(json.dumps( { "mapper-output-"+unique_id: str(tokenize_data)
                        ,"activation_id": str(mapper_activation_id)
                        }))
    # return my_dict
# main("nadesh")
if __name__ == "__main__":
    main()
