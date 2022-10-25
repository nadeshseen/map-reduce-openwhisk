#!/usr/bin/env python3

import redis
import json
import pickle
import sys
def main():
    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    
    params = json.loads(sys.argv[1])
    splitdata_activation_id = params["activation_id"]
    list_of_ids = params["list_of_ids"]
    overall_word_count = {}
    temp_str = ""
    for unique_id in list_of_ids:
        # print(key)
        # temp_str+= " --- "+str(unique_id) + "\n     "
        input_path = "aggregator-input-"+splitdata_activation_id+"-"+str(unique_id)
        
        # print(unique_id)
        individual_word_count = pickle.loads(r.get(input_path))
        
        for key, value in individual_word_count.items():
            if key in overall_word_count.keys():
                overall_word_count[key]+=value
            else:
                overall_word_count[key]=value
    
    pickled_object = pickle.dumps(overall_word_count)
    filename = "final-output-"+splitdata_activation_id
    r.set(filename, pickled_object)
    print(filename)
    print(json.dumps( {
        "aggregator-output": str(overall_word_count)
        ,"output_filename": str(filename)
        }))

if __name__ == "__main__":
    main()
