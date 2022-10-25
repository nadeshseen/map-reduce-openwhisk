#!/usr/bin/env python3
import redis
import os
import json
import pickle
import sys
import math
def main():
    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    activation_id = os.getenv("__OW_ACTIVATION_ID")
    filename = "input1234.txt"
    split_percentage_list_key=""
    if len(sys.argv) == 2:
        params = json.loads(sys.argv[1])
        filename = params["filename"]

        if "driver_activation_id" in params.keys():
            driver_activation_id = params["driver_activation_id"]

        if "split_percentage_list_key" in params.keys():
            split_percentage_list_key = params["split_percentage_list_key"]
    
    split_percentage_list = pickle.loads(r.get(split_percentage_list_key))
    print(split_percentage_list_key, split_percentage_list)
    data = pickle.loads(r.get(filename))
    lines = data.split("\n")
    number_of_lines = len(lines)
    row_number = 0
    start_index=0
    last_index=0
    
    for index in range(len(split_percentage_list)-1):
        row_number += math.ceil(int(split_percentage_list[index])*number_of_lines/100)
        # print(row_number)
        last_index = row_number

        print(start_index, last_index-1)
        # print(lines[start_index:last_index])
        string_combined=""
        for line in lines[start_index:last_index]:
            string_combined+=line+" "
        start_index = last_index
        # print(string_combined)
        
    print(start_index, number_of_lines-1)
    string_combined=""
    for line in lines[start_index:number_of_lines]:
        string_combined+=line+" "
    
    # print(string_combined)
    count=1
    # print(lines)
    # temp=""
    list = []
    for line in lines:
        list.append(count)
        filename = "mapper-input-"+str(driver_activation_id)+"-"+str(count)
        # print(filename, line)
        # temp+=filename+"----"
        pickled_object = pickle.dumps(line)
        r.set(filename, pickled_object)
        count+=1

    list_of_ids = "split-data-id-list-"+str(driver_activation_id)
    r.set(list_of_ids, pickle.dumps(list))
    print("Splitdata function -", activation_id)
    print("Driver function -", driver_activation_id)
    print(json.dumps({  "splitdata": str(len(lines))
                        ,"activation_id": str(activation_id)
                        ,"list_of_ids":str(list_of_ids)
    }))

if __name__ == "__main__":
    main()
