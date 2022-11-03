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
    mapper_split_percentage_list_key=""
    reducer_split_percentage_list_key=""
    split_file=""
    if len(sys.argv) == 2:
        params = json.loads(sys.argv[1])
        filename = params["filename"]

        if "driver_activation_id" in params.keys():
            driver_activation_id = params["driver_activation_id"]

        if "mapper_split_percentage_list_key" in params.keys():
            mapper_split_percentage_list_key = params["mapper_split_percentage_list_key"]
        if "reducer_split_percentage_list_key" in params.keys():
            reducer_split_percentage_list_key = params["reducer_split_percentage_list_key"]
        
        if "split_file" in params.keys():
            split_file = params["split_file"]
    
    mapper_split_percentage_list = pickle.loads(r.get(mapper_split_percentage_list_key))
    reducer_split_percentage_list = pickle.loads(r.get(reducer_split_percentage_list_key))
    print(split_file, mapper_split_percentage_list_key, mapper_split_percentage_list)
    print(split_file, reducer_split_percentage_list_key, reducer_split_percentage_list)
    data = pickle.loads(r.get(filename))
    lines = data.split("\n")




    # if split_file is true

    # number_of_lines = len(lines)
    # row_number = 0
    # start_index=0
    # last_index=0
    # count=1
    # list=[]
    # for index in range(len(split_percentage_list)-1):
    #     #merging lines
    #     row_number += math.ceil(int(split_percentage_list[index])*number_of_lines/100)
    #     last_index = row_number
    #     string_combined=""
    #     for line in lines[start_index:last_index]:
    #         string_combined+=line+" "
        
    #     #storing in redis
    #     list.append(count)
    #     filename = "mapper-input-"+str(driver_activation_id)+"-"+str(count)
    #     pickled_object = pickle.dumps(string_combined)
    #     r.set(filename, pickled_object)
    #     count+=1


    #     start_index = last_index

    
    # #merging lines        
    # # print(start_index, number_of_lines-1)
    # string_combined=""
    # for line in lines[start_index:number_of_lines]:
    #     string_combined+=line+" "

    # list.append(count)
    # filename = "mapper-input-"+str(driver_activation_id)+"-"+str(count)
    # pickled_object = pickle.dumps(string_combined)
    # r.set(filename, pickled_object)

    data_list = []


    # if split_file is false (creating mapper mapping table)
    mapper_mapping_table = {}

    number_of_lines = len(lines)
    row_number = 0
    start_index=0
    last_index=0
    line_count=1
    mapper_count=1
    mapper_list=[]

    for index in range(len(mapper_split_percentage_list)-1):
        row_number += math.ceil(int(mapper_split_percentage_list[index])*number_of_lines/100)
        last_index = row_number

        list_of_mapper_filenames = []
        for line in lines[start_index:last_index]:
            data_list.append(str(line_count))
            mapper_filename = "mapper-input-"+str(driver_activation_id)+"-"+str(line_count)
            list_of_mapper_filenames.append(str(line_count))
            pickled_object = pickle.dumps(line)
            r.set(mapper_filename, pickled_object)
            line_count+=1
    
        mapper_list.append(mapper_count)
        mapper_mapping_table[str(mapper_count)]=(list_of_mapper_filenames)
        mapper_count+=1

        start_index = last_index

    
    #merging lines        
    # print(start_index, number_of_lines-1)

    list_of_mapper_filenames = []
    for line in lines[start_index:number_of_lines]:

        data_list.append(str(line_count))
        mapper_filename = "mapper-input-"+str(driver_activation_id)+"-"+str(line_count)
        list_of_mapper_filenames.append(str(line_count))
        pickled_object = pickle.dumps(line)
        r.set(mapper_filename, pickled_object)
        line_count+=1


    mapper_list.append(mapper_count)
    mapper_mapping_table[str(mapper_count)]=(list_of_mapper_filenames)

    print(mapper_mapping_table)

    #Storing mapping table in redis

    r.set("mapper_mapping_table_key_"+str(driver_activation_id), pickle.dumps(mapper_mapping_table))




    # if split_file is false (creating reducer mapping table)
    reducer_mapping_table = {}

    number_of_lines = len(lines)
    row_number = 0
    start_index=0
    last_index=0
    line_count=1
    reducer_count=1
    reducer_list=[]
    for index in range(len(reducer_split_percentage_list)-1):
        row_number += math.ceil(int(reducer_split_percentage_list[index])*number_of_lines/100)
        last_index = row_number

        list_of_reducer_filenames = []
        for line in lines[start_index:last_index]:
            reducer_filename = "reducer-input-"+str(driver_activation_id)+"-"+str(line_count)
            list_of_reducer_filenames.append(str(line_count))
            pickled_object = pickle.dumps(line)
            r.set(reducer_filename, pickled_object)
            line_count+=1
    
        reducer_list.append(reducer_count)
        reducer_mapping_table[str(reducer_count)]=(list_of_reducer_filenames)
        reducer_count+=1

        start_index = last_index

    
    #merging lines        
    # print(start_index, number_of_lines-1)

    list_of_reducer_filenames = []
    for line in lines[start_index:number_of_lines]:
        reducer_filename = "reducer-input-"+str(driver_activation_id)+"-"+str(line_count)
        list_of_reducer_filenames.append(str(line_count))
        pickled_object = pickle.dumps(line)
        r.set(reducer_filename, pickled_object)
        line_count+=1


    reducer_list.append(reducer_count)
    reducer_mapping_table[str(reducer_count)]=(list_of_reducer_filenames)

    print(reducer_mapping_table)

    #Storing mapping table in redis

    r.set("reducer_mapping_table_key_"+str(driver_activation_id), pickle.dumps(reducer_mapping_table))

    # count+=1
    # # print(string_combined)
    # count=1
    # # print(lines)
    # # temp=""
    # list = []
    # for line in lines:
    #     list.append(count)
    #     filename = "mapper-input-"+str(driver_activation_id)+"-"+str(count)
    #     pickled_object = pickle.dumps(line)
    #     r.set(filename, pickled_object)
    #     count+=1

    mapper_list_key = "mapper_list-"+str(driver_activation_id)
    reducer_list_key = "reducer_list-"+str(driver_activation_id)
    data_list_key = "data_list-"+str(driver_activation_id)
    r.set(mapper_list_key, pickle.dumps(mapper_list))
    r.set(reducer_list_key, pickle.dumps(reducer_list))
    r.set(data_list_key, pickle.dumps(data_list))
    
    print("Splitdata function -", activation_id)
    print("Driver function -", driver_activation_id)
    print(json.dumps({  "splitdata": str(mapper_count)
                        ,"activation_id": str(activation_id)
                        ,"mapper_list_key":str(mapper_list_key)
                        ,"reducer_list_key":str(reducer_list_key)
                        ,"data_list_key":str(data_list_key)
    }))

if __name__ == "__main__":
    main()
