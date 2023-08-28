#!/usr/bin/env python3

import sys, requests, subprocess, threading
import pickle, json, os, time, uuid
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from flask import Flask, request
import math, textwrap, time, psutil, io
import redis as redis_lib
from minio import Minio

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

app = Flask(__name__)


# event_obj_list = {}
sema_list = {}
startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]

worker_node_ips = {"smareo-worker1":"10.129.26.126", "smareo-worker2":"10.129.26.125", "smareo-worker3":"10.129.27.189", "smareo-worker4":"10.129.27.55"}

coordinator_ip=""
coordinator_port=""

response = {}
mapper_response = {}
reducer_response = {}
@app.route('/wake-up/', methods=['GET', 'POST'])
def wake_up():
    request_values = request.json
    function_type = request_values["function_type"]
    unique_id = request_values["unique_id"]
    activation_id = request_values["activation_id"]
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    # print(request_values)
    # print(event_obj_list)
    # event_obj_list[unique_id].set()
    response[wake_up_obj] = "Woken"
    if function_type=="mapper":
        mapper_response[wake_up_obj]="Done"
    if function_type=="reducer":
        reducer_response[wake_up_obj]="Done"
    sema_list[wake_up_obj].release()
    print(wake_up_obj)
    output_json = json.dumps(response)
    return output_json

@app.route('/get_worker_node_ip/', methods=['GET', 'POST'])
def get_worker_node_ip():
    # request_values = request.json
    # hostname = request_values["hostname"]
    # print(hostname)
    # reply = subprocess.check_output(["kubectl get pods -n openwhisk -o wide"], shell=True)
    # reply = reply.decode('utf-8')
    # reply = reply.split("\n")
    response["worker_node_ip"] = str(request.remote_addr)
    # for row in reply:
    #     if hostname in row:
    #         print(row)
    #         row = row.split()
    #         print(row[6])
    #         print(worker_node_ips[row[6]])
    # print()
    # print(reply)

    

    return json.dumps(response)


def mapper_function(unique_id, activation_id, mapper_url, config_file):
    function_type="mapper"
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    mapper_response[wake_up_obj] = "Initiated"
    
    reply = requests.post(url = mapper_url,
        json={"unique_id":str(unique_id), "activation_id":str(activation_id)
        , "mapper_mapping_table_key": "mapper_mapping_table_key_"+str(activation_id)
        , "config_file":config_file}
        , verify=False)
    # event_obj_list[unique_id].wait()
    
    mapper_response[wake_up_obj] = "Executing"
    # print(wake_up_obj," waiting")
    
    sema_list[wake_up_obj].acquire()
    mapper_response[wake_up_obj] = "Done"
    # print(wake_up_obj," passed")
    # print(reply)
    # print(reply.status_code)
    print(reply.text)
    # print(reply.reason)
    print("Mapper", unique_id)
    print(json.dumps(mapper_response, indent = 1))


def reducer_function(unique_id, activation_id, reducer_url, config_file):
    function_type="reducer"
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    reducer_response[wake_up_obj] = "Initiated"
    reply = requests.post(url = reducer_url,
        json={"unique_id":str(unique_id), "activation_id":str(activation_id)
        , "reducer_mapping_table_key": "reducer_mapping_table_key_"+str(activation_id)
        , "config_file": config_file}
        ,verify=False)
    # event_obj_list[unique_id].wait()
    reducer_response[wake_up_obj] = "Executing"
    # print(wake_up_obj," waiting")
    sema_list[wake_up_obj].acquire()

    reducer_response[wake_up_obj] = "Done"
    # print(wake_up_obj," passed")
    # print(reply)
    # print(reply.status_code)
    # print(reply.text)
    # print(reply.reason)
    print("Reducer", unique_id)
    print(json.dumps(reducer_response, indent = 1))
    # reply = reply.json()
    # print(reply)


def start_splitdata_instance(parameter_file, activation_id):
    # print("hello")
    print(activation_id)
    reply = splitdata(parameter_file, activation_id)
    # reply = reply.json()
    
    startup_nodes = None
    redis_conn = None
    connection_details = parameter_file
    if connection_details["storage"]["type"] == "redis":
        redis_host = connection_details["storage"]["redis"]
        redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
    else:
        startup_nodes = connection_details["storage"]["redis_cluster_nodes"]
        redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    return pickle.loads(redis_conn.get(reply["mapper_list_key"])), pickle.loads(redis_conn.get(reply["reducer_list_key"])), reply["data_list_key"], reply["extra_data"]



def start_mapper_instances(list_of_ids, activation_id, mapper_url, config_file):
    mapper_threads = []
    function_type="mapper"    
    for unique_id in list_of_ids:  
        
        wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
        # event_obj_list[id] = threading.Event()
        sema_list[wake_up_obj] = threading.Semaphore(0)
        # event_obj_list.append(threading.Event())
    # print("Mapper Functions started")
    for id in list_of_ids:
        mapper_threads.append(threading.Thread(target=mapper_function, args=[str(id), activation_id, mapper_url, config_file]))
    
    for mapper_thread in mapper_threads:
        mapper_thread.start()
    
    for mapper_thread in mapper_threads:
        mapper_thread.join()

    # print("Mapper Functions ended")
    # print() 



def start_reducer_instances(list_of_ids, activation_id, reducer_url, config_file):
    reducer_threads = []
    # print("Reducer Functions started")
    function_type="reducer"
    for unique_id in list_of_ids:

        wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
        # event_obj_list[id] = threading.Event()
        sema_list[wake_up_obj] = threading.Semaphore(0)
    for id in list_of_ids:
        reducer_threads.append(threading.Thread(target=reducer_function, args=[str(id), activation_id, reducer_url, config_file]))
    
    for reducer_thread in reducer_threads:
        reducer_thread.start()
    
    for reducer_thread in reducer_threads:
        reducer_thread.join()

    # print("Reducer Functions ended")
    # print()

def start_aggregator_instances(data_list_key, activation_id, aggregator_url, config_file):
    # print("Aggregator Functions started")
    ids = {}
    ids["activation_id"] = activation_id
    ids["data_list_key"] = data_list_key
    ids["config_file"] = config_file
    # print(ids)
    function_type = "aggregator"
    unique_id = "1"
    wake_up_obj = function_type+"-"+unique_id+"-"+activation_id
    sema_list[wake_up_obj] = threading.Semaphore(0)

    reply = requests.post(url = aggregator_url
        ,json=ids
        ,verify=False)
    
    sema_list[wake_up_obj].acquire()
    reply = reply.json()
    # print(reply)  

    # print("Aggrregator Functions ended")
    # print()
    return reply


def partition_based_on_list(split_percentage_list
                            ,data_list):
    num_of_blocks = int(len(data_list))
    # print(split_percentage_list)
    list_size = len(split_percentage_list)
    mapping_table = {}
    job_list = [str(i+1) for i in range(list_size)]
    start=0
    end=0
    for i in range(list_size):
        start=end
        if i==list_size-1:
            end=100
        else:
            end = (int(split_percentage_list[i])+end)
        # print(start, end)
        start_i = float(start/100)
        end_i = float(end/100)
        mapping_table[str(i+1)] = (data_list[int(num_of_blocks*start_i): int(num_of_blocks*end_i)])
    
    # display the list results
    # print("Mapping Table:")
    # print(mapping_table)
    return mapping_table, job_list

def equal_partition(parallel_instances
                    ,data_list):
    num_of_blocks = int(len(data_list))
    # print(int(num_of_blocks/parallel_instances))
    # size of each chunk
    size_per_instance = int(num_of_blocks/parallel_instances) + int(num_of_blocks%parallel_instances)

    # partition the list 
    # mapping_table = list(partition(size_per_instance, o_list))
    count=1
    mapping_table={}
    for i in range(0, num_of_blocks, size_per_instance):
        # print(str(count), data_list[i:i + size_per_instance])
        if count==parallel_instances:
            mapping_table[str(count)] = data_list[i:]
            count+=1
            break
        else:
            mapping_table[str(count)] = data_list[i:i + size_per_instance]
        count+=1
    
    job_list = [str(i+1) for i in range(count-1)]
    # display the list results
    # print("Mapping Table:")
    # print(mapping_table)
    return mapping_table, job_list

def single_partitions(data_list):
    num_of_blocks = int(len(data_list))
    count=1
    mapping_table={}
    for i in range(0, num_of_blocks):
        mapping_table[str(count)] = data_list[i:i+1]
        count+=1
    
    job_list = [str(i+1) for i in range(count-1)]
    # display the list results
    # print("Mapping Table:")
    # print(mapping_table)
    return mapping_table, job_list



def multiple_partitions(data_list, blocks_per_instance, job_type, activation_id, parallel_instances):
    num_of_blocks = int(len(data_list))
    job_count=1
    print(parallel_instances)
    mapping_table={}
    for i in range(0, num_of_blocks, blocks_per_instance):
        wake_up_obj = job_type+"-"+str(job_count)+"-"+str(activation_id)
        if job_type=="mapper":
            mapper_response[wake_up_obj]="x"
        else:
            reducer_response[wake_up_obj]="x"
        if job_count==parallel_instances:
            mapping_table[str(job_count)] = data_list[i:]
            job_count+=1
            break
        else:
            mapping_table[str(job_count)] = data_list[i:i+blocks_per_instance]
        job_count+=1
    
    job_list = [str(i+1) for i in range(job_count-1)]
    # display the list results
    # print("Mapping Table:")
    # print(mapping_table)
    return mapping_table, job_list

def splitdata(parameter_file, activation_id):
    global blocks_per_instance
    global per_instance_size
    global num_blocks
    global block_size
    global data_size
    # print("hello")
    print(activation_id)
    startup_nodes = None
    redis_conn = None
    connection_details = parameter_file
    if connection_details["storage"]["type"] == "redis":
        redis_host = connection_details["storage"]["redis"]
        redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
    else:
        startup_nodes = connection_details["storage"]["redis_cluster_nodes"]
        redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # print("hello")
    # activation_id = os.getenv("__OW_ACTIVATION_ID")
    # activation_id = 
    filename = ""
    mapper_split_percentage_list_key=""
    reducer_split_percentage_list_key=""
    split_file=""
    mapper_distribution_using = ""
    reducer_distribution_using = ""
    mapper_parallel_instances=1
    reducer_parallel_instances=1
    mapper_split_percentage_list = []
    mapper_mapping_table={} 
    mapper_list=[]
    reducer_split_percentage_list = []
    reducer_mapping_table={} 
    reducer_list=[]
    # block_size = 100 #in number of characters
    # if len(sys.argv) == 2:
    #     parameter_file = json.loads(sys.argv[1])
    params = parameter_file
    print(json.dumps(params, indent = 1))
    input_data_size=None
    instance_allocation_size=None
    if "input_files" in params.keys():
        input_files = params["input_files"]
    if "mapper_split_percentage_list" in params.keys():
        mapper_split_percentage_list = params["mapper_split_percentage_list"]
    if "reducer_split_percentage_list" in params.keys():
        reducer_split_percentage_list = params["reducer_split_percentage_list"]
    if "mapper_distribution_using" in params.keys():
        mapper_distribution_using = params["mapper_distribution_using"]
    if "reducer_distribution_using" in params.keys():
        reducer_distribution_using = params["reducer_distribution_using"]
    if "mapper_parallel_instances" in params.keys():
        mapper_parallel_instances = int(params["mapper_parallel_instances"])
    if "reducer_parallel_instances" in params.keys():
        reducer_parallel_instances = int(params["reducer_parallel_instances"])
    if "split_file" in params.keys():
        split_file = params["split_file"]
    if "block_size" in params.keys():
        block_size = int(params["block_size"])
    if "blocks_per_instance" in params.keys():
        blocks_per_instance = int(params["blocks_per_instance"])
    if "input_data_size" in params.keys():
        input_data_size = int(params["input_data_size"])
    if "instance_allocation_size" in params.keys():
        instance_allocation_size = int(params["instance_allocation_size"])
    if "split_required" in params.keys():
        split_required = params["split_required"]
    

    
    
    if split_required == "true":
        data_store = parameter_file.get("data_store", "NULL")
        # if data_store != "NULL":

        # ----------------------------------Remote Storage Details-------------------------------------
        data_store = connection_details["data_store"]
        remote_endpoint = data_store["endpoint"]
        remote_access_key = data_store["access_key"]
        remote_secret_key = data_store["secret_key"]
        remote_bucket_name = data_store["bucket_name"]
        
        remote_client = Minio(
            endpoint=remote_endpoint,
            access_key=remote_access_key,
            secret_key=remote_secret_key,
            secure=False,
        )
        found = remote_client.bucket_exists(bucket_name = remote_bucket_name)
        if not found:
            remote_client.make_bucket(bucket_name = remote_bucket_name)
        else:
            print("remote bucket '",remote_bucket_name,"' already exists")
        # ----------------------------------Remote Storage Details-------------------------------------



        # ----------------------------------Temporary Storage Details-------------------------------------
        intermediate_storage = connection_details["intermediate_storage"]
        temporary_bucket_name = intermediate_storage["bucket_name"]
        temporary_access_key = intermediate_storage["access_key"]
        temporary_secret_key = intermediate_storage["secret_key"]
        temporary_endpoint_ip=None
        if intermediate_storage["location"] == "local":
            temporary_endpoint_ip = get_worker_node_ip(connection_details["web_server"])
        else:
            temporary_endpoint_ip = intermediate_storage["endpoint_ip"]
        temporary_endpoint_port = intermediate_storage["endpoint_port"]
        temporary_endpoint = temporary_endpoint_ip+":"+temporary_endpoint_port

        print("temporary_endpoint_ip = ", temporary_endpoint_ip)

        temporary_client = Minio(
            endpoint=temporary_endpoint,
            access_key=temporary_access_key,
            secret_key=temporary_secret_key,
            secure=False,
        )
        found = temporary_client.bucket_exists(bucket_name = temporary_bucket_name)
        if not found:
            temporary_client.make_bucket(bucket_name = temporary_bucket_name)
        else:
            print("Local bucket '",temporary_bucket_name,"' already exists")
        # ----------------------------------Temporary Storage Details-------------------------------------
        
        block_count=0
        for filename in input_files:
            print(filename)
            
            data = remote_client.get_object(remote_bucket_name, filename).data.decode("utf-8")
            data_size = len(data)
            print("Data size =", data_size/1024/1024,"MB")
            print("New block size =", block_size,"MB")
            
            block_size_in_bytes = block_size*1024*1024
            
            blocks = textwrap.wrap(data, block_size_in_bytes, break_long_words=False)
            del(data)
            for block in blocks:
                block_count+=1
                mapper_filename = "mapper-input-"+str(activation_id)+"-"+str(block_count)
                print(mapper_filename, len(block)/1024/1024,"MB")
                # redis_conn.set(mapper_filename, pickle.dumps(block))
                pickled_object = pickle.dumps(block)
                temporary_client.put_object(temporary_bucket_name, mapper_filename, io.BytesIO(pickled_object), length=len(pickled_object))
                # remote_client.put_object(remote_bucket_name, mapper_filename, io.BytesIO(pickled_object), length=len(pickled_object))
                
            del(blocks)
    # print("num_of_blocks =", num_of_blocks)
    num_blocks = math.ceil(input_data_size/block_size)
    blocks_per_instance = int(num_blocks/mapper_parallel_instances)
    print("Number of blocks",num_blocks)
    print("Total Number of blocks of size",block_size,"=", num_blocks)

    print("Blocks Per Instance : ", blocks_per_instance)
    # mapper_parallel_instances = num_blocks
    reducer_parallel_instances = mapper_parallel_instances
    data_list = [str(i+1) for i in range(num_blocks)]
    print(data_list)

    
    # if split_file is false (creating mapper mapping table)
    if mapper_distribution_using=="percentage_list":
        mapper_mapping_table, mapper_list = partition_based_on_list(mapper_split_percentage_list, data_list)
    elif mapper_distribution_using=="equal":
        mapper_mapping_table, mapper_list = equal_partition(mapper_parallel_instances, data_list)
    else:
        mapper_mapping_table, mapper_list = multiple_partitions(data_list, blocks_per_instance, "mapper", activation_id, mapper_parallel_instances)

    redis_conn.set("mapper_mapping_table_key_"+str(activation_id), pickle.dumps(mapper_mapping_table))
    print("mapper_mapping_table", mapper_mapping_table)
    print("mapper_list", mapper_list)

    # if split_file is false (creating reducer mapping table)
    if reducer_distribution_using=="percentage_list":
        reducer_mapping_table, reducer_list = partition_based_on_list(reducer_split_percentage_list, data_list)
    elif reducer_distribution_using=="equal":
        reducer_mapping_table, reducer_list = equal_partition(reducer_parallel_instances, data_list)
    else:
        reducer_mapping_table, reducer_list = multiple_partitions(data_list, blocks_per_instance, "reducer", activation_id, reducer_parallel_instances)
        

    redis_conn.set("reducer_mapping_table_key_"+str(activation_id), pickle.dumps(reducer_mapping_table))
    print("reducer_mapping_table", reducer_mapping_table)
    print("reducer_list", reducer_list)

    mapper_list_key = "mapper_list-"+str(activation_id)
    reducer_list_key = "reducer_list-"+str(activation_id)
    data_list_key = "data_list-"+str(activation_id)

    redis_conn.set(mapper_list_key, pickle.dumps(mapper_list))
    redis_conn.set(reducer_list_key, pickle.dumps(reducer_list))
    redis_conn.set(data_list_key, pickle.dumps(data_list))
    
    print("Splitdata function -", activation_id)
    print("Driver function -", activation_id)

    extra_data = {
    "blocks_per_instance":blocks_per_instance
    ,"num_blocks":num_blocks
    ,"block_size":block_size
    ,"data_size":data_size
    }


    print(json.dumps({  "mapper_list_key":str(mapper_list_key)
                        ,"reducer_list_key":str(reducer_list_key)
                        ,"data_list_key":str(data_list_key)
                        ,"extra_data":extra_data
    }))
    return {    "mapper_list_key":str(mapper_list_key)
                ,"reducer_list_key":str(reducer_list_key)
                ,"data_list_key":str(data_list_key)
                ,"extra_data":extra_data
    }

def upload(data_store, input_files, input_path):
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    print(data_store["endpoint"])
    print(data_store["access_key"])
    print(data_store["secret_key"])
    client = Minio(
        endpoint = data_store["endpoint"],
        access_key=data_store["access_key"],
        secret_key=data_store["secret_key"],
        secure=False,
    )
    my_bucket_name = data_store["bucket_name"]
    print(my_bucket_name)
    # Make 'smareo' bucket if not exist.
    found = client.bucket_exists(bucket_name = my_bucket_name)
    if not found:
        client.make_bucket(bucket_name = my_bucket_name)
    else:
        print("Bucket 'smareo' already exists")

    for filename in input_files:
        input_path_with_filename = input_path+filename
        print(input_path_with_filename)
        # if client.stat_object(bucket_name = my_bucket_name, object_name = filename):
        #     print(filename, "already exists")
        # else:
        client.fput_object(bucket_name = my_bucket_name, object_name = filename, file_path = input_path_with_filename)
        print(filename,"successfully uploaded")

def clean_tmp_data(parameter_file, activation_id):
    # connection_details = parameter_file.get("config_file", "NULL")
    intermediate_storage = parameter_file["intermediate_storage"]
    temporary_bucket_name = intermediate_storage["bucket_name"]
    temporary_access_key = intermediate_storage["access_key"]
    temporary_secret_key = intermediate_storage["secret_key"]
    temporary_endpoint_ip=None
    if intermediate_storage["location"] == "local":
        temporary_endpoint_ip = get_worker_node_ip(connection_details["web_server"])
    else:
        temporary_endpoint_ip = intermediate_storage["endpoint_ip"]
    temporary_endpoint_port = intermediate_storage["endpoint_port"]
    temporary_endpoint = temporary_endpoint_ip+":"+temporary_endpoint_port

    print("temporary_endpoint_ip = ", temporary_endpoint_ip)

    temporary_client = Minio(
        endpoint=temporary_endpoint,
        access_key=temporary_access_key,
        secret_key=temporary_secret_key,
        secure=False,
    )
    found = temporary_client.bucket_exists(bucket_name = temporary_bucket_name)
    if not found:
        temporary_client.make_bucket(bucket_name = temporary_bucket_name)
    else:
        objects = temporary_client.list_objects(bucket_name = temporary_bucket_name, recursive=True)

        # Delete each object in the bucket
        for obj in objects:
            if activation_id in obj.object_name:
                temporary_client.remove_object(temporary_bucket_name, obj.object_name)
        print("Deleted")
def delete_keys_containing_word(dictionary, word):
    keys_to_delete = [key for key in dictionary.keys() if word in key]
    for key in keys_to_delete:
        del dictionary[key]

@app.route('/driver-function/', methods=['GET', 'POST'])
def main():
    



    activation_id = str(uuid.uuid4())
    print("Generated Unique UUID:", activation_id)

    print("Driver Server Function:")
    parameter_file = request.json
    mapper_url = parameter_file.get("mapper_url", "NULL")
    reducer_url = parameter_file.get("reducer_url", "NULL")
    aggregator_url = parameter_file.get("aggregator_url", "NULL")
    if mapper_url=="NULL" or reducer_url=="NULL"or aggregator_url=="NULL":
        return json.dumps({"Output":"Failed"})
    # log_data = ""



    data_store = parameter_file.get("data_store", "NULL")
    config_file = {"web_server":{
                        "url": coordinator_ip
                        ,"port": coordinator_port
                    }
                    ,"storage": parameter_file.get("storage", "NULL")
                    ,"data_store": data_store
                    ,"intermediate_storage": parameter_file.get("intermediate_storage")
    }

    input_path = parameter_file.get("input_path")
    input_files = parameter_file.get("input_files")
    upload_required = parameter_file.get("upload_required", "false")
    if upload_required == "true":
        upload(data_store, input_files, input_path)
    
    # connection_details = config_file
    # startup_nodes = None
    # redis_conn = None
    # if connection_details["storage"]["type"] == "redis":
    #     redis_host = connection_details["storage"]["redis"]
    #     redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
    # else:
    #     startup_nodes = connection_details["storage"]["redis_cluster_nodes"]
    #     redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)
    # config_file.update(parameter_file)
    print(json.dumps(config_file, indent = 1))
    # web_server_spec = 

    print(time.time())
    print("Split Stage")
    start = time.time()
    mapper_list, reducer_list, data_list_key, extra_data = start_splitdata_instance(parameter_file, activation_id)
    end = time.time()
    split_time = round(end-start, 2)
    print("split_time = " + str(split_time) + " " + str(split_time/60) +"m")

    print(time.time())
    print("Metadata Generator Stage")
    start = time.time()
    # mapper_list, reducer_list, data_list_key, num_blocks, activation_id = metadata_generator(parameter_file)
    end = time.time()
    metadata_time = round(end-start, 2)
    print("metadata_time = " + str(metadata_time) + " " + str(metadata_time/60) +"m")

    # activation_id = str(time.time())
    print("Mapper Stage")
    start = time.time()
    start_mapper_instances(mapper_list, activation_id, mapper_url, config_file)
    end = time.time()
    mapper_time = round(end-start, 2)
    print("mapper_time = " + str(mapper_time) + " " + str(mapper_time/60) +"m")
    
    print("Reducer Stage")
    start = time.time()
    start_reducer_instances(reducer_list, activation_id, reducer_url, config_file)
    end = time.time()
    reducer_time = round(end-start, 2)
    print("reducer_time = " + str(reducer_time) + " " + str(reducer_time/60) +"m")


    print("Aggregate Stage")
    start = time.time()
    agg_reply = start_aggregator_instances(data_list_key, activation_id, aggregator_url, config_file)
    end = time.time()
    aggre_time = round(end-start, 2)
    print("aggre_time = " + str(aggre_time) + " " + str(aggre_time/60) +"m")
    total_time = round(mapper_time+reducer_time+aggre_time, 2)
    metrics_fd = open("./metrics/word_count/metrics.txt", "a")
    intermediate_storage = parameter_file.get("intermediate_storage")

    # Containers alive at the end of the execution
    output = subprocess.run("./scripts/kube-instances/count_instances.sh", shell=True, capture_output=True)
    
    if output.stderr.decode("utf-8")!="":
        print(output.stderr.decode("utf-8"))
    output = output.stdout.decode("utf-8")
    script_mapper_count = "0"
    script_reducer_count = "0"
    script_aggre_count = "0"
    if output!="":
        output=output.split("\n")
        script_mapper_count = (output[0])
        script_reducer_count = (output[1])
        script_aggre_count = (output[2])
        print(script_mapper_count, script_reducer_count, script_aggre_count)
    print()

    blocks_per_instance = str(extra_data["blocks_per_instance"])
    num_blocks = str(extra_data["num_blocks"])
    block_size = str(extra_data["block_size"])
    data_size = str(extra_data["data_size"])
    
    metric_data = intermediate_storage["location"]+","+activation_id+","+str(len(mapper_list))\
                +","+str(total_time)+","+str(split_time)+","+str(mapper_time)+","+str(reducer_time)+","+str(aggre_time)\
                +","+str(blocks_per_instance)+","+str(num_blocks)+","+str(block_size)+","\
                +parameter_file.get("input_data_size")+","+script_mapper_count+","+script_reducer_count+","+script_aggre_count+"\n"
    metrics_fd.write(metric_data)
    metrics_fd.close()

    time_stat = {
        "split_time":split_time
        ,"mapper_time":mapper_time
        ,"reducer_time":reducer_time
        ,"aggre_time":aggre_time
    }

    # ----------------------------------Clean Temporary Data-------------------------------------
    clean_tmp_data(parameter_file, activation_id)
    print(response)
    print(mapper_response)
    print(reducer_response)
    delete_keys_containing_word(response, activation_id)
    delete_keys_containing_word(mapper_response, activation_id)
    delete_keys_containing_word(reducer_response, activation_id)
    print(response)
    print(mapper_response)
    print(reducer_response)
    # ----------------------------------Clean Temporary Data-------------------------------------
    
    # redis_conn.set("driver_time_stat", pickle.dumps(time_stat))

    return (json.dumps({
        "input":{
            # "input_files": str(input_filename)
            # ,"split_file": str(split_file)
            # "mapper_parallel_instances": str(mapper_parallel_instances)
            # ,"reducer_parallel_instances": str(reducer_parallel_instances)
            "activation_id":str(activation_id)
            ,"num_blocks":num_blocks
            # ,"mapper_split_percentage_list":str(mapper_split_percentage_list)
            # ,"reducer_split_percentage_list":str(reducer_split_percentage_list)
        }
        ,"output":{
            "output_filename": str(agg_reply["output_filename"])
            ,"number_of_mapper_instances": str(len(mapper_list))
            ,"number_of_reducer_instances": str(len(reducer_list))
            # ,"output": str(agg_reply["aggregator-output"])
            ,"split_time": str(split_time)
            ,"mapper_time": str(mapper_time)
            ,"reducer_time": str(reducer_time)
            ,"aggre_time": str(aggre_time)
            # ,"remaining_time":str(remaining_time)
            
        }
    }))


if __name__ == '__main__':
    coordinator_ip=sys.argv[1]
    coordinator_port=sys.argv[2]
    print(coordinator_ip, coordinator_port)
    app.run(host=coordinator_ip, port=int(coordinator_port))


