import sys
import requests
import subprocess
import threading
import redis
import pickle
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# import builtins
# import traceback

# def print(*objs, **kwargs):
#     my_prefix = "driver-code : "
#     builtins.print(my_prefix, *objs, **kwargs)

def start_splitdata_instance():
    print("Split Data")
    reply = requests.get(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/splitdata-function",
     verify=False)
    reply = reply.json()
    print(reply)
    # list = reply["list"].encode("latin_1")
    # print(list.decode("utf-8"))
    print()
    return int(reply["splitdata"]), reply["activation_id"]
    

def mapper_function(unique_id, activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/mapper-function",
        json={"unique_id":str(unique_id), "activation_id":str(activation_id)},
        verify=False)
    reply = reply.json()
    print(reply)

def start_mapper_instances(number_of_threads, activation_id):
    mapper_threads = []
    print("Mapper Functions started")
    for i in range(0, number_of_threads):
        mapper_threads.append(threading.Thread(target=mapper_function, args=[str(i+1), activation_id]))
    
    for mapper_thread in mapper_threads:
        mapper_thread.start()
    
    for mapper_thread in mapper_threads:
        mapper_thread.join()

    print("Mapper Functions ended")
    print()

def reducer_function(unique_id, activation_id):
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/reducer-function",
        json={"unique_id":str(unique_id), "activation_id":str(activation_id)},
        verify=False)
    reply = reply.json()
    print(reply)  

def start_reducer_instances(number_of_threads, activation_id):
    reducer_threads = []
    print("Reducer Functions started")
    for i in range(0, number_of_threads):
        reducer_threads.append(threading.Thread(target=reducer_function, args=[str(i+1), activation_id]))
    
    for reducer_thread in reducer_threads:
        reducer_thread.start()
    
    for reducer_thread in reducer_threads:
        reducer_thread.join()

    print("Reducer Functions ended")
    print()



def start_aggregator_instances(number_of_threads, activation_id):
    print("Aggregator Functions started")
    ids = {}
    list = []
    for i in range(0, number_of_threads):
        # ids["value"+str(i+1)] = str(i+1)
        list.append(i+1)
    ids["activation_id"] = activation_id
    ids["list_of_ids"] = list
    # print(ids)
    reply = requests.post(url = "https://10.129.28.219:31001/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/aggregator-function",
        json=ids,
        verify=False)
    reply = reply.json()
    print(reply)  

    print("Aggrregator Functions ended")
    print()


def clear_db():
    reply = subprocess.check_output(["redis-cli -h 10.129.28.219 -n 1 flushdb"], shell=True)
    print(reply.decode('utf-8'))
    pass

def input_db():
    filename = "input.txt"
    # if sys
    file = open(filename)
    file_contents = file.read()
    r = redis.Redis(host='10.129.28.219', port=6379, db=1)
    pickled_object = pickle.dumps(file_contents)
    r.set(filename, pickled_object)
    file_contents = pickle.loads(r.get(filename))
    print(file_contents)
    print()

def output_db(splitdata_activation_id):
    filename = "final-output-"+splitdata_activation_id
    print("Output stored in Redis with key",filename)
    r = redis.Redis(host='10.129.28.219', port=6379, db=1)
    file_contents = pickle.loads(r.get(filename))
    print(file_contents)
    print()

def main():
    input_db()
    number_of_mapper_instances, splitdata_activation_id = start_splitdata_instance()
    print(number_of_mapper_instances, splitdata_activation_id)
    number_of_reducer_instances = number_of_mapper_instances
    start_mapper_instances(number_of_mapper_instances, splitdata_activation_id)
    start_reducer_instances(number_of_reducer_instances, splitdata_activation_id)
    start_aggregator_instances(number_of_reducer_instances, splitdata_activation_id)
    output_db(splitdata_activation_id)

    # clear_db()
    pass
  

if __name__ == "__main__":
    main()