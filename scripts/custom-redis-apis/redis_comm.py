import sys, time
import requests
import subprocess
import threading
import redis as redis_lib
import pickle
# import rediscluster as redis_lib
openwhisk_nodes = {
                "10.129.28.58",
                "10.129.26.126",
                "10.129.26.125",
                "10.129.27.189",
                "10.129.26.157",
                "10.129.26.121",
                "10.129.27.36",
                "10.129.26.53",
                "10.129.26.94",
                "10.129.28.150",
                "10.129.28.49",
                "10.129.27.175",
                }
def clear_db():
    # reply = subprocess.check_output(["redis-cli -h 10.129.28.219 -n 1 flushdb"], shell=True)
    # reply = subprocess.check_output(["redis-cli -h 10.129.28.57 -p 7000 flushall"], shell=True)
    # print(reply.decode('utf-8'))
    # reply = subprocess.check_output(["redis-cli -h 10.129.28.101 -p 7002 flushall"], shell=True)
    # print(reply.decode('utf-8'))
    # reply = subprocess.check_output(["redis-cli -h 10.129.27.40 -p 7004 flushall"], shell=True)
    # print(reply.decode('utf-8'))
    pass

def input_db(r, objname, filepath):
    # if sys
    file = open(filepath)
    file_contents = file.read()
    pickled_object = pickle.dumps(file_contents)
    
    upload_start_time = time.time()
    r.set(objname, pickled_object)
    upload_time = time.time() - upload_start_time
    print(upload_time)
    # file_contents = pickle.loads(r.get(filename))
    # print(file_contents)
    # print()
def final_output(r, output_filename):
    # filename = "final-output-"+splitdata_activation_id
    filename = output_filename
    print("Output stored in Redis with key",filename)
    file_contents = (r.get(filename))
    # print(len(file_contents))
    print(file_contents)
    # return file_contents
def output_length(r, output_filename):
    filename = output_filename
    print("Output stored in Redis with key",filename)
    file_contents = pickle.loads(r.get(filename))
    # file_contents = file_contents.split('\n')
    print(len(file_contents))

def compute_words(r, output_filename):
    filename = output_filename
    print("Output stored in Redis with key",filename)
    file_contents = pickle.loads(r.get(filename))
    file_contents = file_contents.split()
    print(len(file_contents))

def output_db(r, output_filename):
    # filename = "final-output-"+splitdata_activation_id
    filename = output_filename
    print("Output stored in Redis with key",filename)
    
    download_start_time = time.time()
    file_contents = pickle.loads(r.get(filename))
    download_time = time.time() - download_start_time
    print(download_time)
    # print(len(file_contents))
    # print(file_contents)
    return file_contents

def calc(r, output_filename):
    # filename = "final-output-"+splitdata_activation_id
    filename = output_filename
    print("Output stored in Redis with key",filename)
    file_contents = pickle.loads(r.get(filename))
    print(file_contents)
    print()
    return float(file_contents)

def output_log(num):
    mapper_list = [str(i+1) for i in range(num)]
    print(mapper_list)
    total=0
    for mapper_id in mapper_list:
        mapper_metadata = "mapper-metadata-time-"+mapper_id
        mapper_data = "mapper-data-time-"+mapper_id
        mapper_total = "mapper-total-time-"+mapper_id
        calc(mapper_metadata)
        calc(mapper_data)
        total+=calc(mapper_total)
    print(total)


def output_driver_time():
    time_stat = output_db("driver_time_stat")
    total_time = 0
    for i in time_stat.values():
        print(i)
        total_time+=i
    print("total_time", total_time, "seconds")
def main():
    try:
        conn_type = "redis"
        redis_conn = None
        # if conn_type == "redis":
        redis_host = {}
        redis_host["host"] = "10.129.26.125"
        redis_host["port"] = "6379"
        redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
        # else:
        #     startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
        #     redis_conn = redis_lib.RedisCluster(startup_nodes=startup_nodes)    
        
        
        
        # if len(sys.argv) > 1:
        # print(sys.argv[1])
        
        print("Options - ")
        print("1. input")
        print("2. output")
        print("3. length")
        print("4. compute")
        print("5. logs")
        print("6. time_stat")
        
        command=input("Select one option - ")
        
        if command=="input":
            filename = input("Type the filename - ")
            filepath = input("Type the filepath - ")
            input_db(redis_conn, filename, filepath)
        elif command=="output":
            filename = input("Type the filename - ")
            final_output(redis_conn, filename)
        elif command=="length":
            filename = input("Type the filename - ")
            output_length(redis_conn, filename)
        elif command=="compute":
            filename = input("Type the filename - ")
            compute_words(redis_conn, filename)
        elif command=="logs":
            num = input("Type the filename - ")
            output_log(int(num))
        elif command=="time_stat":
            # time_stat = input()
            output_driver_time()
        else:
            clear_db()
            for filename in sys.argv[2:]:
                print(filename)
                input_db(redis_conn, filename)
                print(filename, "Inserted")

    except Exception as error:
        print('An exception occurred: {}'.format(error))
        print("Command Format is:")
        print("python3 redis-comm.py command_name(clear/input/output) filename")

  

if __name__ == "__main__":
    main()