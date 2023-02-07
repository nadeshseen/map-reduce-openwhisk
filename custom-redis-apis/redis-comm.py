import sys
import requests
import subprocess
import threading
import redis
import pickle
import rediscluster

def clear_db():
    # reply = subprocess.check_output(["redis-cli -h 10.129.28.219 -n 1 flushdb"], shell=True)
    reply = subprocess.check_output(["redis-cli -h 10.129.28.57 -p 7000 flushall"], shell=True)
    print(reply.decode('utf-8'))
    pass

def input_db(filename):
    # if sys
    file = open(filename)
    file_contents = file.read()
    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    pickled_object = pickle.dumps(file_contents)
    r.set(filename, pickled_object)
    # file_contents = pickle.loads(r.get(filename))
    # print(file_contents)
    # print()
def final_output(output_filename):
    # filename = "final-output-"+splitdata_activation_id
    filename = output_filename
    print("Output stored in Redis with key",filename)
    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    file_contents = pickle.loads(r.get(filename))
    # print(len(file_contents))
    print(file_contents)
    # return file_contents
def output_length(output_filename):
    filename = output_filename
    print("Output stored in Redis with key",filename)
    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    file_contents = pickle.loads(r.get(filename))
    # file_contents = file_contents.split('\n')
    print(len(file_contents))

def compute_words(output_filename):
    filename = output_filename
    print("Output stored in Redis with key",filename)
    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    file_contents = pickle.loads(r.get(filename))
    file_contents = file_contents.split()
    print(len(file_contents))

def output_db(output_filename):
    # filename = "final-output-"+splitdata_activation_id
    filename = output_filename
    print("Output stored in Redis with key",filename)
    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
    file_contents = pickle.loads(r.get(filename))
    print(len(file_contents))
    # print(file_contents)
    return file_contents

def calc(output_filename):
    # filename = "final-output-"+splitdata_activation_id
    filename = output_filename
    print("Output stored in Redis with key",filename)
    startup_nodes = [{"host": "10.129.28.57", "port": "7000"}]
    r = rediscluster.RedisCluster(startup_nodes=startup_nodes)
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
        # if len(sys.argv) > 1:
        # print(sys.argv[1])
        command=sys.argv[1]

        
        if command=="input":
            filename = sys.argv[2]
            input_db(filename)
        elif command=="output":
            filename = sys.argv[2]
            final_output(filename)
        elif command=="length":
            filename = sys.argv[2]
            output_length(filename)
        elif command=="compute":
            filename = sys.argv[2]
            compute_words(filename)
        elif command=="logs":
            num = sys.argv[2]
            output_log(int(num))
        elif command=="time_stat":
            # time_stat = sys.argv[2]
            output_driver_time()
        else:
            filename = sys.argv[2]
            clear_db()
            input_db(filename)
            print(filename, "Inserted")
    except Exception as error:
        print('An exception occurred: {}'.format(error))
        print("Command Format is:")
        print("python3 redis-comm.py command_name(clear/input/output) filename")

  

if __name__ == "__main__":
    main()