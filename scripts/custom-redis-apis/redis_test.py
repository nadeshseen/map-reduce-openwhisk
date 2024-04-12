import redis_comm
import sys

import redis as redis_lib
# import rediscluster as redis_lib
openwhisk_nodes = {
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

def main():
    try:
        iterlist = list(range(int(sys.argv[1])))
        objname = sys.argv[2]
        filepath = sys.argv[3]
        print(iterlist)
        print(objname)
        print(filepath)
        iternum = 1
        for worker_node in openwhisk_nodes:
            print(iternum)
            print(worker_node)
            iternum+=1
            redis_host = {}
            redis_host["host"] = worker_node
            redis_host["port"] = "6379"
            redis_conn = redis_lib.Redis(host=redis_host["host"], port=redis_host["port"])
            for i in iterlist:
                redis_comm.input_db(redis_conn, objname, filepath)
            for i in iterlist:
                redis_comm.output_db(redis_conn, objname)

    except Exception as error:
        print('An exception occurred: {}'.format(error))
        print("Command Format is:")
        print("python3 redis-comm.py command_name(clear/input/output) filename")

  

if __name__ == "__main__":
    main()