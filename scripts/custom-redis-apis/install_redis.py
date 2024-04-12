import sys
import requests
import subprocess
import threading
import redis as redis_lib
import pickle
# import rediscluster as redis_lib

username = "smareo"
worker_node_ip = {
                # "10.129.28.58",
                "10.129.26.125",
                "10.129.27.189",
                "10.129.27.55",
                "10.129.26.157",
                "10.129.26.121",
                "10.129.27.36",
                "10.129.26.53",
                "10.129.26.94",
                "10.129.28.150",
                "10.129.28.49",
                "10.129.27.175",
                }
# comm = " \"echo 1234 | sudo -S apt install -y redis\" "
# comm2 = "\"echo 1234 | sudo -S systemctl status redis\""
comm = "\"echo 1234 | sudo -S systemctl restart redis\""
# comm = "\"uname -a\""
# comm = "\"echo 1234 | sudo -S swapoff -a\""
# comm = "\"echo 1234 | sudo -S reboot\""
# comm = "redis-cli -h 10.129.28.57 -p 7000 flushall"
def install_redis():
    # reply = subprocess.check_output(["redis-cli -h 10.129.28.219 -n 1 flushdb"], shell=True)
    for worker_node in worker_node_ip:
        print(worker_node)
        ssh_comm = "ssh "+username+"@"+worker_node+ " "+comm
        print(ssh_comm)
        print()
        reply = subprocess.check_output([ssh_comm], shell=True)
        print(reply.decode('utf-8'))
        print()
        ssh_comm = "ssh "+username+"@"+worker_node+ " "+comm2
        print(ssh_comm)
        print()
        reply = subprocess.check_output([ssh_comm], shell=True)
        print(reply.decode('utf-8'))
        print()

def restart_redis():
    # reply = subprocess.check_output(["redis-cli -h 10.129.28.219 -n 1 flushdb"], shell=True)
    for worker_node in worker_node_ip:
        
        print(worker_node)
        

        ssh_comm = "echo 1234 | sudo -S scp /etc/redis/redis.conf "+username+"@"+worker_node+":redis.conf"
        # ssh_comm = "ssh "+username+"@"+worker_node+ " "+comm
        print(ssh_comm)
        print()
        reply = subprocess.check_output([ssh_comm], shell=True)
        print(reply.decode('utf-8'))
        print()


        comm = "\"echo 1234 | sudo -S cp redis.conf /etc/redis/redis.conf\" "
        ssh_comm = "ssh "+username+"@"+worker_node+ " "+comm
        print(ssh_comm)
        print()
        reply = subprocess.check_output([ssh_comm], shell=True)
        print(reply.decode('utf-8'))
        print()


        comm = "\"echo 1234 | sudo -S systemctl restart redis\" "
        ssh_comm = "ssh "+username+"@"+worker_node+ " "+comm
        print(ssh_comm)
        print()
        reply = subprocess.check_output([ssh_comm], shell=True)
        print(reply.decode('utf-8'))
        print()

def clear_redis():
    # reply = subprocess.check_output(["redis-cli -h 10.129.28.219 -n 1 flushdb"], shell=True)
    for worker_node in worker_node_ip:
        print(worker_node)
        port = 6379
        comm = "redis-cli -h "+str(worker_node)+ " -p "+str(port)+" flushall"
        print(comm)
        reply = subprocess.check_output([comm], shell=True)
        print(reply.decode('utf-8'))
        print()

def main():
    try:
      clear_redis()
    #   restart_redis()

    except Exception as error:
        print('An exception occurred: {}'.format(error))

  

if __name__ == "__main__":
    main()