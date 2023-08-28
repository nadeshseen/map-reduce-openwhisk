import subprocess
import time 

def usage():

    mapper_fd = open("mapper_usage.txt", "w")
    reducer_fd = open("reducer_usage.txt", "w")
    mapper_fd.close()
    reducer_fd.close()
    while(True):
        mapper_fd = open("mapper_usage.txt", "a")
        reducer_fd = open("reducer_usage.txt", "a")
        time.sleep(5)
        output = subprocess.check_output("kubectl top pods -n openwhisk", shell=True)
        openwhisk_usage = output.decode().split("\n")
        mapper_usage = [x for x in openwhisk_usage if "mapper" in x]
        reducer_usage = [x for x in openwhisk_usage if "reducer" in x]
        for mapper in mapper_usage:
            mapper_fd.write(mapper+"\n")
            print(mapper)
        for reducer in reducer_usage:
            reducer_fd.write(reducer+"\n")
            print(reducer)
        if(len(mapper_usage)>0):
            mapper_fd.write("\n")

        if(len(reducer_usage)>0):
            reducer_fd.write("\n")
        
        mapper_fd.close()
        reducer_fd.close()


if __name__ == '__main__':
    usage()