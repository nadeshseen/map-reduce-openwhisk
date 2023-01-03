import requests
import sys
import json

import textwrap

def testing():
    file_fd = open("input.txt", "r")
    count = 0
    
    while True:
        count += 1
        
        # Get next line from file
        line = file_fd.readline()
        
        # if line is empty
        # end of file is reached
        if not line:
            break
        print(len(line))
        print("Line{}: {}".format(count, len(line.strip())))
    # line = file_fd.read()
    # print(line)
    # print(len(line))
    # # line = "sadf asdf sdfsdfsdfsdf kj"
    # block_size = 200 #in number of characters
    # result = textwrap.wrap(line,block_size, break_long_words=False)
    # print(result)
    # sum=0
    # for i in result:
    #     sum+=len(i)

    # print(sum)

def server():
    server_ip = "10.129.28.219"
    server_port = "5001"
    driver_url = "http://"+server_ip+":"+server_port+"/driver-function/"
    input_json_file = open(sys.argv[1])
    params = json.load(input_json_file)
    print(params)
    reply = requests.post(url = driver_url, json = params)

    print(reply.json())
def main():

    

    

    pass

if __name__=="__main__":
    main()