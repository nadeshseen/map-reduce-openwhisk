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


def wake_up():
    server_ip = "10.129.28.219"
    server_port = "5001"
    driver_url = "http://"+server_ip+":"+server_port+"/wake-up/"
    # input_json_file = open(sys.argv[1])
    # params = json.load(input_json_file)
    # print(params)

    function_type = "mapper"
    unique_id = "1"
    activation_id = "None"
    
    unique_id=sys.argv[1]
    function_type=sys.argv[2]
    reply = requests.post(url = driver_url, json = {"function_type": str(function_type), "unique_id": str(unique_id), "activation_id": str(activation_id)})

    print(reply.json())
    

def main():

    wake_up()
    

    

    pass

if __name__=="__main__":
    main()