import requests
import sys
import json

def server():
    server_ip = "10.129.28.219"
    server_port = "5001"
    dag_name = "driver-function"
    driver_url = "http://"+server_ip+":"+server_port+"/"+dag_name+"/"
    input_json_file = open(sys.argv[1])
    params = json.load(input_json_file)
    print(params)
    reply = requests.post(url = driver_url, json = params)

    print(reply.json())



def main():
    server()

if __name__=="__main__":
    main()