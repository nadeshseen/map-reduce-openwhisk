import requests
import sys
import json

def server():
    input_spec_file = open(sys.argv[2])
    web_server_spec = json.load(input_spec_file)
    web_server = web_server_spec.get("web_server", "NULL")
    if web_server=="NULL":
        print("Could not get web server details")
        return
    print("Web Server Specs")
    print(json.dumps(web_server, indent = 1))
    input_json_file = open(sys.argv[1])
    params = json.load(input_json_file)
    print("Input File")
    print(json.dumps(params, indent = 1))

    
    server_ip = web_server["url"]
    server_port = web_server["port"]
    
    dag_name = "driver-function"
    driver_url = "http://"+server_ip+":"+server_port+"/"+dag_name+"/"

    
    reply = requests.post(url = driver_url, json = params)
    print("Output: ")
    print(json.dumps(reply.json(), indent = 1))



def main():
    server()

if __name__=="__main__":
    main()