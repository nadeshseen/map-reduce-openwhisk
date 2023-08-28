import os
import subprocess
import json

list_of_data = ["10", "20", "30", "40"]
intermediate_storage = ["remote", "local"]
mapper_parallel_instances = ["15"]
# mapper_parallel_instances = ["1"]
# list_of_data = ["4"]
# intermediate_storage = ["remote"]
json_path = "../params.json"
# for location in intermediate_storage:
for instances in mapper_parallel_instances:
    print("Instances =", instances)
    # output = subprocess.check_output("../custom_kube_delete_prewarm.sh", shell=True)
    # output = output.decode("utf-8")
    # print(output)

    with open(json_path, 'r') as file:
        data = json.load(file)
        # data["input_data_size"] = data_size
        # data["intermediate_storage"]["location"] = location
        data["mapper_parallel_instances"] = instances

    with open(json_path, 'w') as file:
        json.dump(data, file, indent = 4)
    
    output = subprocess.run("python3 ../control-plane/demo-client-invocation-file.py ../params.json ../specification.json", shell=True, capture_output=True)
    # output = output.stdout.decode("utf-8")
    # print(output)

    # try:        
    # output = subprocess.run("../scripts/count_instances.sh", shell=True, capture_output=True)
    # output = output.stdout.decode("utf-8")

    # script_mapper_count = 0
    # script_reducer_count = 0
    # script_aggre_count = 0
    # if output!="":
    #     output=output.split("\n")
    #     script_mapper_count = int(output[0])
    #     script_reducer_count = int(output[1])
    #     script_aggre_count = int(output[2])
    #     print(script_mapper_count, script_reducer_count, script_aggre_count)
    # print()
    print("Deleting containers...........")
    output = subprocess.run("../scripts/custom_kube_delete.sh", shell=True, capture_output=True)
    output = output.stdout.decode("utf-8")
    if output!="":
        print(output)
    
    # except:
        # print("Couldn't delete all pods")
