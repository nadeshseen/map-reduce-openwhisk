#!/usr/bin/env python3
import redis
import os
import json
import pickle
import sys
def main():
    r = redis.Redis(host="10.129.28.219", port=6379, db=1)
    activation_id = os.getenv("__OW_ACTIVATION_ID")
    filename = "input1234.txt"
    if len(sys.argv) == 2:
        params = json.loads(sys.argv[1])
        filename = params["filename"]
    data = pickle.loads(r.get(filename))
    lines = data.split("\n")
    count=1
    # print(lines)
    # temp=""
    list = []
    for line in lines:
        list.append(count)
        filename = "mapper-input-"+str(activation_id)+"-"+str(count)
        # print(filename, line)
        # temp+=filename+"----"
        pickled_object = pickle.dumps(line)
        r.set(filename, pickled_object)
        count+=1

    list_of_ids = "split-data-id-list-"+str(activation_id)
    r.set(list_of_ids, pickle.dumps(list))
    print(json.dumps({  "splitdata": str(len(lines))
                        ,"activation_id": str(activation_id)
                        ,"list_of_ids":str(list_of_ids)
    }))

if __name__ == "__main__":
    main()
