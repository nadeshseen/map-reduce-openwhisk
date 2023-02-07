#!/usr/bin/env python3
import redis
# import aisehi
import json
def main():
    r = redis.Redis(host='10.129.28.219', port=6379)
    # r.set('foo', 'bar')
    value = r.get('foo')
    value = value.decode("utf-8")
    # print(value)
    # print("Mapper Function")
    # my_dict = 
    # print(my_dict)
    # return {"tokens": "tokenize_data", "new:": "aise hi"}
    
    print(json.dumps({"tokens": "tokenize_data", "value": str(value)}))
    # main("nadesh")
if __name__ == "__main__":
    main()
