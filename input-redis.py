import redis
import pickle
def main():
    r = redis.Redis(host='10.129.28.219', port=6379, db=1)
    pickled_object = pickle.dumps("Redis values may be a  a same random values number  of of different data types.\n\
     We’ll cover some of the more essential value data types\n\
      in this tutorial: string, list, hashes, and sets.")
    r.set("input.txt", pickled_object)
    value = pickle.loads(r.get("input.txt"))
    print(value)

if __name__ == "__main__":
    main()
