import redis
import pickle
def main():
    filename = "log_data"
    print("Log Data")
    r = redis.Redis(host='10.129.28.219', port=6379, db=1)
    file_contents = pickle.loads(r.get(filename))
    print(file_contents)
    print()

if __name__=="__main__":
    main()