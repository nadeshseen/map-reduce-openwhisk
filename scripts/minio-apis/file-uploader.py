from minio import Minio
from minio.error import S3Error
import sys
import time


def multiple():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        endpoint="10.129.28.204:32706",
        access_key="minio",
        secret_key="12345678",
        secure=False,
    )

    # Make 'smareo' bucket if not exist.
    bucket_name = "input"
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        client.make_bucket(bucket_name=bucket_name)
    else:
        print("Bucket "+bucket_name+" already exists")

    # Upload '/home/nadesh/Downloads/college/miniotesting/input.txt' as object name
    # 'asiaphotos-2015.zip' to bucket 'smareo'.
    
    file_path = sys.argv[1]
    filename = sys.argv[2]
    
    print(filename)
    input_filename = file_path+filename
    print(len(sys.argv))
    object_name = sys.argv[3]
    if len(sys.argv) >= 5:
        
        num_objs = int(sys.argv[4])
        my_list = list(range(num_objs))
        if len(sys.argv) == 6:
            file_num = int(sys.argv[4])-1
            end = int(sys.argv[5])-1
            my_list = list(range(file_num, end))
        # print(my_list)
        for i in my_list:
            file = object_name+"_"+str(i+1)+".txt"
            print(file)
            
            start = time.time()
            client.fput_object(bucket_name, file, input_filename,)
            upload_time = time.time() - start
            print("Upload Time - ", upload_time)
            print(filename+" is successfully uploaded as object "+file +" to bucket "+bucket_name+".")
        pass
    else:
        start = time.time()
        file = object_name+".txt"
        print(file)
        client.fput_object(bucket_name, file, input_filename,)
        upload_time = time.time() - start
        print("Upload Time - ", upload_time)
        print(filename+" is successfully uploaded as object "+file +" to bucket "+bucket_name+".")

    


if __name__ == "__main__":
    try:
        multiple()
    except S3Error as exc:
        print("error occurred.", exc)