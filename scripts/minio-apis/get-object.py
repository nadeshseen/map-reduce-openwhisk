from minio import Minio
from minio.error import S3Error
import pickle
import json
import sys
def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        endpoint="10.129.28.254:31196",
        access_key="minio",
        secret_key="12345678",
        secure=False,
    )
    file_num = sys.argv[1]
    # print(file_num)
    input_file_name = "input_unique_5m_100m_"+file_num+".txt"
    client.fget_object("input", input_file_name, "test/"+input_file_name)

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)