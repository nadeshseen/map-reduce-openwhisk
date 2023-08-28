from minio import Minio
from minio.error import S3Error
import sys

def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        endpoint="10.129.26.118:30772",
        access_key="minio",
        secret_key="12345678",
        secure=False,
    )

    # Make 'smareo' bucket if not exist.
    found = client.bucket_exists("smareo")
    if not found:
        client.make_bucket("smareo")
    else:
        print("Bucket 'smareo' already exists")

    # Upload '/home/nadesh/Downloads/college/miniotesting/input.txt' as object name
    # 'asiaphotos-2015.zip' to bucket 'smareo'.
    filename = sys.argv[1]
    print(filename)
    input_filename = "../input_files/"+filename
    activation_id = "None"
    data_id = "1"
    # object_name = "mapper-input-"+activation_id+"-"+data_id
    object_name = filename
    client.fput_object(
        "smareo", object_name, input_filename,
    )
    print(
        "'input.txt' is successfully uploaded as "
        "object 'input.txt' to bucket 'smareo'."
    )


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)