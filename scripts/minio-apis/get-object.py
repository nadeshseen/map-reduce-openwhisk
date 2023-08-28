from minio import Minio
from minio.error import S3Error
import pickle
import json
def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        "10.129.26.184:9000",
        access_key="2OD6Pu39mU4fiieA",
        secret_key="bfv7DPEr86IcC1hhlhUuHqQMOlFGGSEF",
        secure=False,
    )

    # Make 'smareo' bucket if not exist.
    # Get data of an object.
    activation_id = "None"
    data_id = "1"
    mapper_input_param = "mapper-input-"+activation_id+"-"+data_id
    response = client.get_object("smareo", mapper_input_param)
    print(json.dumps(pickle.loads(client.get_object("smareo", "final-output-1").data), indent=4))
    # print(response.data)
    # Read data from response.
    response.close()
    response.release_conn()

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)