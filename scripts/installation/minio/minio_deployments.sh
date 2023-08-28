comm="podman run -p 9000:9000 -p 9001:9001 -v ~/minio/data:/data quay.io/minio/minio server /data --console-address ":9001""
echo "###############smareo-1#################"
ssh smareo@10.129.26.126 "$comm" &
echo "###############smareo-2#################"
ssh smareo@10.129.26.125 "$comm" &
echo "###############smareo-3#################"
ssh smareo@10.129.27.189 "$comm" &
echo "###############smareo-4#################"
ssh smareo@10.129.27.55 "$comm" &