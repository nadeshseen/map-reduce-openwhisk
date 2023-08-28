var1="wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio_20230602231726.0.0_amd64.deb -O minio.deb"
var2="echo 1234 | sudo -S dpkg -i minio.deb"
echo "###############smareo-1#################"
# ssh smareo@10.129.26.126 "$var1" &
ssh smareo@10.129.26.126 "$var2" &
echo "###############smareo-2#################"
# ssh smareo@10.129.26.125 "$var1" &
ssh smareo@10.129.26.125 "$var2" &
echo "###############smareo-3#################"
# ssh smareo@10.129.27.189 "$var1" &
ssh smareo@10.129.27.189 "$var2" &
echo "###############smareo-4#################"
# ssh smareo@10.129.27.55 "$var1" &
ssh smareo@10.129.27.55 "$var2" &