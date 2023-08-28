
# comm1="echo 1234 | sudo -S apt-get update"
comm2="echo 1234 | sudo -S apt -y install podman"
comm1="echo \"deb https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_20.04/ /\" | sudo tee /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list"
comm1="curl -L "https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_20.04/Release.key" | sudo apt-key add -"
# comm1="sudo apt update"
# comm1="mkdir -p ~/minio/data"
# comm1="sudo apt install podman"
comm1="echo 1234 | sudo -S apt -y install podman"
echo "###############smareo-1#################"
ssh smareo@10.129.26.126 "$comm1" &
# ssh smareo@10.129.26.126 "$comm2" 
echo "###############smareo-2#################"
ssh smareo@10.129.26.125 "$comm1" &
# ssh smareo@10.129.26.125 "$comm2" 
echo "###############smareo-3#################"
ssh smareo@10.129.27.189 "$comm1" &
# ssh smareo@10.129.27.189 "$comm2" 
echo "###############smareo-4#################"
ssh smareo@10.129.27.55 "$comm1" &
# ssh smareo@10.129.27.55 "$comm2" 