command="echo 1234 | sudo -S kubeadm reset -f && echo 1234 | sudo -S  rm -r /etc/cni/net.d && echo 1234 | sudo -S ip link delete cni0 && echo 1234 | sudo -S ip link delete flannel.1"
command="echo 1234 | sudo -S ip link delete cni0 && echo 1234 | sudo -S ip link delete flannel.1"
echo "###############smareo-1################3"
ssh smareo@10.129.26.126 "$command"  &
echo "###############smareo-2################3"
ssh smareo@10.129.26.125 "$command" &
echo "###############smareo-3################3"
ssh smareo@10.129.27.189 "$command" &
echo "###############smareo-4################3"
ssh smareo@10.129.27.55 "$command" &
echo "############Done###################"