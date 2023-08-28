command="kubeadm join 10.129.26.3:6443 --token zyc9vq.c7w47cj9efpnigvp --discovery-token-ca-cert-hash sha256:cfa82106c780decfcd1be0cf41aeb007b3ce086be475bb01e09ac8f204c88bc5"
echo "###############smareo-1################3"
ssh smareo@10.129.26.126 "echo 1234 | sudo -S $command" &
echo "###############smareo-2################3"
ssh smareo@10.129.26.125 "echo 1234 | sudo -S $command" &
echo "###############smareo-3################3"
ssh smareo@10.129.27.189 "echo 1234 | sudo -S $command" &
echo "###############smareo-4################3"
ssh smareo@10.129.27.55 "echo 1234 | sudo -S $command" &
echo "############Done###################"