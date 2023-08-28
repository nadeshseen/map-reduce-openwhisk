sudo kubeadm reset &&
sudo rm -r $HOME/.kube &&
sudo rm -r /etc/cni/net.d &&
sudo ip link delete cni0 &&
sudo ip link delete flannel.1