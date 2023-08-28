echo "###############smareo-primary#################"
sudo swapoff -a &&
sudo kubeadm init --pod-network-cidr=10.244.0.0/16 && 
echo "###############kubeadminit####################" && 
mkdir -p $HOME/.kube && sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config && sudo chown $(id -u):$(id -g) $HOME/.kube/config &&
kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml &&
echo "############Done###################"