import subprocess
import sys

podname = sys.argv[1]
print(podname)
comm = "kubectl get pods -n openwhisk --no-headers=true | awk '/"+podname+"/{print $1}'| xargs  kubectl delete -n openwhisk pod"
reply = subprocess.check_output([comm], shell=True)
print(reply.decode())


# kubectl get pods -n openwhisk --no-headers=true | awk '/prewarm-nodejs/{print $1}'| xargs  kubectl delete -n openwhisk pod
# kubectl get pods -n openwhisk --no-headers=true | awk '/invokerhealthtestaction/{print $1}'| xargs  kubectl delete -n openwhisk pod


