kubectl get pods -n openwhisk --no-headers=true | awk '/prewarm-nodejs/{print $1}'| xargs  kubectl delete -n openwhisk pod
