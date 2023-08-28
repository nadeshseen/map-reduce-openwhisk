kubectl get pods -n openwhisk --no-headers=true | awk '/mapperfunction/{print $1}'| xargs  echo | wc -w
kubectl get pods -n openwhisk --no-headers=true | awk '/reducerfunction/{print $1}'| xargs  echo | wc -w
kubectl get pods -n openwhisk --no-headers=true | awk '/aggregatorfunction/{print $1}'| xargs  echo | wc -w
