# echo Total Pods Running
# kubectl get pods -n openwhisk -o wide --no-headers=true | awk '/Running/{print $0}' | wc -l
val="$(kubectl get pods -n openwhisk -o wide --no-headers=true)"
echo Total Pods
# echo "$val"
echo "$val" | wc -l
echo Total lithops actions initiated
echo "$val" | awk '/dockerio/{print $0}' | wc -l
echo "Total invoker health actions (0 - all invoker healthy)"
echo "$val" | awk '/invokerhealthtestaction0/{print $0}' | wc -l

echo smareo-worker2 
echo "$val" | awk '/smareo-worker2/{print $0}' | wc -l
echo smareo-worker3
echo "$val" | awk '/smareo-worker3/{print $0}' | wc -l
echo smareo-worker4
echo "$val" | awk '/smareo-worker4/{print $0}' | wc -l
echo lithops-5
echo "$val" | awk '/lithops-5/{print $0}' | wc -l
echo lithops-6
echo "$val" | awk '/lithops-6/{print $0}' | wc -l
echo lithops-7
echo "$val" | awk '/lithops-7/{print $0}' | wc -l
echo lithops-8
echo "$val" | awk '/lithops-8/{print $0}' | wc -l
echo ub-05-1
echo "$val" | awk '/ub-05-1/{print $0}' | wc -l
echo ub-05-2
echo "$val" | awk '/ub-05-2/{print $0}' | wc -l
echo ub-05-3
echo "$val" | awk '/ub-05-3/{print $0}' | wc -l
echo ub-05-4
echo "$val" | awk '/ub-05-4/{print $0}' | wc -l
