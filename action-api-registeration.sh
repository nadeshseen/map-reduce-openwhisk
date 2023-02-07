wsk -i action update mapper-function --docker 10.129.28.219:5000/mapper-function mapper-function/mapper-function.py --web true --timeout 300000 
wsk -i action update reducer-function --docker 10.129.28.219:5000/reducer-function reducer-function/reducer-function.py --web true --timeout 300000 --memory 512
wsk -i action update aggregator-function --docker 10.129.28.219:5000/aggregator-function aggregator-function/aggregator-function.py --web true --timeout 300000
wsk -i api create /aggregator-function /aggregate post aggregator-function --response-type json
wsk -i api create /reducer-function /reducer post reducer-function --response-type json 
wsk -i api create /mapper-function /mapper post mapper-function --response-type json