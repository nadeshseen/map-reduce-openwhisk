wsk -i action update mapper-function --docker 10.129.26.3:5000/mapper-function mapper-function/mapper-function.py --web true --timeout 2400000 --memory 600
wsk -i action update reducer-function --docker 10.129.26.3:5000/reducer-function reducer-function/reducer-function.py --web true --timeout 2400000 --memory 600
wsk -i action update aggregator-function --docker 10.129.26.3:5000/aggregator-function aggregator-function/aggregator-function.py --web true --timeout 2400000 --memory 600
# # wsk -i action update aggregator-function-max --docker 10.129.26.3:5000/aggregator-function-max aggregator-function-max/aggregator-function-max.py --web true --timeout 300000 --memory 512
# # wsk -i api create /aggregator-function-max /aggregate-max post aggregator-function-max --response-type json
wsk -i api create /aggregator-function /aggregate post aggregator-function --response-type json
wsk -i api create /reducer-function /reducer post reducer-function --response-type json 
wsk -i api create /mapper-function /mapper post mapper-function --response-type json