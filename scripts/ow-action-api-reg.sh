# docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip install -r requirements.txt"
# cd mapper-function-ow && zip -r package.zip virtualenv __main__.py && cd ..
wsk -i action update mapper-function --kind python:3 mapper-function-ow/package.zip --web true --timeout 2400000 --memory 600
# cd reducer-function-ow && zip -r package.zip virtualenv __main__.py && cd ..
wsk -i action update reducer-function --kind python:3 reducer-function-ow/package.zip --web true --timeout 2400000 --memory 600
cd aggregator-function-ow && zip -r package.zip virtualenv __main__.py && cd ..
wsk -i action update aggregator-function --kind python:3 aggregator-function-ow/package.zip --web true --timeout 2400000 --memory 600

wsk -i api create /aggregator-function /aggregate post aggregator-function --response-type json
wsk -i api create /reducer-function /reducer post reducer-function --response-type json 
wsk -i api create /mapper-function /mapper post mapper-function --response-type json