# Dockerfile for Python whisk docker action
FROM openwhisk/dockerskeleton

ENV FLASK_PROXY_PORT 8080

# Install our action's Python dependencies
ADD requirements.txt /action/requirements.txt
RUN apk add musl-dev; apk add linux-headers; apk add gcc;
RUN cd /action; pip install -r requirements.txt

# Ensure source assets are not drawn from the cache 
# after this date
ENV REFRESHED_AT 2016-09-05T13:59:39Z
# Add all source assets
ADD . /action
# Rename our executable Python action
ADD mapper-function.py /action/exec

# Leave CMD as is for Openwhisk
CMD ["/bin/bash", "-c", "cd actionProxy && python3 -u actionproxy.py"]