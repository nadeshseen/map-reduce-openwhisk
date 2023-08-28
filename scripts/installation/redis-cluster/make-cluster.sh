ssh redis1@10.129.28.57 \
'
echo yes | redis-cli --cluster create 10.129.28.57:7000 10.129.28.101:7002 10.129.27.40:7004 --cluster-replicas 0
'

# cluster-info
# redis-cli --cluster call 192.168.122.77:7000 info