#!/bin/bash
pids=()

echo "现在进行本地测试 记得把util.h的RUN_REMOTE设为0 表示不进行etcd相关操作 所有服务器会直接运行在一个容器中 只通过端口进行区分"

echo "node-1"

NODE_ID=1 ../server -p 50051 &    
pids+=($!)
sleep 1

echo "node-2"

NODE_ID=2 ../server -p 50052 &    
pids+=($!)
sleep 1

echo "node-3"

NODE_ID=3 ../server -p 50053 &    
pids+=($!)
sleep 1

echo "balancer"

../balancer &
pids+=($!)

echo ${pids[@]} > pids.txt  # 把 PID 写入文件以便稍后使用