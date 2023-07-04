#!/bin/bash
pids=()

NODE_ID=1 ../server -p 50051 &    
pids+=($!)
sleep 1

NODE_ID=2 ../server -p 50052 &    
pids+=($!)
sleep 1

NODE_ID=3 ../server -p 50053 &    
pids+=($!)
sleep 1

../balancer &
pids+=($!)

echo ${pids[@]} > pids.txt  # 把 PID 写入文件以便稍后使用