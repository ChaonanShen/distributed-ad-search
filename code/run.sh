#!/bin/bash

sleep 5

# 根据NODE_ID的值作不同的事
if [ "$NODE_ID" == "1" ]; then
  echo "this is NODE-1"
  ./alimama/balancer &
  ./alimama/server -p 50051 
elif [ "$NODE_ID" == "2" ]; then
  echo "this is NODE-2"
  ./alimama/server -p 50052
elif [ "$NODE_ID" == "3" ]; then
  echo "this is NODE-3"
  ./alimama/server -p 50053
else
  echo "NODE_ID is not 1, 2, or 3"
fi

