#!/bin/bash
if [ -f pids.txt ]; then
    pids=$(cat pids.txt)
    for pid in ${pids[@]}; do
      kill $pid 
    done
    rm pids.txt  # 删除文件
    rm savedFile*
else
    echo "No PIDs file found."
fi