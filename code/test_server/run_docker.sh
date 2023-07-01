IMAGE=public-images-registry.cn-hangzhou.cr.aliyuncs.com/public/alimama-2023:B-v1

# 映射目录目录 - 奇怪，在自己服务器上是把项目目录映射过去，在hpf这是把docker/目录映射过去
docker run -it -v /home/hpf/scn:/scn --cap-add=SYS_PTRACE --security-opt seccomp=unconfined --name test-server ${IMAGE}

# 连接上这个运行的容器
# docker exec -it <container_name_or_id> bash
