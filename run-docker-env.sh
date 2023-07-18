# 开启docker环境编写代码

IMAGE=public-images-registry.cn-hangzhou.cr.aliyuncs.com/public/alimama-2023:B-v1

docker run -it -v ./:/work  -w /work --name test_alimama ${IMAGE} bash

# 连接上这个运行的容器
# docker exec -it <container_name_or_id> bash