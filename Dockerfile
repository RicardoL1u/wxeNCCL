# 第一阶段：构建 Go 环境
FROM alpine:latest

WORKDIR /app

# 先复制 go.mod 和 go.sum 并下载依赖
COPY go.mod go.sum ./

# 然后复制 Go 代码并构建应用
COPY workerPart/ ./

# 将 Python 代码和依赖复制到镜像中
COPY pythonFile/warmup.py ./