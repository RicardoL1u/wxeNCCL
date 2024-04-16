# 使用官方Python镜像作为基础镜像
FROM python:3.9

# 设置工作目录
WORKDIR /app

# 安装Go
# 首先下载Go的指定版本
# 这里以Go 1.22.1为例，可以根据需要调整版本号
ENV GO_VERSION 1.22.1
RUN curl -OL https://golang.org/dl/go${GO_VERSION}.linux-arm64.tar.gz \
    && tar -C /usr/local -xzf go${GO_VERSION}.linux-arm64.tar.gz \
    && rm go${GO_VERSION}.linux-arm64.tar.gz

# 将Go二进制添加到PATH，这样可以在任何地方运行Go命令
ENV PATH="/usr/local/go/bin:${PATH}"


# 将 Python 代码和依赖复制到镜像中
COPY pythonFile/train_ddp.py pythonFile/requirement.txt ./

# 安装 Python 依赖
RUN pip install -r requirement.txt