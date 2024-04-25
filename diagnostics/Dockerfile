FROM python:3.9

WORKDIR /app

# 将 Python 代码和依赖复制到镜像中
COPY diagnostics/simulation_train_ddp.py diagnostics/requirement.txt ./

# 安装 Python 依赖
RUN pip install -r requirement.txt