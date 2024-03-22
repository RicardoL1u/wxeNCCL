# 使用基础的Python镜像
FROM python:3.9
# 设置工作目录
WORKDIR /app

# 复制训练代码和依赖文件
COPY train_ddp.py /app/
COPY requirement.txt /app/

# 安装PyTorch和其他依赖项
RUN pip install --no-cache-dir -r requirement.txt

CMD ["tail", "-f", "/dev/null"]