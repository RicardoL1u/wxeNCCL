import os
import torch
import torch.distributed as dist
import sys
import time

def setup():
    # 获取环境变量
    world_size = int(os.environ['WORLD_SIZE'])
    s = os.environ['RANK']
    last_char = s[-1]  # 获取最后一个字符
    last_digit = int(last_char)  # 将字符转换为数字

    rank = last_digit
    master_addr = os.environ['MASTER_ADDR']
    master_port = os.environ['MASTER_PORT']

    # 初始化进程组
    dist.init_process_group(
        backend='gloo',  # 使用 gloo 后端
        init_method=f'tcp://{master_addr}:{master_port}',
        world_size=world_size,
        rank=rank
    )

def cleanup():
    # 销毁进程组
    dist.destroy_process_group()

def main():
    setup()

    # 获取当前进程的rank
    rank = dist.get_rank()

    # 创建一个简单的模型和优化器
    model = torch.nn.Linear(10, 10)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

    # 同步初始模型参数
    for param in model.parameters():
        dist.broadcast(param.data, src=0)

    # 执行50次迭代的分布式训练
    for iteration in range(100):
        # 前向传播
        outputs = model(torch.randn(20, 10))
        loss = torch.nn.functional.mse_loss(outputs, torch.randn(20, 10))

        # 反向传播和优化
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        # All-Reduce梯度
        for param in model.parameters():
            dist.all_reduce(param.grad.data, op=dist.ReduceOp.SUM)
            param.grad.data /= dist.get_world_size()

        # 打印当前进程的迭代信息
        if rank == 0:
            print(f"Iteration {iteration + 1}/{100}, Loss: {loss.item()}")
            sys.stdout.flush()  # 确保信息被即时输出

    cleanup()

if __name__ == '__main__':
    main()