import os
import torch
import torch.distributed as dist
import time


def setup():
    # 获取环境变量
    world_size = int(os.environ['WORLD_SIZE'])
    rank_str = os.environ['RANK']  # 从环境变量获取 'RANK'
    rank_value = rank_str.split('-')[-1]  # 根据 '-' 分割并取最后一个值
    rank = int(rank_value)  # 将字符串转换为整数

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
    # 检查版本
    # 输出
    # 获取当前进程的rank
    rank = dist.get_rank()

    # 创建一个简单的张量
    tensor = torch.ones(1) * rank

    # 打印张量的值
    print(f"Rank {rank}: Initial tensor value: {tensor.item()}")
    time.sleep(30)
    # 执行All-Reduce操作
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)

    # 打印All-Reduce后的张量值
    print(f"Rank {rank}: Final tensor value after All-Reduce: {tensor.item()}")
    # 休眠30秒

    cleanup()

if __name__ == '__main__':
    main()
