import socket
import torch
import torch.distributed as dist


def setup(backend):
    """Initialize distributed environment based on environment variables."""
    dist.init_process_group(backend=backend)
    world_size = dist.get_world_size()
    rank = dist.get_rank()

    if backend == 'nccl':
        torch.cuda.set_device(rank % torch.cuda.device_count())


def main():
    setup('nccl')
    rank = dist.get_rank()

    hostname = socket.gethostname()
    max_length = 256
    # 将主机名转换为固定长度的 ASCII tensor
    ascii_values = [ord(c) for c in hostname] + [0] * (max_length - len(hostname))
    hostname_tensor = torch.tensor(ascii_values, dtype=torch.long, device='cuda')

    # 为所有进程的主机名创建一个列表
    gathered_hostnames = [torch.zeros(max_length, dtype=torch.long, device='cuda') for _ in range(dist.get_world_size())]

    # 使用 all_gather 收集所有进程的主机名
    dist.all_gather(gathered_hostnames, hostname_tensor)

    # 将 ASCII tensor 转换回字符串
    hostnames = [''.join(chr(i) for i in host if i != 0) for host in gathered_hostnames]

    if rank == 0:
        print("Gathered hostnames:", hostnames)

if __name__ == "__main__":
    main()