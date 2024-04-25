import argparse
import os
import subprocess
import sys
import torch
import torch.distributed as dist
import time

def parse_size(size_str):
    """Parse memory size string like '128M' or '1G' into bytes."""
    units = {'K': 1024, 'M': 1024**2, 'G': 1024**3}
    if size_str[-1].upper() not in units:
        raise ValueError(f"Invalid size unit in '{size_str}'. Expected one of {list(units.keys())}.")
    unit = size_str[-1].upper()
    number = float(size_str[:-1])
    return int(number * units[unit])

def setup(backend, gpus_per_node):
    """Initialize distributed environment based on environment variables."""
    if not gpus_per_node:
        raise ValueError("GPU count per node is required.")
    dist.init_process_group(
        backend=backend,
    )
    world_size = dist.get_world_size()
    rank = dist.get_rank()

    if backend == 'nccl':
        torch.cuda.set_device(rank % torch.cuda.device_count())

def allgather_run(cmd):
    """Execute a command and gather outputs across all processes."""
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = result.stdout.decode('utf-8')
    outputs = [None for _ in range(dist.get_world_size())]
    dist.all_gather_object(outputs, output)
    return outputs

def allequal(iterator):
    """Check if all process outputs are identical."""
    iterator = iter(iterator)
    try:
        first = next(iterator)
    except StopIteration:
        return True
    return all(first == rest for rest in iterator)

def benchmark_nccl_communication(begin_size, end_size, factor, gpus_node, num_tests=10):
    """Conduct NCCL communication performance tests."""
    rank = dist.get_rank()
    if rank == 0:
        print(f"NCCL communication benchmark from {begin_size} to {end_size} by {factor}x factor.")
    size = parse_size(begin_size)
    end_size = parse_size(end_size)
    #在这之前做一下warmup, 做5轮的all_reduce
    tensor = torch.rand(size // (torch.finfo(torch.float32).bits // 8), device='cuda')
    for _ in range(5):
        dist.all_reduce(tensor)
        dist.barrier()  # synchronize all processes
    
    while size <= end_size:
        
        num_elements = size // (torch.finfo(torch.float32).bits // 8)


        tensor = torch.rand(num_elements, device='cuda')
        # tensor_copy = tensor.clone()
        # gathered_tensors = [torch.zeros_like(tensor_copy) for _ in range(dist.get_world_size())]
        dist.barrier()
        start_time = time.time()
        
        for _ in range(num_tests):
            dist.all_reduce(tensor)
        dist.barrier()   
        duration = (time.time() - start_time) / num_tests
        # torch.cuda.empty_cache()
        # start_time_gather = time.time()
        # for _ in range(num_tests):
        #     dist.all_gather(gathered_tensors, tensor_copy)

        # dist.barrier()
        # elapsed_time = (time.time() - start_time_gather)/num_tests
        # if rank == 0:
        #     print(f"Elapsed time: {elapsed_time} seconds")
        
        torch.cuda.empty_cache()
        
        # if rank == 0:
        #     print(f"Duration: {duration:.6f}s")
        # 每个节点的数据传输量为2 * N * S（其中N是节点数，S是数据大小）
        # total_data_per_node = 2 * dist.get_world_size() * size
        # 计算算法带宽（单位转换为Gigabytes per second）
        # algbw_gather = (size*(gpus_node-1) / elapsed_time) / 1e9
        # if rank == 0:
        #     print(f"all_gather: Size: {size} bytes, Duration: {elapsed_time:.6f}s, Algbw: {algbw_gather:.2f} GB/s")
        algbw = (size / duration) / 1e9 #计算方式存疑
        busbw = algbw * (2 * (gpus_node - 1) / gpus_node)
        if rank == 0:
            
            print(f"all_reduce: Size: {size} bytes, Duration: {duration:.6f}s, Algbw: {algbw:.2f} GB/s, Busbw: {busbw:.2f} GB/s")
        size *= factor

def cleanup():
    dist.destroy_process_group()
        
def main():
    parser = argparse.ArgumentParser(description="Simulate nccl-test for distributed communication performance testing.")
    parser.add_argument("-b", "--begin-size", default="8", help="Starting data size.")
    parser.add_argument("-e", "--end-size", default="128M", help="Ending data size.")
    parser.add_argument("-f", "--factor", type=int, default=2, help="Data size growth factor.")
    parser.add_argument("-g", "--gpus", type=int, required=True, help="GPUs per node (ignored if using mpirun).")
    args = parser.parse_args()

    print(os.getenv('MASTER_ADDR', 'default-address'))
    try:
        torch_version = torch.__version__
        print(f"Checking PyTorch compatibility: ...........  v{torch_version} [\033[32mPASSED\033[0m]")
    except Exception as e:
        print("Checking PyTorch compatibility: ...........  [\033[31mFAILED\033[0m]")
        return

    # Check CUDA availability if PyTorch compatibility is passed
    if torch.cuda.is_available():
        print("CUDA is available: ...........  [\033[32mPASSED\033[0m]")
    else:
        print("CUDA is available: ...........  [\033[31mFAILED\033[0m]")
        return

    # Check CUDA version supported by the PyTorch version
    try:
        cuda_version = torch.version.cuda
        print(f"CUDA version supported by PyTorch: ...........  {cuda_version} [\033[32mPASSED\033[0m]")
    except Exception as e:
        print("CUDA version supported by PyTorch: ...........  [\033[31mFAILED\033[0m]")
        return


    # Check number of available GPUs
    try:
        num_gpus = torch.cuda.device_count()
        print(f"Number of available GPUs: ...........  {num_gpus} [\033[32mPASSED\033[0m]")
    except Exception as e:
        print("Number of available GPUs: ...........  [\033[31mFAILED\033[0m]")
        return

    # Check GPU type
    try:
        gpu_name = torch.cuda.get_device_name()
        print(f"GPU type: ...........  {gpu_name} [\033[32mPASSED\033[0m]")
    except Exception as e:
        print("GPU type: ...........  [\033[31mFAILED\033[0m]")
        return

    # Check NCCL version
    try:
        nccl_version = torch.cuda.nccl.version()
        print(f"NCCL version: ...........  {nccl_version} [\033[32mPASSED\033[0m]")
    except Exception as e:
        print("NCCL version: ...........  [\033[31mFAILED\033[0m]")
        return

    setup(backend='nccl', gpus_per_node=args.gpus)
    print(f"Rank: {dist.get_rank()}, World size: {dist.get_world_size()}")
    print(f"Using GPUs: {torch.cuda.current_device()}")
    # print(args.gpus)
    # outputs = allgather_run("nvidia-smi topo -m")
    # if not allequal(outputs):
    #     print('Output of "nvidia-smi topo -m" differs between machines')
    #     sys.exit(1)

    if dist.get_rank() == 0:
        print("-----------------------------------")
        print("PyTorch Distributed Benchmark Suite")
        print("-----------------------------------")
        print(f"* PyTorch version: {torch.__version__}")
        print(f"* CUDA version: {torch.version.cuda}")
        # print("--- nvidia-smi topo -m ---")
        # print(outputs[0])
        print("--------------------------")

    benchmark_nccl_communication(args.begin_size, args.end_size, args.factor,args.gpus)
    cleanup()
if __name__ == "__main__":
    main()
