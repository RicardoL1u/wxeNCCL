import torch
import torch.distributed as dist
#应该是warmup

def main():
    # Check PyTorch compatibility
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

    if num_gpus > 0:
        # Initialize distributed process group
        dist.init_process_group(backend='nccl')

        rank = dist.get_rank()
        world_size = dist.get_world_size()

        local_tensor = torch.tensor([rank]).float().cuda()
        dist.all_reduce(local_tensor, op=dist.ReduceOp.SUM)

        # Gather results of all_reduce to all nodes
        gather_list = [torch.zeros_like(local_tensor) for _ in range(world_size)]
        dist.all_gather(gather_list, local_tensor)

        # Check if all_reduce results are consistent across all processes
        expected_value = sum(range(world_size))
        all_correct = all(tensor.item() == expected_value for tensor in gather_list)

        # Output the check result
        if all_correct:
            print("Checking distributed training............\033[32m[PASSED]\033[0m")
        else:
            print("Checking distributed training............\033[31m[FAILED]\033[0m")

        dist.destroy_process_group()

if __name__ == "__main__":
    main()
