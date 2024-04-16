#!/bin/bash

# 初始化GPU列表，这里假设有8个GPU编号从0到7
gpus=(0 1 2 3 4 5 6 7)

# nccl-tests 的路径
nccl_tests_path="/path/to/nccl-tests/build"

# 执行NCCL测试的函数
run_nccl_test() {
    local -n gpus=$1
    echo "Running NCCL test on GPUs: ${gpus[*]}"
    CUDA_VISIBLE_DEVICES=$(IFS=,; echo "${gpus[*]}")
    mpirun -np ${#gpus[@]} -x CUDA_VISIBLE_DEVICES $nccl_tests_path/all_reduce_perf -b 8 -e 128M -f 2 -g 1
}

# 二分法测试的函数
binary_search_test() {
    local -a gpus=("$@")
    
    if [ ${#gpus[@]} -le 1 ]; then
        echo "Issue found with GPU or connection: ${gpus[*]}"
        return
    fi

    mid=$(( ${#gpus[@]} / 2 ))
    left=("${gpus[@]:0:mid}")
    right=("${gpus[@]:mid}")

    echo "Testing left half: ${left[*]}"
    run_nccl_test left
    
    # 基于测试结果，决定是否继续在左侧子集测试
    # 这里需要手动判断，或者根据输出自动化判断
    read -p "Continue testing on left half? (y/n): " answer
    if [ "$answer" == "y" ]; then
        binary_search_test "${left[@]}"
    else
        echo "Testing right half: ${right[*]}"
        binary_search_test "${right[@]}"
    fi
}

# 开始执行二分法测试
binary_search_test "${gpus[@]}"
