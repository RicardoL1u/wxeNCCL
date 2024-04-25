# 设置环境变量 
# 默认在RoCE场景下已经设置了NCCL_IB_HCA、NCCL_IB_GID_INDEX、NCCL_CROSS_NIC、NCCL_IB_TC环境变量，无需设置 
export FIRST_HCA=$(echo $NCCL_IB_HCA | awk -F',' '{print $1}')
export NCCL_IB_HCA=$(echo $NVIDIA_VISIBLE_DEVICES | awk -F"," '{if(NF<8){print ENVIRON["FIRST_HCA"]}else{print ENVIRON["NCCL_IB_HCA"]}}')
export NCCL_IB_DISABLE=${NCCL_IB_DISABLE:-0}
export NCCL_IB_TIMEOUT=${NCCL_IB_TIMEOUT:-2}
export NCCL_IB_RETRY_CNT=${NCCL_IB_RETRY_CNT:-7}
export NCCL_P2P_DISABLE=${NCCL_P2P_DISABLE:-0}

# 设置多线程数量
export OMP_NUM_THREADS=${OMP_NUM_THREADS:-4} #需动态配置
# export OMP_NUM_THREADS=4
export CUDA_DEVICE_MAX_CONNECTIONS=${CUDA_DEVICE_MAX_CONNECTIONS:-1} #PP

# 设置单POD的GPU数量
GPUS_PER_NODE=$(echo $NVIDIA_VISIBLE_DEVICES | awk -F"," '{print NF}')
# 设置分布式训练DDP所需的环境变量， 如果是单机场景，就默认赋值单机的参数 
MASTER_ADDR=${MASTER_ADDR:-'127.0.0.1'}
MASTER_PORT=${MASTER_PORT:-'29500'}
# NNODES=${WORLD_SIZE:-'1'}
NNODES=$2
NODE_RANK=${RANK:-'0'}
WORLD_SIZE=$(($GPUS_PER_NODE*$NNODES))

NODE_RANK=$3
MASTER_ADDR=$1
DISTRIBUTED_ARGS="
    --nproc_per_node $GPUS_PER_NODE \
    --nnodes $NNODES \
    --node_rank $NODE_RANK \
    --master_addr $MASTER_ADDR \
    --master_port $MASTER_PORT
"
echo "test"
echo $NODE_RANK
torchrun $DISTRIBUTED_ARGS benchmarkComm.py -b 128M -e 4G -f 2 -g $WORLD_SIZE