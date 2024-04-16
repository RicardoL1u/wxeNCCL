package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// 假设集群中有128个节点
var nodes = make([]int, 128)

// 随机设置10个故障节点
var faultyNodes = make(map[int]bool)

// 维护一个已知正常节点的列表
var knownGoodNodes = []int{}
var mutex sync.Mutex

// 初始化故障节点
func initFaultyNodes(numNodes, numFaults int) {
	nodes := rand.Perm(numNodes)
	for i := 0; i < numFaults; i++ {
		faultyNodes[nodes[i]] = true
	}
}

// 模拟AllReduce操作，返回是否成功
func allReduce(group []int) bool {
	for _, node := range group {
		if faultyNodes[node] { // 检查是否是故障节点
			return false
		}
	}
	// 如果allReduce成功，更新已知的正常节点列表
	for _, node := range group {
		if !contains(knownGoodNodes, node) {
			knownGoodNodes = append(knownGoodNodes, node)
		}
	}
	return true
}

// 检查切片中是否包含某个元素
func contains(slice []int, item int) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// 二分查找故障节点
func binarySearchFaultyNodes(start, end int, nodes []int, wg *sync.WaitGroup, faults chan<- int) {
	defer wg.Done()

	if start >= end {
		return
	}

	if end-start == 1 { // 只剩两个节点时的特殊处理
		testNodesWithNormalNodes(start, end, nodes, wg, faults)
		return
	}

	mid := (start + end) / 2
	group1 := nodes[start : mid+1]
	group2 := nodes[mid+1 : end+1]

	wg.Add(2)
	go testGroup(group1, start, mid, nodes, wg, faults)
	go testGroup(group2, mid+1, end, nodes, wg, faults)
}

// 测试一个组是否含有故障节点
func testGroup(group []int, start, end int, nodes []int, wg *sync.WaitGroup, faults chan<- int) {
	if !allReduce(group) { // 如果测试失败
		binarySearchFaultyNodes(start, end, nodes, wg, faults)
	} else {
		wg.Done()
	}
}

func testNodesWithNormalNodes(start, end int, nodes []int, wg *sync.WaitGroup, faults chan<- int) {
	if len(knownGoodNodes) < 2 {
		panic("Not enough normal nodes available for testing")
	}

	wg.Add(2) // 准备启动两个goroutine

	// 为每个测试分配一个不同的正常节点
	normalNode1 := getNormalNode()
	normalNode2 := getNormalNode()

	go func() {
		defer wg.Done()
		if !allReduce([]int{nodes[start], normalNode1}) {
			faults <- nodes[start]
		}
		putBackNormalNode(normalNode1) // 测试后立即放回节点
	}()

	go func() {
		defer wg.Done()
		if !allReduce([]int{nodes[end], normalNode2}) {
			faults <- nodes[end]
		}
		putBackNormalNode(normalNode2) // 测试后立即放回节点
	}()
}

func putBackNormalNode(node int) {
	mutex.Lock() // 确保线程安全
	defer mutex.Unlock()
	knownGoodNodes = append(knownGoodNodes, node)
}

// 获取一个已知的正常节点并从列表中移除
func getNormalNode() int {
	if len(knownGoodNodes) > 0 {
		node := knownGoodNodes[0]
		knownGoodNodes = knownGoodNodes[1:] // 移除已使用的节点
		return node
	}
	panic("No normal nodes available") // 如果没有可用的正常节点，抛出错误
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// 初始化节点编号
	for i := range nodes {
		nodes[i] = i
	}

	// 随机指定10个故障节点，确保不重复
	initFaultyNodes(len(nodes), 10)

	fmt.Println("Faulty nodes:", faultyNodes)

	var wg sync.WaitGroup
	faults := make(chan int, 10)

	wg.Add(1)
	go binarySearchFaultyNodes(0, len(nodes)-1, nodes, &wg, faults)

	go func() {
		wg.Wait()
		close(faults)
		fmt.Println("All nodes have been tested. Faulty nodes identification completed.")
	}()

	// 收集并打印找到的故障节点
	for fault := range faults {
		fmt.Printf("Faulty node found: %d\n", fault)
	}
}
