package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type PodInfo struct {
	IP         string
	Rank       int
	testRank   string
	SSHCommand string
	gpuIP      string
}

var (
	// Mutex to protect the known good pods list
	mutex sync.Mutex
	// Known good pods are now handled via a channel
	knownGoodPods chan PodInfo
)

// 获取 Kubernetes 客户端
func getKubernetesClient() (*kubernetes.Clientset, error) {
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}

// 获取 Pod IPs
func getPodIPs(clientset *kubernetes.Clientset, namespace string) ([]PodInfo, error) {
	fmt.Print("Getting pod IPs\n")
	podInfos := []PodInfo{}
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app=example-task",
	})
	if err != nil {
		return nil, err
	}

	for i, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning {
			podInfos = append(podInfos, PodInfo{IP: pod.Status.PodIP, Rank: i})
		}
	}
	sort.Slice(podInfos, func(i, j int) bool { return podInfos[i].Rank < podInfos[j].Rank })
	return podInfos, nil
}

// 二分法检测故障 Pods
func binarySearchFaultyPods(pods []PodInfo, wg *sync.WaitGroup, faults chan<- string) {
	defer wg.Done()
	if len(pods) <= 1 {
		if len(pods) == 1 {
			wg.Add(1)
			testPodsWithNormalPods(pods, wg, faults)
			return
		}
	}
	// wg.Add(1)
	// go testGroup(pods, wg, faults)

	mid := len(pods) / 2

	wg.Add(2)
	go testGroup(pods[:mid], wg, faults)
	go testGroup(pods[mid:], wg, faults)

	// 这里可以加入实际的 allReduce 测试逻辑
	// 模拟: 打印当前正在测试的 pod 组
	fmt.Printf("Testing Pods from rank %d to %d\n", pods[0].Rank, pods[len(pods)-1].Rank)
}

func allReduce(group []PodInfo) (bool, error) {
	if len(group) == 0 {
		return false, errors.New("the group cannot be empty")
	}

	var normalPod PodInfo
	singleNodeTest := len(group) == 1
	minRank := group[0].Rank

	if singleNodeTest {
		var ok bool
		normalPod, ok = <-knownGoodPods
		if !ok {
			return false, errors.New("failed to retrieve a normal pod from the channel")
		}
		normalPod.testRank = strconv.Itoa(1)
		group = append(group, normalPod) // 将 normalPod 加入到 group 中
	} else {
		if minRank != 0 {
			for i := range group {
				group[i].testRank = strconv.Itoa(group[i].Rank % minRank)
			}
		} else {
			for i := range group {
				group[i].testRank = strconv.Itoa(group[i].Rank)
			}
		}

	}

	masterAddr := group[0].gpuIP
	NNodes := strconv.Itoa(len(group))
	fmt.Println("AllReduce: ", group, masterAddr, NNodes)
	if err := executeSSHCommands(group, masterAddr, NNodes); err != nil {
		if singleNodeTest {
			putBackNormalPod(normalPod) // 单节点测试失败，将 normalPod 放回
		}
		return false, err
	}

	for _, pod := range group {
		select {
		case knownGoodPods <- pod:
		default:
			log.Println("Warning: knownGoodPods channel is full or not accepting new entries.")
		}
	}

	if singleNodeTest {
		putBackNormalPod(normalPod)
	}

	return true, nil
}

func executeSSHCommands(group []PodInfo, masterAddr, NNodes string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(group))

	for _, pod := range group {
		wg.Add(1)

		go func(pod PodInfo) {
			defer wg.Done()

			// 设置执行命令的超时时间为 1 分钟
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()

			command := fmt.Sprintf("%s 'sudo bash --login /home/wuxuaner/wxe/launchBenchmark.sh %s %s %s'", pod.SSHCommand, masterAddr, NNodes, pod.testRank)
			fmt.Printf("Executing command: %s, %v, %v\n", command, pod.Rank, pod.testRank)
			fmt.Println("----------------------")

			// 使用 context 控制超时
			cmd := exec.CommandContext(ctx, "sh", "-c", command)
			output, err := cmd.CombinedOutput()

			if err != nil {
				if ctx.Err() == context.DeadlineExceeded {
					log.Printf("SSH command timeout: %s\n", command)
					errChan <- fmt.Errorf("command timeout")
				} else {
					log.Printf("Error executing SSH command: %s, error: %v\n", command, err)
					errChan <- err
				}
				return
			}

			fmt.Printf("Output: %s\n", string(output))
		}(pod)
	}

	fmt.Println("Waiting for SSH commands to complete")
	wg.Wait()
	close(errChan)

	// 检查并返回第一个错误
	for err := range errChan {
		if err != nil {
			fmt.Println("Error in executeSSHCommands:", err)
			return err
		}
	}

	fmt.Println("All SSH commands have completed successfully")
	return nil
}

func splitPodsByMedian(pods []PodInfo) ([]PodInfo, []PodInfo) {
	mid := len(pods) / 2
	return pods[:mid], pods[mid:]
}

func testGroup(group []PodInfo, wg *sync.WaitGroup, faults chan<- string) {
	defer wg.Done()
	success, err := allReduce(group)
	if err != nil {
		log.Println("Error during allReduce:", err)
		return
	}
	if !success {
		wg.Add(1)
		go binarySearchFaultyPods(group, wg, faults)
	}
}

func putBackNormalPod(pod PodInfo) {
	select {
	case knownGoodPods <- pod:
		// Pod successfully returned to the channel
	default:
		// If the channel is full, handle appropriately, possibly logging or handling the error
		log.Println("Failed to return normal pod to the channel: Channel is full")
	}
}

func testPodsWithNormalPods(pods []PodInfo, wg *sync.WaitGroup, faults chan<- string) {
	defer wg.Done()
	if len(knownGoodPods) == 0 {
		log.Fatal("Not enough known good pods for testing")
	}

	wg.Add(2) // Launch a goroutine for each Pod test

	go func(pod PodInfo) {
		defer wg.Done()
		success, err := allReduce([]PodInfo{pod})
		if err != nil {
			log.Println("Error in allReduce:", err)
			return
		}
		if !success {
			faults <- pod.IP
		}
	}(pods[0]) // Pass the first pod
}

func main() {
	clientset, err := getKubernetesClient()
	if err != nil {
		log.Fatalf("Error getting Kubernetes client: %v", err)
	}

	sshCommands := []string{
		"ssh -p 40219 wuxuaner@61.135.204.120",
		"ssh -p 42700 wuxuaner@61.135.204.120",
		"ssh -p 42226 wuxuaner@61.135.204.120",
		"ssh -p 42067 wuxuaner@61.135.204.120",
	}

	gpuIPs := []string{
		"10.200.34.183",
		"10.200.28.147",
		"10.200.17.10",
		"10.200.17.10",
	}
	podInfos, err := getPodIPs(clientset, "default")
	fmt.Print(podInfos)
	if err != nil {
		log.Fatalf("Error getting pod IPs: %v", err)
	}

	// 假设你已经有某种方式确定了每个 Pod 对应哪个 SSH 命令
	for i, pod := range podInfos {
		if i < len(sshCommands) {
			pod.SSHCommand = sshCommands[i]
			pod.gpuIP = gpuIPs[i]
			podInfos[i] = pod // 确保更新了 slice 中的元素
		}
	}

	var wg sync.WaitGroup
	faults := make(chan string, len(podInfos))
	knownGoodPods = make(chan PodInfo, len(podInfos)) // Adjust size accordingly
	wg.Add(1)
	go binarySearchFaultyPods(podInfos, &wg, faults)
	go func() {
		wg.Wait()
		close(faults)
		fmt.Println("All pods have been tested. Faulty pods identification completed.")
	}()

	for fault := range faults {
		fmt.Printf("Faulty pod found: %s\n", fault)
	}
}
