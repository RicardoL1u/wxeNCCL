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
	mid := len(pods) / 2
	// 模拟: 打印当前正在测试的 pod 组
	fmt.Printf("Testing Pods from rank %d to %d\n", pods[0].Rank, pods[len(pods)-1].Rank)
	fmt.Println("----------------------------------------------")

	wg.Add(2)
	leftHalfCopy := make([]PodInfo, len(pods)/2)
	copy(leftHalfCopy, pods[mid:])

	rightHalfCopy := make([]PodInfo, len(pods)/2)
	copy(rightHalfCopy, pods[:mid])

	go testGroup(leftHalfCopy, wg, faults)
	go testGroup(rightHalfCopy, wg, faults)
}

func allReduce(group []PodInfo) (bool, error) {
	if len(group) == 0 {
		return false, errors.New("the group cannot be empty")
	}

	var normalPod PodInfo
	singleNodeTest := len(group) == 1
	minRank := group[0].Rank
	if singleNodeTest {
		group[0].testRank = strconv.Itoa(0)

		var ok bool
		normalPod, ok := <-knownGoodPods
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
			fmt.Println("Executing pod INFO: ", pod)
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			command := fmt.Sprintf("%s 'sudo bash /home/wuxuaner/wxe/launchBenchmark.sh %s %s %s'", pod.SSHCommand, masterAddr, NNodes, pod.testRank)
			fmt.Println("Executing SSH command: ", command)
			output, err := exec.CommandContext(ctx, "sh", "-c", command).CombinedOutput()
			if err != nil {
				log.Printf("Error executing SSH command: %s, error: %v\n", command, err)
				if ctx.Err() == context.DeadlineExceeded {
					log.Printf("SSH command timeout: %s\n", command)
					errChan <- fmt.Errorf("command timeout")
					kill := fmt.Sprintf("%s 'sudo pkill -f benchmarkComm.py'", pod.SSHCommand)
					killCmd := exec.Command("sh", "-c", kill)
					if killOutput, killErr := killCmd.CombinedOutput(); killErr != nil {
						log.Printf("Failed to kill process: %s, error: %v, output: %s\n", command, killErr, string(killOutput))
					} else {
						log.Printf("Process killed successfully after timeout: %s\n", command)
					}
				} else {
					log.Printf("Process killed successfully after timeout: %s\n", command)
					errChan <- fmt.Errorf("command timed out and process killed: %s", command)
				}
			} else {
				fmt.Printf("Output of command: %s\n%s\n", command, string(output))
			}
		}(pod)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return <-errChan // 返回第一个错误
	} else {
		fmt.Println("This group of pods has passed the allReduce test.")
		fmt.Println("----------------------------------------------")
		return nil
	}
}

func testGroup(group []PodInfo, wg *sync.WaitGroup, faults chan<- string) {
	defer wg.Done()
	if len(group) == 0 {
		return
	}
	success, err := allReduce(group)
	fmt.Println("Begin test", group)
	if !success {
		fmt.Println(len(group))
		if len(group) == 1 {
			faults <- group[0].IP
			log.Println("Error in allReduce:", err)
			return
		} else {
			log.Println("Error during allReduce:", err)
			binarySearchFaultyPods(group, wg, faults)
		}

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
		"10.200.17.76",
	}
	podInfos, err := getPodIPs(clientset, "default")

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

	wg := &sync.WaitGroup{}
	faults := make(chan string, len(podInfos))
	knownGoodPods = make(chan PodInfo, len(podInfos)) // Adjust size accordingly
	// 测试所有pods
	wg.Add(1)
	go testGroup(podInfos, wg, faults)
	wg.Wait()
	close(faults)
	fmt.Println("All pods have been tested. Faulty pods identification completed.")
	fmt.Println("Faulty pods:", len(faults))
	for fault := range faults {
		fmt.Printf("Faulty pod found: %s\n", fault)
	}
}
