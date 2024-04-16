package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
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
	IP   string
	Rank int
}

var (
	// Mutex to protect the known good pods list
	mutex sync.Mutex
	// List of known good pods
	knownGoodPods []PodInfo
)

func getKubernetesClient() (*kubernetes.Clientset, error) {
	kubeconfigPath := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}

func getPodIPs(clientset *kubernetes.Clientset, namespace string) ([]PodInfo, error) {
	podInfos := []PodInfo{}
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app=myApp", //是什么
	})
	if err != nil {
		return nil, err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning {
			rankStr, ok := pod.Labels["rank"]
			if !ok {
				continue
			}
			rank, err := strconv.Atoi(rankStr)
			if err != nil {
				continue
			}
			podInfos = append(podInfos, PodInfo{IP: pod.Status.PodIP, Rank: rank})
		}
	}
	sort.Slice(podInfos, func(i, j int) bool { return podInfos[i].Rank < podInfos[j].Rank })
	return podInfos, nil
}

func allReduce(group []PodInfo) (bool, error) {
	// Check if the group is empty
	if len(group) == 0 {
		return false, errors.New("the group cannot be empty")
	}

	// Initialize a default normalPod
	var normalPod PodInfo

	// Find the smallest rank
	singleNodeTest := len(group) == 1
	minRank := group[0].Rank

	if singleNodeTest {
		// 执行只有一个节点时的特殊逻辑
		group[0].Rank = 0
		normalPod = getNormalPod()
		normalPod.Rank = 1
	} else {
		// Adjust ranks based on the smallest rank
		for i := range group {
			group[i].Rank = group[i].Rank % minRank
		}
	}

	// Set environment variables and execute the command
	masterAddr := group[0].IP
	if err := os.Setenv("MASTER_ADDR", masterAddr); err != nil {
		return false, err
	}
	if err := os.Setenv("NNODES", strconv.Itoa(len(group))); err != nil {
		return false, err
	}

	cmd := exec.Command("/bin/sh", "launchBenchmark.sh")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return false, err
	}

	knownGoodPods = append(knownGoodPods, group...)

	// If it was a single node test, put back the normal pod
	if singleNodeTest {
		putBackNormalPod(normalPod)
	}

	return true, nil
}

func putBackNormalPod(pod PodInfo) {
	mutex.Lock() // Ensure thread safety
	defer mutex.Unlock()
	knownGoodPods = append(knownGoodPods, pod)
}

// 获取一个已知的正常节点并从列表中移除
func getNormalPod() PodInfo {
	if len(knownGoodPods) > 0 {
		pod := knownGoodPods[0]
		knownGoodPods = knownGoodPods[1:] // 移除已使用的节点
		return pod
	}
	panic("No normal nodes available") // 如果没有可用的正常节点，抛出错误
}
func testPodsWithNormalPods(pods []PodInfo, wg *sync.WaitGroup, faults chan<- string) {
	defer wg.Done()
	if len(knownGoodPods) < 2 {
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
	}(pods[1]) // Pass the second pod
}

func binarySearchFaultyPods(pods []PodInfo, wg *sync.WaitGroup, faults chan<- string) {
	defer wg.Done()

	if len(pods) <= 1 {
		return
	}

	if len(pods) == 2 {
		wg.Add(1)
		testPodsWithNormalPods(pods, wg, faults)
		return
	}

	group1, group2 := splitPodsByMedianRank(pods)
	wg.Add(2)
	go testGroup(group1, wg, faults)
	go testGroup(group2, wg, faults)
}

func splitPodsByMedianRank(podInfos []PodInfo) ([]PodInfo, []PodInfo) {
	if len(podInfos) < 2 {
		return podInfos, nil // Return directly if there is one or no pod
	}

	// Calculate the median rank
	medianRank := podInfos[0].Rank + (podInfos[len(podInfos)-1].Rank-podInfos[0].Rank)/2

	// Split groups based on median rank
	midpoint := 0
	for i, pod := range podInfos {
		if pod.Rank > medianRank {
			midpoint = i
			break
		}
	}

	return podInfos[:midpoint], podInfos[midpoint:]
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

func main() {
	rand.Seed(time.Now().UnixNano())

	clientset, err := getKubernetesClient()
	if err != nil {
		log.Fatal("Error getting Kubernetes client:", err)
	}

	podInfos, err := getPodIPs(clientset, "default")
	if err != nil {
		log.Fatal("Error getting pod IPs:", err)
	}

	var wg sync.WaitGroup
	faults := make(chan string, len(podInfos))

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
