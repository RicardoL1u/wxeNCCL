package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

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
	podInfos := []PodInfo{}
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app=myApp",
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

// 二分法检测故障 Pods
func binarySearchFaultyPods(pods []PodInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	if len(pods) <= 1 {
		return
	}

	mid := len(pods) / 2
	wg.Add(2)
	go binarySearchFaultyPods(pods[:mid], wg)
	go binarySearchFaultyPods(pods[mid:], wg)

	// 这里可以加入实际的 allReduce 测试逻辑
	// 模拟: 打印当前正在测试的 pod 组
	fmt.Printf("Testing Pods from rank %d to %d\n", pods[0].Rank, pods[len(pods)-1].Rank)
}

func main() {
	clientset, err := getKubernetesClient()
	if err != nil {
		log.Fatalf("Error getting Kubernetes client: %v", err)
	}

	podInfos, err := getPodIPs(clientset, "default")
	if err != nil {
		log.Fatalf("Error getting pod IPs: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go binarySearchFaultyPods(podInfos, &wg)
	wg.Wait()

	fmt.Println("Faulty pod identification completed.")
}
