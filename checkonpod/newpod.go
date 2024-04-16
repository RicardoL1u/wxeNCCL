package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	kubeconfig := flag.String("kubeconfig", os.Getenv("HOME")+"/.kube/config", "path to the kubeconfig file")
	taskName := flag.String("taskName", "example-task", "the name of the StatefulSet")
	taskWorker := flag.Int("taskWorker", 4, "number of workers in the StatefulSet")
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatalf("Error reading kubeconfig: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating Kubernetes client: %v", err)
	}

	// 创建 StatefulSet 配置
	st := createStatefulSet(*taskName, *taskWorker)
	stsClient := clientset.AppsV1().StatefulSets("default") // 假设在默认命名空间

	// 创建 StatefulSet
	result, err := stsClient.Create(context.TODO(), st, metav1.CreateOptions{})
	if err != nil {
		log.Fatalf("Failed to create StatefulSet: %v", err)
	}
	fmt.Printf("Created StatefulSet %q.\n", result.GetObjectMeta().GetName())
}

func createStatefulSet(name string, workers int) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(int32(workers)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "nginx",
							Image: "nginx:1.19.4",
							Ports: []corev1.ContainerPort{{ContainerPort: 80}},
						},
					},
				},
			},
		},
	}
}

func int32Ptr(i int32) *int32 {
	return &i
}
