package main

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// 加载 Kubernetes 配置
	config, err := clientcmd.BuildConfigFromFlags("", "path/to/kubeconfig")
	if err != nil {
		panic(err)
	}

	// 创建 Kubernetes 客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	// 定义 StatefulSet
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ddp-training",
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "ddp-master",
			Replicas:    int32Ptr(4),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "ddp-training",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "ddp-training",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "ddp-container",
							Image: "fortypercent/ddp-training-cpu:v6.0",
							Env: []corev1.EnvVar{
								{
									Name:  "WORLD_SIZE",
									Value: "4",
								},
								{
									Name: "RANK",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "metadata.name",
										},
									},
								},
								{
									Name:  "MASTER_ADDR",
									Value: "ddp-master",
								},
								{
									Name:  "MASTER_PORT",
									Value: "8888",
								},
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8888,
								},
							},
							Command: []string{"/bin/sh", "-c", "tail -f /dev/null & python train_ddp.py & wait"},
						},
					},
				},
			},
		},
	}

	// 创建 StatefulSet
	_, err = clientset.AppsV1().StatefulSets("default").Create(context.Background(), statefulSet, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Println("StatefulSet created successfully")

	// 定义 Service
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ddp-master",
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": "ddp-training",
			},
			Ports: []corev1.ServicePort{
				{
					Port:       8888,
					TargetPort: intstr.FromInt(8888),
				},
			},
		},
	}

	// 创建 Service
	_, err = clientset.CoreV1().Services("default").Create(context.Background(), service, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Println("Service created successfully")
}

func int32Ptr(i int32) *int32 {
	return &i
}
