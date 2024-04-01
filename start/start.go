package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/nats-io/nats.go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type MasterProcess struct {
	nc *nats.Conn
}

func main() {
	// 定义命令行参数
	taskName := flag.String("taskName", "", "任务名称")
	taskWorker := flag.Int("taskWorker", 0, "任务工作者数量")

	// 解析命令行参数
	flag.Parse()
	natsServers := "nats://127.0.0.1:4222"

	// 检查是否提供了必需的参数
	if *taskName == "" {
		fmt.Println("请提供任务名称")
		return
	}

	if *taskWorker <= 0 {
		fmt.Println("任务工作者数量必须大于0")
		return
	}

	// 使用获取的参数值
	fmt.Printf("任务名称: %s\n", *taskName)
	fmt.Printf("任务工作者数量: %d\n", *taskWorker)

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}

	// 创建 Kubernetes 客户端
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// 创建 PVC
	err = createPVC(clientset, "default")
	if err != nil {
		log.Fatal(err)
	}

	replicas := int32(*taskWorker)

	// 创建 StatefulSet
	err = createTaskStatefulSet(clientset, *taskName, replicas, natsServers)
	if err != nil {
		log.Fatal(err)
	}

}

func createPVC(clientset *kubernetes.Clientset, namespace string) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "task-manager-pvc",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("200Mi"),
				},
			},
		},
	}

	_, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func createTaskStatefulSet(clientset *kubernetes.Clientset, taskName string, replicas int32, natsServers string) error {
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: taskName,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "ddp-master",
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": taskName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": taskName,
					},
				},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Name:  "init-setup",
							Image: "busybox",
							Command: []string{
								"sh",
								"-c",
								"cp /source/data/* /dest/data", //可修改
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "shared-volume",
									MountPath: "/dest/data", //可修改
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "main-app",
							Image: "fortypercent/ddp-training-cpu:v6.0",
							Command: []string{
								"/app/data/myapp", //可修改
							},
							Env: []corev1.EnvVar{
								{
									Name:  "NATS_SERVER",
									Value: natsServers,
								},
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
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
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "shared-volume",
									MountPath: "/app/data", //可修改
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "shared-volume",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "task-manager-pvc",
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := clientset.AppsV1().StatefulSets("default").Create(context.Background(), statefulSet, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	fmt.Println("StatefulSet created successfully")

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ddp-master",
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": taskName,
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

	return nil
}
