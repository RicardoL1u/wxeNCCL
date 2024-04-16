package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	statuswatchclientset "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned"
	statuswatchv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
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
	natsServers := "nats://10.96.248.35:4222"

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
		log.Println(err)
	}

	replicas := int32(*taskWorker)

	err = createGRPCService(clientset, *taskName)
	if err != nil {
		log.Println(err)
	}

	err = createDDPMasterService(clientset, *taskName)
	if err != nil {
		log.Println(err)
	}

	err = createTaskStatefulSet(clientset, *taskName, replicas, natsServers)
	if err != nil {
		log.Fatal(err)
	}

	// 创建我们自定义资源的客户端集
	swClient, err := statuswatchclientset.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Error building example clientset: %s", err.Error())
	}
	fmt.Println("StatusWatch client created successfully")

	err = createStatusWatch(clientset, swClient, *taskName, replicas)
	if err != nil {
		log.Fatal(err)
	}
	go watchStatefulSetsAndPods(clientset, swClient, *taskName)

	// 设置信号通道以侦听中断（如 Ctrl+C）和终止（如 kubernetes 停止 pod）信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 阻塞等待信号
	sig := <-sigChan
	fmt.Printf("Received signal: %s, exiting program.\n", sig)
}

func watchStatefulSetsAndPods(kubeClient *kubernetes.Clientset, swClient *statuswatchclientset.Clientset, taskName string) {
	namespace := "default" // 根据需要调整命名空间

	// 监视特定名称的StatefulSet变化
	// stsWatcher, err := kubeClient.AppsV1().StatefulSets(namespace).Watch(context.TODO(), metav1.ListOptions{
	// 	LabelSelector: "app=" + taskName, // 根据你的需求调整标签选择器
	// })
	// if err != nil {
	// 	log.Fatalf("Failed to start watching StatefulSets: %v", err)
	// }
	// log.Printf("Started watching StatefulSets for task: %s", taskName)

	// 监视特定StatefulSet下的Pods变化
	podWatcher, err := kubeClient.CoreV1().Pods(namespace).Watch(context.TODO(), metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				"app": taskName,
			},
		}),
	})

	if err != nil {
		log.Fatalf("Failed to start watching Pods: %v", err)
	}
	log.Printf("Started watching Pods for task: %s", taskName)

	// // 处理StatefulSet事件
	// go func() {
	// 	for event := range stsWatcher.ResultChan() {
	// 		// 这里可以处理StatefulSet相关事件，如果需要
	// 	}
	// }()

	// 处理Pod事件
	go func() {
		for event := range podWatcher.ResultChan() {
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				log.Println("Unexpected type")
				continue
			}

			switch event.Type {
			case watch.Modified:
				log.Printf("Pod %s is modified: %s", pod.Name, taskName)

				// 此处假设每次Pod修改都需要更新CRD，实际使用时应该更加精细化控制
				// 找到与Pod关联的StatefulSet（此示例中假设Pod的标签与StatefulSet匹配）
				// setLabel := pod.GetLabels()
				// log.Printf("Pod %s has labels: %v", pod.Name, setLabel)
				statefulset, err := kubeClient.AppsV1().StatefulSets(namespace).Get(context.TODO(), taskName, metav1.GetOptions{})
				if err != nil {
					// 处理错误
					log.Printf("Failed to get StatefulSet: %v", err)
				}

				// 更新第一个匹配的StatefulSet对应的StatusWatch CRD，根据实际情况可能需要调整
				updateCRDForStatefulSet(kubeClient, swClient, statefulset, taskName)
			case watch.Deleted:
				log.Printf("Pod %s is deleted", pod.Name)
				// case watch.Added:
				// 	log.Printf("Pod %s is added", pod.Name)
			}
		}
	}()
}

func updateCRDForStatefulSet(kubeClient *kubernetes.Clientset, swClient *statuswatchclientset.Clientset, sts *appsv1.StatefulSet, taskName string) {
	// Assuming the namespace of StatusWatch and StatefulSet are the same and using "default" here. Adjust as necessary.
	namespace := "default"

	podList, err := kubeClient.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				"app": taskName,
			},
		}),
	})
	if err != nil {
		log.Printf("Failed to list Pods for StatefulSet %s: %v", sts.Name, err)
		return
	}

	// Prepare the updated Workers array based on the current Pods
	var workers []statuswatchv1.WorkerSpec
	for _, pod := range podList.Items {
		workers = append(workers, statuswatchv1.WorkerSpec{
			Name:    pod.Name,
			PodUUID: string(pod.ObjectMeta.UID),
		})
	}

	// Fetch the corresponding StatusWatch CRD instance
	statusWatchName := fmt.Sprintf("%s-statuswatch", taskName)
	statusWatch, err := swClient.StatuswatchV1().StatusWatches(namespace).Get(context.TODO(), statusWatchName, metav1.GetOptions{})
	if err != nil {
		log.Printf("Failed to get StatusWatch for task %s: %v", statusWatchName, err)
		return
	}

	// Update the StatusWatch instance with the new Workers array
	statusWatch.Spec.Workers = workers
	_, err = swClient.StatuswatchV1().StatusWatches(namespace).Update(context.TODO(), statusWatch, metav1.UpdateOptions{})
	if err != nil {
		log.Printf("Failed to update StatusWatch for task %s with new workers: %v", taskName, err)
	} else {
		log.Printf("StatusWatch %s updated with the current workers of StatefulSet %s\n", statusWatch.Name, sts.Name)
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

func createGRPCService(clientset *kubernetes.Clientset, taskName string) error {
	grpcService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "grpc-service",
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": taskName,
			},
			ClusterIP: "None", // 将 clusterIP 设置为 "None",使其成为 headless service
			Ports: []corev1.ServicePort{
				{
					Name: "grpc",
					Port: 8888,
				},
			},
		},
	}

	_, err := clientset.CoreV1().Services("default").Create(context.Background(), grpcService, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	fmt.Println("Headless service grpc-service created successfully")
	return nil
}

func createDDPMasterService(clientset *kubernetes.Clientset, taskName string) error {
	ddpMasterService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ddp-master",
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": taskName,
			},
			Ports: []corev1.ServicePort{
				{
					Port:       8889,
					TargetPort: intstr.FromInt(8889),
				},
			},
		},
	}

	_, err := clientset.CoreV1().Services("default").Create(context.Background(), ddpMasterService, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	fmt.Println("Service ddp-master created successfully")
	return nil
}

func createTaskStatefulSet(clientset *kubernetes.Clientset, taskName string, replicas int32, natsServers string) error {
	// 创建 ServiceAccount
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-service-account", taskName),
		},
	}
	_, err := clientset.CoreV1().ServiceAccounts("default").Create(context.Background(), serviceAccount, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			log.Printf("ServiceAccount %s already exists", serviceAccount.Name)
		} else {
			return fmt.Errorf("failed to create ServiceAccount: %v", err)
		}
	} else {
		log.Printf("ServiceAccount %s created successfully", serviceAccount.Name)
	}

	// 创建 ClusterRole
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-cluster-role", taskName),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch", "update", "patch"},
			},
		},
	}
	_, err = clientset.RbacV1().ClusterRoles().Create(context.Background(), clusterRole, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			log.Printf("ClusterRole %s already exists", clusterRole.Name)
		} else {
			return fmt.Errorf("failed to create ClusterRole: %v", err)
		}
	} else {
		log.Printf("ClusterRole %s created successfully", clusterRole.Name)
	}

	// 创建 ClusterRoleBinding
	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-cluster-role-binding", taskName),
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      serviceAccount.Name,
				Namespace: "default",
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     clusterRole.Name,
		},
	}
	_, err = clientset.RbacV1().ClusterRoleBindings().Create(context.Background(), clusterRoleBinding, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			log.Printf("ClusterRoleBinding %s already exists", clusterRoleBinding.Name)
		} else {
			return fmt.Errorf("failed to create ClusterRoleBinding: %v", err)
		}
	} else {
		log.Printf("ClusterRoleBinding %s created successfully", clusterRoleBinding.Name)
	}

	// 定义 StatefulSet 的配置
	statefulSetSpec := appsv1.StatefulSetSpec{
		ServiceName: "grpc-service",
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
				ServiceAccountName: serviceAccount.Name,
				InitContainers: []corev1.Container{
					{
						Name:  "init-setup",
						Image: "fortypercent/init:v0.1.8",
						Command: []string{
							"sh",
							"-c",
							"cp -r /app/* /dest/data",
						},
						ImagePullPolicy: corev1.PullIfNotPresent,
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "shared-volume",
								MountPath: "/dest/data",
							},
						},
					},
				},
				Containers: []corev1.Container{
					{
						Name:  "main-app",
						Image: "fortypercent/train:v0.0.1",
						Command: []string{
							"sh",
							"-c",
							"cd /app/data/ && ./worker-linux-arm",
						},
						ImagePullPolicy: corev1.PullIfNotPresent,
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
								Value: fmt.Sprintf("%d", replicas),
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
								Value: "8889",
							},
							{
								Name:  "GRPC_PORT",
								Value: "8888",
							},
						},
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: 8888,
							},
							{
								ContainerPort: 8889,
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "shared-volume",
								MountPath: "/app/data",
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
	}

	// 创建 StatefulSet
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: taskName,
		},
		Spec: statefulSetSpec,
	}
	_, err = clientset.AppsV1().StatefulSets("default").Create(context.Background(), statefulSet, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create StatefulSet: %v", err)
	}
	log.Printf("StatefulSet %s created successfully", statefulSet.Name)

	return nil
}

func createTaskResources(clientset *kubernetes.Clientset, taskName string, replicas int32, natsServers string) error {
	err := createGRPCService(clientset, taskName)
	if err != nil {
		return fmt.Errorf("failed to create GRPCService: %v", err)
	}

	err = createDDPMasterService(clientset, taskName)
	if err != nil {
		return err
	}

	err = createTaskStatefulSet(clientset, taskName, replicas, natsServers)
	if err != nil {
		return fmt.Errorf("failed to create DDPMasterService: %v", err)
	}

	return nil
}

func createStatusWatch(clientset *kubernetes.Clientset, statuswatchClientset *statuswatchclientset.Clientset, taskName string, replicas int32) error {
	service, err := clientset.CoreV1().Services("default").Get(context.Background(), "grpc-service", metav1.GetOptions{})
	if err != nil {
		return err
	}

	statusWatch := &statuswatchv1.StatusWatch{
		ObjectMeta: metav1.ObjectMeta{
			Name: taskName + "-statuswatch",
		},
		Spec: statuswatchv1.StatusWatchSpec{
			Number:    int(replicas),
			ServiceIP: service.Spec.ClusterIP,
			Workers:   []statuswatchv1.WorkerSpec{},
		},
	}

	maxRetries := 10
	retryDelay := 5 * time.Second

	// 为每个 Pod 添加对应的 WorkerSpec
	for i := 0; i < int(replicas); i++ {
		podName := fmt.Sprintf("%s-%d", taskName, i)
		fmt.Println("Pod Name: ", podName)

		var pod *v1.Pod
		var err error

		for retry := 0; retry < maxRetries; retry++ {
			pod, err = clientset.CoreV1().Pods("default").Get(context.Background(), podName, metav1.GetOptions{})
			if err == nil {
				break
			}

			fmt.Printf("Failed to get pod %s, retrying in %v...\n", podName, retryDelay)
			time.Sleep(retryDelay)
		}

		if err != nil {
			return fmt.Errorf("failed to get pod %s after %d retries: %v", podName, maxRetries, err)
		}

		// 打印获取到的 Pod 信息
		// fmt.Printf("Got Pod: %+v\n", pod)

		statusWatch.Spec.Workers = append(statusWatch.Spec.Workers, statuswatchv1.WorkerSpec{
			PodUUID: string(pod.ObjectMeta.UID),
			Name:    podName,
		})
	}

	_, err = statuswatchClientset.StatuswatchV1().StatusWatches("default").Create(context.Background(), statusWatch, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	fmt.Println("StatusWatch created successfully")
	return nil
}
