package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	statuswatchclientset "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned"
	statuswatchv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
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

	// 创建 dynamic client
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// 创建 PVC
	err = createPVC(clientset, "kubeflow")
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

	// podName := "example-init-only-pod"
	// namespace := "kubeflow"
	// err = createInitOnlyPod(clientset, podName, namespace)
	// if err != nil {
	// 	log.Fatalf("Error creating init-only Pod: %s", err)
	// }

	// 创建 PyTorchJob
	err = createPyTorchJob(clientset, dynamicClient, *taskName, replicas, natsServers)
	if err != nil {
		log.Fatal(err)
	}

	// 等待 PyTorchJob 变为运行状态
	err = waitForPyTorchJobRunning(clientset, "kubeflow", *taskName, 10*time.Minute)
	if err != nil {
		log.Fatalf("Failed to wait for PyTorchJob to be running: %v", err)
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

func waitForPyTorchJobRunning(clientset *kubernetes.Clientset, namespace, jobName string, timeout time.Duration) error {
	// 定义一个等待超时的计时器
	fmt.Println("Waiting for PyTorchJob to be running...")
	timeoutChan := time.After(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutChan:
			return fmt.Errorf("timeout while waiting for PyTorchJob %s to be running", jobName)
		case <-ticker.C:
			pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=%s", jobName), // 根据实际的 label selector 调整
			})
			if err != nil {
				fmt.Println(err)
				continue
			}
			allRunning := true
			for _, pod := range pods.Items {
				if pod.Status.Phase != corev1.PodRunning {
					allRunning = false
					break
				}
			}

			if allRunning {
				return nil // 所有 Pod 都处于 Running 状态
			}
		}
	}
}

func watchStatefulSetsAndPods(kubeClient *kubernetes.Clientset, swClient *statuswatchclientset.Clientset, taskName string) {
	namespace := "kubeflow" // 根据需要调整命名空间

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
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("Failed to load kubeconfig: %v", err)
	}
	// 处理Pod事件

	//todo: 为了简化，这里只处理Pod的修改事件，实际情况可能需要处理更多事件
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

				// 此处假设每次Pod修改都需要更新CRD，这里要改
				// 找到与Pod关联的PytorchJob（此示例中假设Pod的标签与PytorchJob匹配）
				// setLabel := pod.GetLabels()
				// log.Printf("Pod %s has labels: %v", pod.Name, setLabel)
				// 创建动态客户端
				dynamicClient, err := dynamic.NewForConfig(config)
				if err != nil {
					log.Fatalf("Failed to create dynamic client: %v", err)
				}
				gvr := schema.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "pytorchjobs"}

				// 获取 PyTorchJob
				namespace := "kubeflow"
				jobName := "test"
				pytorchJob, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
				if err != nil {
					log.Printf("Failed to get PyTorchJob: %v", err)
					return
				}

				// fmt.Printf("Got PyTorchJob: %s\n", pytorchJob)

				// 更新第一个匹配的StatefulSet对应的StatusWatch CRD，根据实际情况可能需要调整
				updateCRDForPytorchJob(kubeClient, swClient, pytorchJob, taskName)
			case watch.Deleted:
				log.Printf("Pod %s is deleted", pod.Name)
				// case watch.Added:
				// 	log.Printf("Pod %s is added", pod.Name)
			}
		}
	}()
}

func updateCRDForPytorchJob(kubeClient *kubernetes.Clientset, swClient *statuswatchclientset.Clientset, pytorchJob *unstructured.Unstructured, taskName string) {
	// 使用 "kubeflow" 命名空间，根据实际情况调整
	namespace := "kubeflow"

	// 列出与任务名相关的所有 Pods
	podList, err := kubeClient.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				"app": taskName,
			},
		}),
	})
	if err != nil {
		log.Printf("Failed to list Pods for task %s: %v", taskName, err)
		return
	}

	// 根据当前 Pod 列表准备更新的 Workers 数组
	var workers []statuswatchv1.WorkerSpec
	for _, pod := range podList.Items {
		workers = append(workers, statuswatchv1.WorkerSpec{
			Name:    pod.Name,
			PodUUID: string(pod.ObjectMeta.UID),
		})
	}

	// 获取对应的 StatusWatch CRD 实例
	statusWatchName := fmt.Sprintf("%s-statuswatch", taskName)
	statusWatch, err := swClient.StatuswatchV1().StatusWatches(namespace).Get(context.TODO(), statusWatchName, metav1.GetOptions{})
	if err != nil {
		log.Printf("Failed to get StatusWatch for task %s: %v", taskName, err)
		return
	}

	// 使用新的 Workers 数组更新 StatusWatch 实例
	statusWatch.Spec.Workers = workers
	_, err = swClient.StatuswatchV1().StatusWatches(namespace).Update(context.TODO(), statusWatch, metav1.UpdateOptions{})
	if err != nil {
		log.Printf("Failed to update StatusWatch for task %s with new workers: %v", taskName, err)
	} else {
		log.Printf("StatusWatch %s updated with current workers\n", statusWatchName)
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

	_, err := clientset.CoreV1().Services("kubeflow").Create(context.Background(), grpcService, metav1.CreateOptions{})
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

	_, err := clientset.CoreV1().Services("kubeflow").Create(context.Background(), ddpMasterService, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	fmt.Println("Service ddp-master created successfully")
	return nil
}

// func createInitOnlyPod(clientset *kubernetes.Clientset, podName string, namespace string) error {
// 	pod := &corev1.Pod{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      podName,
// 			Namespace: namespace,
// 		},
// 		Spec: corev1.PodSpec{
// 			RestartPolicy: "Never",
// 			Volumes: []corev1.Volume{
// 				{
// 					Name: "shared-volume",
// 					VolumeSource: corev1.VolumeSource{
// 						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
// 							ClaimName: "task-manager-pvc",
// 						},
// 					},
// 				},
// 			},
// 			Containers: []corev1.Container{
// 				{
// 					Name:  "init-setup",
// 					Image: "fortypercent/init:v0.1.8",
// 					Command: []string{
// 						"sh", "-c", "cp -r /app/* /dest/data",
// 					},
// 					VolumeMounts: []corev1.VolumeMount{
// 						{
// 							Name:      "shared-volume",
// 							MountPath: "/dest/data",
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	_, err := clientset.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
// 	if err != nil {
// 		log.Printf("Failed to create Pod: %v", err)
// 		return err
// 	} else {
// 		log.Printf("Pod %s created successfully in namespace %s", podName, namespace)
// 	}
// 	return nil
// }

func createPyTorchJob(clientset *kubernetes.Clientset, dynamicClient *dynamic.DynamicClient, taskName string, replicas int32, natsServers string) error {
	// 创建 ServiceAccount

	serviceAccountName := fmt.Sprintf("%s-service-account", taskName)
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: serviceAccountName,
		},
	}
	_, err := clientset.CoreV1().ServiceAccounts("kubeflow").Create(context.Background(), serviceAccount, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			log.Printf("ServiceAccount %s already exists", serviceAccount.Name)
		} else {
			return fmt.Errorf("failed to create ServiceAccount: %v", err)
		}
	} else {
		log.Printf("ServiceAccount %s created successfully", serviceAccount.Name)
	}

	// 创建 Role
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-reader",
			Namespace: "kubeflow",
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
	_, err = clientset.RbacV1().Roles("kubeflow").Create(context.Background(), role, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create Role: %v", err)
	}

	// 创建 RoleBinding
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "read-pods",
			Namespace: "kubeflow",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      serviceAccountName,
				Namespace: "kubeflow",
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "Role",
			Name:     "pod-reader",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
	_, err = clientset.RbacV1().RoleBindings("kubeflow").Create(context.Background(), roleBinding, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create RoleBinding: %v", err)
	}

	// 定义 PyTorchJob 的配置
	pytorchJob := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "kubeflow.org/v1",
			"kind":       "PyTorchJob",
			"metadata": map[string]interface{}{
				"name":      taskName,
				"namespace": "kubeflow",
			},
			"spec": map[string]interface{}{
				"pytorchReplicaSpecs": map[string]interface{}{
					"Worker": map[string]interface{}{
						"replicas":      replicas,
						"restartPolicy": "OnFailure",
						"template": map[string]interface{}{
							"metadata": map[string]interface{}{
								"labels": map[string]string{
									"app": taskName,
								},
							},
							"spec": map[string]interface{}{
								"serviceAccountName": serviceAccount.Name,
								"initContainers": []map[string]interface{}{
									{
										"name":  "init-setup",
										"image": "fortypercent/init:v3.4.6",
										"command": []string{
											"sh",
											"-c",
											"cp -r /app/* /dest/data",
										},
										"volumeMounts": []map[string]interface{}{
											{
												"name":      "shared-volume",
												"mountPath": "/dest/data",
											},
										},
									},
								},
								"containers": []interface{}{map[string]interface{}{
									"name":  "pytorch",
									"image": "fortypercent/train:v0.0.1",
									"command": []string{
										"sh",
										"-c",
										"ls && cd /app/data/ && ls && ./worker-linux-arm",
									},
									"env": []interface{}{
										map[string]interface{}{
											"name":  "NATS_SERVER",
											"value": natsServers,
										},
										map[string]interface{}{
											"name": "POD_NAME",
											"valueFrom": map[string]interface{}{
												"fieldRef": map[string]interface{}{
													"fieldPath": "metadata.name",
												},
											},
										},
										map[string]interface{}{
											"name":  "WORLD_SIZE",
											"value": fmt.Sprintf("%d", replicas),
										},
										map[string]interface{}{
											"name": "RANK",
											"valueFrom": map[string]interface{}{
												"fieldRef": map[string]interface{}{
													"apiVersion": "v1",
													"fieldPath":  "metadata.name",
												},
											},
										},
										map[string]interface{}{
											"name":  "MASTER_ADDR",
											"value": "ddp-master",
										},
										map[string]interface{}{
											"name":  "MASTER_PORT",
											"value": "8889",
										},
										map[string]interface{}{
											"name":  "GRPC_PORT",
											"value": "8888",
										},
									},
									"ports": []interface{}{
										map[string]interface{}{
											"containerPort": 8888,
										},
										map[string]interface{}{
											"containerPort": 8889,
										},
									},
									"imagePullPolicy": "IfNotPresent",
									"volumeMounts": []interface{}{
										map[string]interface{}{
											"name":      "shared-volume",
											"mountPath": "/app/data",
										},
									},
								}},
								"volumes": []interface{}{
									map[string]interface{}{
										"name": "shared-volume",
										"persistentVolumeClaim": map[string]interface{}{
											"claimName": "task-manager-pvc",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	// Create PyTorchJob
	gvr := schema.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "pytorchjobs"}
	result, err := dynamicClient.Resource(gvr).Namespace("kubeflow").Create(context.TODO(), pytorchJob, metav1.CreateOptions{})
	if err != nil {
		log.Fatalf("Failed to create PyTorchJob: %s", err)
		return err
	} else {
		log.Printf("Created PyTorchJob %q successfully.", result.GetName())
	}
	return nil
}

func createStatusWatch(clientset *kubernetes.Clientset, statuswatchClientset *statuswatchclientset.Clientset, taskName string, replicas int32) error {
	statusWatch := &statuswatchv1.StatusWatch{
		ObjectMeta: metav1.ObjectMeta{
			Name: taskName + "-statuswatch",
		},
		Spec: statuswatchv1.StatusWatchSpec{
			Number:  int(replicas),
			Workers: []statuswatchv1.WorkerSpec{},
		},
	}

	maxRetries := 10
	retryDelay := 5 * time.Second

	// 为每个 Pod 添加对应的 WorkerSpec
	for i := 0; i < int(replicas); i++ {
		podName := fmt.Sprintf("%s-worker-%d", taskName, i)
		fmt.Println("Pod Name: ", podName)

		var pod *v1.Pod
		var err error

		for retry := 0; retry < maxRetries; retry++ {
			pod, err = clientset.CoreV1().Pods("kubeflow").Get(context.Background(), podName, metav1.GetOptions{})
			if err == nil {
				break
			}

			fmt.Printf("Failed to get pod %s, retrying in %v...\n", podName, retryDelay)
			time.Sleep(retryDelay)
		}

		if err != nil {
			return fmt.Errorf("failed to get pod %s after %d retries: %v", podName, maxRetries, err)
		}

		// 获取 Pod IP 地址
		podIP := pod.Status.PodIP
		fmt.Printf("Pod IP: %s\n", podIP)

		// 将 Pod IP 转换为特定格式
		formattedIP := strings.Replace(podIP, ".", "-", -1)
		fmt.Printf("Formatted IP: %s\n", formattedIP)

		statusWatch.Spec.Workers = append(statusWatch.Spec.Workers, statuswatchv1.WorkerSpec{
			PodUUID: string(pod.ObjectMeta.UID),
			Name:    formattedIP, //暂时变成IP
		})
	}

	_, err := statuswatchClientset.StatuswatchV1().StatusWatches("kubeflow").Create(context.Background(), statusWatch, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	fmt.Println("StatusWatch created successfully")
	return nil
}
