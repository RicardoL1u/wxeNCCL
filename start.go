package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	// 为每个任务写一个account jwt,每个任务每个worker pod写一个user jwt
	kubeconfig := "$HOME/.kube/config"

	// 生成账号的 NKEY 对
	accountPublicKey, _, err := generateAccountNkeys()
	if err != nil {
		panic(err)
	}

	// 生成用户的 Public Key
	userPublicKeys := make(map[string]string)
	for i := 0; i < *taskWorker; i++ {
		// 生成用户的 NKEY 对
		userPublicKey, _, err := generateUserNKeys()
		if err != nil {
			panic(err)
		}

		userPublicKeys[fmt.Sprintf("worker%d", i)] = userPublicKey
	}

	// 更新 nats-auth 的 Secret
	err = updateNATSAuthSecret(kubeconfig, accountPublicKey, userPublicKeys)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("NATS Auth Secret 已更新")

	// 通知 master 订阅任务的主题，这个可能要改
	// 订阅者需要先订阅主题,然后才能接收发布到该主题的消息
	// localhost应该是masterPod的地址，但这里可能到时候得改一下
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewTaskManagerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.SubscribeToTopic(ctx, &pb.TaskSubscription{TaskName: *taskName})
	if err != nil {
		log.Fatalf("could not subscribe: %v", err)
	}
	log.Printf("Subscription Response: %s", r.GetMessage())

	replicas := int32(*taskWorker)
	err = createBlockedStatefulSet(kubeconfig, replicas, natsServers, userPublicKeys)
	if err != nil {
		panic(err)
	}

}

func createBlockedStatefulSet(kubeconfig string, replicas int32, natsServers string, userPublicKeys map[string]string) error {
	// 加载 Kubernetes 配置
	kubeconfig = os.ExpandEnv(kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}

	// 创建 Kubernetes 客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	// 准备一个启动脚本,根据 Pod 名称设置 USERPUBLICKEY 环境变量并执行原始命令
	startupScript := `#!/bin/sh
podName=$(hostname)
publicKey=$(eval echo \$PUBLICKEY_$podName)
export USERPUBLICKEY=$publicKey
sleep infinity & go run worker.go & wait
`

	// 定义 StatefulSet
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "blocked-statefulset",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "blocked-pods",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "blocked-pods",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "blocked-container",
							Image:   "fortypercent/ddp-training-cpu:v7.0",
							Command: []string{"/bin/sh", "-c", startupScript},
							// 将所有 Public Keys 以环境变量形式传递给容器
							Env: append(generatePublicKeyEnvVars(userPublicKeys), corev1.EnvVar{
								Name:  "NATS_SERVER",
								Value: natsServers,
							}),
						},
					},
				},
			},
		},
	}

	// 创建 StatefulSet
	_, err = clientset.AppsV1().StatefulSets("default").Create(context.Background(), statefulSet, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	fmt.Println("StatefulSet created successfully")
	return nil
}

// 辅助函数,用于生成包含所有 Public Keys 的环境变量列表
func generatePublicKeyEnvVars(userPublicKeys map[string]string) []corev1.EnvVar {
	var envVars []corev1.EnvVar
	for podName, publicKey := range userPublicKeys {
		envVar := corev1.EnvVar{
			Name:  fmt.Sprintf("PUBLICKEY_%s", podName),
			Value: publicKey,
		}
		envVars = append(envVars, envVar)
	}
	return envVars
}

func generateAccountNkeys() (publicKey string, kp nkeys.KeyPair, err error) {
	// 创建账号的密钥对
	kp, err = nkeys.CreateAccount()
	if err != nil {
		return "", nil, err
	}

	// 获取公钥
	publicKey, err = kp.PublicKey()
	if err != nil {
		return "", nil, err
	}

	return publicKey, kp, nil
}

func generateUserNKeys() (string, nkeys.KeyPair, error) {
	// 创建用户的密钥对
	ukp, err := nkeys.CreateUser()
	if err != nil {
		return "", nil, err
	}

	// 获取公钥
	publicKey, err := ukp.PublicKey()
	if err != nil {
		return "", nil, err
	}

	return publicKey, ukp, nil
}

func updateNATSAuthSecret(kubeconfig, accountPublicKey string, userPublicKeys map[string]string) error {
	// 加载kubeconfig
	kubeconfig = os.ExpandEnv(kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Println("2", err)
		return err
	}

	// 创建Kubernetes客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	// 更新nats-auth的Secret
	secret, err := clientset.CoreV1().Secrets("default").Get(context.Background(), "nats-auth", metav1.GetOptions{})
	if err != nil {
		return err
	}

	// 更新Secret的数据
	secret.Data["account-public-key"] = []byte(accountPublicKey)
	for podName, userPublicKey := range userPublicKeys {
		secret.Data[podName+"-public-key"] = []byte(userPublicKey)
	}

	// 更新Secret
	_, err = clientset.CoreV1().Secrets("default").Update(context.Background(), secret, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}
