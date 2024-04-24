package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"context"
	"fmt"
	"net"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	pb "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/messageControllerMaster"

	"github.com/nats-io/jwt"
)

type Message struct {
	ID      string `json:"id"`
	Time    string `json:"time"`
	MsgType string `json:"msgType"`
	Data    string `json:"Data"`
}

type server struct {
	pb.UnimplementedTaskManagerServer
	natsConn      *nats.Conn
	sub           *nats.Subscription
	natsConnected chan bool
	clientset     *kubernetes.Clientset
	cancel        context.CancelFunc
	formattedIP   string
	podName       string
	PodError      bool
}

var (
	accountSeed   []byte
	userPublicKey string
	mutex         sync.RWMutex
)

// func getPort() (int32, error) {
// 	podName := os.Getenv("POD_NAME")
// 	if podName == "" {
// 		return 0, fmt.Errorf("POD_NAME environment variable not set")
// 	}

// 	// 创建Kubernetes客户端
// 	config, err := rest.InClusterConfig()
// 	if err != nil {
// 		return 0, fmt.Errorf("failed to get in-cluster config: %v", err)
// 	}
// 	clientset, err := kubernetes.NewForConfig(config)
// 	if err != nil {
// 		return 0, fmt.Errorf("failed to create clientset: %v", err)
// 	}

// 	// 提取Pod名称的第一部分
// 	parts := strings.Split(podName, "-")
// 	if len(parts) < 2 {
// 		return 0, fmt.Errorf("invalid pod name format: %s", podName)
// 	}
// 	// podNamePrefix := parts[0]
// 	//不需要getservice，直接获取端口

// 	// 生成Service名称
// 	serviceName := "grpc-service"

// 	service, err := clientset.CoreV1().Services("default").Get(context.TODO(), serviceName, metav1.GetOptions{})
// 	if err != nil {
// 		return 0, fmt.Errorf("failed to get service: %v", err)
// 	}

// 	// 查找与Pod名称匹配的端口
// 	for _, port := range service.Spec.Ports {
// 		if port.Name == fmt.Sprintf("grpc-%s", podName) {
// 			return port.Port, nil
// 		}
// 	}

// 	return 0, fmt.Errorf("port not found for pod %s", podName)
// }

func (s *server) startGRPCServer(natsConnChan chan *nats.Conn, taskName string) {
	// 获取 gRPC 端口
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051" //使用默认端口
		log.Printf("GRPC_PORT not set, using default: %s", port)
		return
	}

	// 创建 gRPC 服务器
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return
	}

	grpcServer := grpc.NewServer()
	pb.RegisterTaskManagerServer(grpcServer, s)

	// 获取 Pod 的主机名
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get hostname: %v", err)
	}
	// 获取 headless service 的名称
	headlessService := os.Getenv("HEADLESS_SERVICE_NAME")
	if headlessService == "" {
		headlessService = "grpc-service"
		log.Printf("HEADLESS_SERVICE_NAME not set, using default: %s", headlessService)
	}

	// 构建 gRPC 服务器的地址
	addr := fmt.Sprintf("%s.%s.default.svc.cluster.local:%s", hostname, headlessService, port)
	log.Printf("Starting gRPC server at %s", addr)

	// 启动gRPC服务器
	go func() {
		log.Printf("gRPC server listening on %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// 等待 `userPublicKey` 被设置
	log.Println("Waiting for userPublicKey to be set...")
	for {
		if userPublicKey != "" {
			break
		}
		time.Sleep(1 * time.Second)
	}
	log.Println("userPublicKey has been set, continuing...")

	// 生成 NATS JWT
	log.Println("Generating NATS JWT...")
	natsJWT := s.generateNATSJWT(userPublicKey, taskName)

	// 从 accountSeed 恢复账号密钥对
	akp, err := nkeys.FromSeed(accountSeed)
	if err != nil {
		log.Fatalf("Failed to restore account key pair from seed: %v", err)
	}

	// 将 JWT 编码为字符串
	jwtString, err := natsJWT.Encode(akp)
	if err != nil {
		log.Fatalf("Failed to encode NATS JWT: %v", err)
	}

	// // 创建 NATS 连接选项
	// podName = os.Getenv("POD_NAME")
	opts := []nats.Option{
		nats.Name(s.podName),
		nats.Token(jwtString),
	}

	// 建立 NATS 连接
	natsServers := os.Getenv("NATS_SERVER")
	log.Printf("Connecting to NATS servers: %s", natsServers)
	nc, err := nats.Connect(natsServers, opts...)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	log.Println("Connected to NATS successfully")

	// 将 NATS 连接发送到 channel
	s.natsConnected <- true
	log.Println("Push successful")
	natsConnChan <- nc
}

func (s *server) generateNATSJWT(userPublicKey string, taskName string) *jwt.UserClaims {
	// 获取 Pod 的名称
	log.Printf("Generating JWT for task: %s", taskName)
	// podName := os.Getenv("POD_NAME")

	// 创建用户的 JWT
	userJwt := jwt.NewUserClaims(userPublicKey)
	userJwt.Name = s.podName
	userJwt.Expires = time.Now().Add(time.Hour * 24 * 365).Unix()

	// 设置用户的权限和订阅
	userJwt.Pub.Allow.Add(taskName + ".>")
	userJwt.Sub.Allow.Add(taskName + ".>")

	return userJwt
}

func main() {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Failed to create in-cluster config: %v", err)
	}

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		log.Fatal("POD_NAME environment variable not set")
	}

	parts := strings.SplitN(podName, "-", 2)
	if len(parts) < 2 {
		log.Fatalf("Invalid Pod name format: %s", podName)
	}
	taskName := parts[0]
	namespace := "kubeflow"

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes clientset: %v", err)
	}

	pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		log.Fatalf("Failed to get pod: %v", err)
	}

	podIP := pod.Status.PodIP
	if podIP == "" {
		log.Fatal("Pod IP not available")
	}

	formattedIP := strings.ReplaceAll(podIP, ".", "-")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &server{
		clientset:     clientset,
		natsConnected: make(chan bool),
		cancel:        cancel,
		formattedIP:   formattedIP,
		podName:       podName,
		PodError:      false,
	}
	natsConnChan := make(chan *nats.Conn)
	go s.startGRPCServer(natsConnChan, taskName)

	select {
	case s.natsConn = <-natsConnChan:
		log.Println("NATS connection established")
		defer s.natsConn.Close()
	case <-ctx.Done():
		if s.PodError {
			log.Println("killed")
			os.Exit(1)
		} else {
			log.Println("Context canceled, exiting...")
			return
		}
	}

	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-signalCtx.Done()
	log.Println("Received termination signal, exiting...")
	if s.PodError {
		log.Println("killed")
		os.Exit(1)
	} else {
		log.Println("Context canceled, exiting...")
		return
	}
}

func (s *server) SubscribeToTask(ctx context.Context, req *pb.TaskSubscription) (*pb.SubscriptionResponse, error) {
	<-s.natsConnected
	taskName := req.TaskName
	log.Printf("Received task subscription request for task: %s", taskName)

	// 提取订阅主题名称
	subscription := extractSubscriptionName(taskName)
	taskSubscription := subscription + "-master"

	if s.sub != nil {
		log.Printf("Already subscribed to task: %s, skipping new subscription", taskSubscription)
		return &pb.SubscriptionResponse{Success: false, Message: "Already subscribed"}, nil
	}
	maxRetries := 3
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		sub, err := s.natsConn.Subscribe(taskSubscription, s.handleTaskMessage(taskName))
		if err == nil {
			s.sub = sub
			log.Printf("Successfully subscribed to task on attempt %d", attempt)
			return &pb.SubscriptionResponse{Success: true, Message: "Subscribed successfully"}, nil
		}

		log.Printf("Failed to subscribe to task on attempt %d: %v", attempt, err)
		time.Sleep(time.Second * time.Duration(attempt)) // Exponential back-off could be considered here
	}

	log.Printf("Failed to subscribe to task after %d attempts", maxRetries)
	return &pb.SubscriptionResponse{Success: false, Message: "Failed to subscribe to task after multiple attempts"}, err
}

func extractSubscriptionName(taskName string) string {
	lastIndex := strings.LastIndex(taskName, "-")
	if lastIndex == -1 {
		return taskName
	}
	return taskName[:lastIndex]
}

func (s *server) handleTaskMessage(taskName string) func(msg *nats.Msg) {
	callCount := 0
	var temp *nats.Msg
	return func(msg *nats.Msg) {
		if temp != nil && temp == msg {

		} else {
			temp = msg
			callCount++
			var message Message
			err := json.Unmarshal(msg.Data, &message)
			if err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				return
			}
			if message.MsgType == "order" {
				log.Printf("Received order message for task: %s", taskName)
				// 判断是否为停止工作的消息
				if strings.HasPrefix(message.Data, "Stop all workers") {
					// 解析消息以提取stage和status
					parts := strings.Split(message.Data, ":")
					if len(parts) == 3 { // 确保消息格式正确
						stage := strings.TrimSpace(parts[1])
						task := strings.TrimSpace(parts[2])
						log.Printf("Received stop message for task: %s, stage: %s, task: %s", taskName, stage, task)
						// 这里调用停止工作线程的处理函数
						s.handleStopWorkersMessage(taskName, stage, task)
					} else {
						log.Printf("Invalid stop message format for task: %s", taskName)
					}
				} else if strings.HasPrefix(message.Data, "restart:") {
					// 以下是重启相关消息的处理逻辑
					stage := strings.TrimSpace(strings.Split(message.Data, ": ")[1])
					log.Println(stage)
					switch stage {
					case "Warmup":
						log.Printf("Received restart warmup message for task: %s", taskName)
						// 这里调用重启warmup的处理函数
						go s.handleWarmupMessage(taskName, message)
					case "Train":
						log.Printf("Received restart train message for task: %s", taskName)
						// 这里调用重启train的处理函数
						go s.handleTrainStartMessage(taskName)
					default:
						log.Printf("Received unknown restart message for task: %s, stage: %s", taskName, stage)
					}
				} else {
					// 处理其他类型的消息
					switch message.Data {
					case "warmup":
						go s.handleWarmupMessage(taskName, message)
					case "train start":
						go s.handleTrainStartMessage(taskName)
					case "retrain":
						log.Printf("Received retrain message for task: %s", taskName)
						go s.handleWarmupMessage(taskName, message)
						//s.handleRetrainMessage() //
					case "exit":
						log.Printf("Received exit message for task: %s", taskName)
						go s.exitWorkerProcessAndPod(taskName, message)
					}
				}
			}
		}
	}
}

// func (s *server) handleRetrainMessage() {
// 	// 有新的节点起来后，实现训练重新训练停止的逻辑
// 	// warmup.py
// 	// 成功后，调用train_ddp.py
// }

func (s *server) handleStopWorkersMessage(taskName, stage, task string) {
	// Log the receipt of the stop command
	log.Printf("Handling stop workers message for task: %s, stage: %s, task: %s", taskName, stage, task)

	parts := strings.SplitN(stage, ",", 2)
	stage = parts[0]
	// Define the script name based on the stage
	scriptName := ""
	switch stage {
	case "Warmup":
		scriptName = "warmup.py"
	case "Train":
		scriptName = "train_ddp.py"
	}

	// If a script is identified, attempt to kill its processes
	if scriptName != "" {
		cmd := exec.Command("pkill", "-f", "python.*"+scriptName)
		if err := cmd.Run(); err != nil {
			log.Printf("Failed to kill %s Python processes for task %s: %v", stage, taskName, err)
		} else {
			// Construct a success message
			//暂时不处理了
			// msg := Message{
			// 	ID:      taskName, // Assuming taskName can serve as an ID
			// 	Time:    time.Now().Format(time.RFC3339),
			// 	MsgType: "ProcessStopped",
			// 	Data:    stage + " processes stopped successfully.",
			// }
			// workerTopic := extractSubscriptionName(taskName) + "-worker"
			// err := s.publishToNATS(workerTopic, msg)
			// if err != nil {
			// 	log.Printf("Failed to publish stop message for stage %s to NATS: %v", stage, err)
			// 	return
			// }
			log.Printf("%s Python processes for task %s stopped successfully.", stage, taskName)
		}
	} else {
		log.Printf("No script specified for stopping workers in stage: %s", stage)
	}
}

func (s *server) handleTrainStartMessage(taskName string) {
	output, success := s.runTrainScript()
	responseMsg := s.createResponseMessage(success)
	workerTopic := extractSubscriptionName(taskName) + "-worker"
	err := s.publishToNATS(workerTopic, responseMsg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", workerTopic, err)
		return
	}

	log.Printf("Message published to %s", workerTopic)

	if success {
		log.Println("Train completed successfully. Terminating worker process and pod.")
		s.terminateWorkerProcessAndPod()
	} else {
		log.Printf("Train failed with output: %s", output)
	}
}

func (s *server) createResponseMessage(success bool) Message {
	msgType := "ACK"
	data := "Train completed successfully"
	if !success {

		msgType = "error"
		data = "Train task failed"
	}

	return Message{
		ID:      s.formattedIP, //这个id应该是pod的name，不是nats的id
		Time:    time.Now().Format(time.RFC3339),
		MsgType: msgType,
		Data:    data,
	}
}

func (s *server) exitWorkerProcessAndPod(taskName string, message Message) {
	// Check if we need to terminate the worker process and Pod
	if message.ID == s.formattedIP {
		log.Printf("Received exit message for task: %s, terminating worker process and Pod", taskName)
		s.terminateOneWorker(taskName)
	}
}

func (s *server) terminateOneWorker(taskName string) {
	// 使用 defer 来确保订阅最终会被取消
	defer func() {
		if err := s.sub.Unsubscribe(); err != nil {
			log.Printf("Failed to unsubscribe: %v", err)
		}
	}()

	// 获取当前 Pod 对象
	pod, err := s.clientset.CoreV1().Pods("kubeflow").Get(context.Background(), s.podName, metav1.GetOptions{})
	if err != nil {
		log.Fatalf("Failed to get Pod: %v", err)
	}

	// 检查 Pod 的状态
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		log.Printf("Pod is already in %s state, no need to terminate", pod.Status.Phase)
		return
	}
	// Prepare a response message
	responseMsg := Message{
		ID:      s.formattedIP,
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "ACK",
		Data:    "Pod exits successfully",
	}
	workerTopic := extractSubscriptionName(taskName) + "-worker"
	err = s.publishToNATS(workerTopic, responseMsg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", workerTopic, err)
		return
	}
	// 终止 worker 进程
	log.Println("Terminating worker process...")
	s.PodError = true
	s.cancel() // 取消 context,停止主进程
}

func (s *server) terminateWorkerProcessAndPod() {

	if err := s.sub.Unsubscribe(); err != nil {
		// Handle the error, perhaps logging it or taking corrective action
		log.Printf("Failed to unsubscribe: %v", err)
		// Decide whether to return the error or handle it differently
		return
	} else {
		log.Println("Unsubscribed successfully")
	}

	// 终止 worker 进程
	log.Println("Terminating worker process...")
	if s.cancel != nil {
		s.cancel() // 取消 context, 停止主进程
	} else {
		log.Println("Cancel function is not initialized.")
	}

	// 此处不需要更新 Pod 状态，Kubernetes 会根据容器退出代码自动更新
	log.Println("Worker process is scheduled for termination.")
}

func (s *server) runTrainScript() (string, bool) {
	log.Printf("Starting train process...")
	fmt.Println("-----------------")
	cmd := exec.Command("python", "/app/train_ddp.py")

	// 创建 stdout 和 stderr 的管道
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Failed to create stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatalf("Failed to create stderr pipe: %v", err)
	}

	// 启动命令
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start command: %v", err)
	}

	// 使用 bufio.Scanner 实时读取输出
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		fmt.Println(scanner.Text()) // 打印每一行输出
	}

	// 也读取 stderr
	errScanner := bufio.NewScanner(stderr)
	for errScanner.Scan() {
		log.Println(errScanner.Text()) // 打印每一行错误输出
	}

	// 等待命令完成
	if err := cmd.Wait(); err != nil {
		log.Printf("Failed to execute train process: %v", err)
		return "", false
	}

	exitCode := cmd.ProcessState.ExitCode()
	if exitCode == 0 {
		log.Println("Train process completed successfully")
		return "Success", true // 返回一条成功消息
	} else {
		log.Printf("Train process exited with code %d", exitCode)
		return "Failed", false // 返回一条失败消息
	}
}

func (s *server) handleWarmupMessage(taskName string, message Message) {
	success := s.runWarmupScript()
	responseMsg := s.createWarmupResponseMessage(message, success)
	workerTopic := extractSubscriptionName(taskName) + "-worker"
	err := s.publishToNATS(workerTopic, responseMsg)
	if err != nil {
		log.Printf("Failed to publish warmup response message to %s: %v", workerTopic, err)
		return
	}

	log.Printf("Warmup response message published to %s", workerTopic)
}

func (s *server) createWarmupResponseMessage(message Message, success bool) Message {
	msgType := "ACK"
	data := "Warmup completed"
	if !success {

		msgType = "error"
		data = "Warmup failed"
	}

	return Message{
		ID:      s.formattedIP, //这个应该改一下，应该是pod的name
		Time:    time.Now().Format(time.RFC3339),
		MsgType: msgType,
		Data:    data,
	}
}

func (s *server) runWarmupScript() bool {
	log.Printf("Starting warmup process...")
	fmt.Println("-----------------")
	cmd := exec.Command("python", "warmup.py")
	// 创建 stdout 和 stderr 的管道
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Failed to create stdout pipe: %v", err)
		return false
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatalf("Failed to create stderr pipe: %v", err)
		return false
	}

	// 启动命令
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start command: %v", err)
		return false
	}

	// 使用 bufio.Scanner 实时读取输出
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		fmt.Println("Warmup output:", scanner.Text()) // 打印每一行标准输出
	}

	// 也读取 stderr
	errScanner := bufio.NewScanner(stderr)
	for errScanner.Scan() {
		log.Println("Warmup error:", errScanner.Text()) // 打印每一行错误输出
	}

	// 等待命令完成
	if err := cmd.Wait(); err != nil {
		log.Printf("Failed to execute warmup process: %v", err)
		return false
	}

	exitCode := cmd.ProcessState.ExitCode()
	log.Printf("exitCode: %d\n", exitCode)
	if exitCode == 0 {
		log.Println("Warmup process completed successfully")
		return true
	} else {
		log.Printf("Warmup process exited with code %d", exitCode)
		return false
	}
}

// 可能需要另一个方法来处理取消订阅的逻辑，或者在上面的订阅处理函数中

// 1. 监听一个特定的消息类型来触发取消订阅。由监听到删除操作的时候挂掉
// 2. pod挂掉的时候，他的grpc就会挂掉，然后订阅就会结束

func (s *server) publishToNATS(topic string, msg Message) error {
	// 序列化消息
	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	// 发布消息到NATS主题
	err = s.natsConn.Publish(topic, msgData)
	if err != nil {
		return fmt.Errorf("failed to publish message to %s: %v", topic, err)
	} else {
		return nil
	}
}

func (s *server) UpdateWorkerConfig(ctx context.Context, req *pb.UpdateConfigRequest) (*pb.UpdateConfigResponse, error) {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		return nil, fmt.Errorf("POD_NAME environment variable not set")
	}

	s.updateAccountConfig(req)

	log.Printf("Updated config for worker %s with public key: %s", podName, userPublicKey)

	return &pb.UpdateConfigResponse{Success: true, Message: "Worker config updated successfully"}, nil
}

func (s *server) updateAccountConfig(req *pb.UpdateConfigRequest) {
	mutex.Lock()
	defer mutex.Unlock()

	accountSeed = req.AccountSecretKey
	userPublicKey = req.UsrPublicKey

	log.Printf("Updated account config with secret key: %s, public key: %s", req.AccountSecretKey, req.UsrPublicKey)
}
