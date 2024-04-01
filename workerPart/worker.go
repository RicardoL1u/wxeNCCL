package main

import (
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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pb "masterPart/messageControllerMaster"

	"github.com/nats-io/jwt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Message struct {
	ID      string `json:"id"`
	Time    string `json:"time"`
	MsgType string `json:"msgType"`
	Data    string `json:"Data"`
}

type server struct {
	pb.UnimplementedTaskManagerServer
	natsConn *nats.Conn
	sub      *nats.Subscription
}

var (
	userPublicKey string
	mutex         sync.RWMutex
	wg            sync.WaitGroup
)

func getPort() (int32, error) {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		return 0, fmt.Errorf("POD_NAME environment variable not set")
	}

	// 创建Kubernetes客户端
	config, err := rest.InClusterConfig()
	if err != nil {
		return 0, fmt.Errorf("failed to get in-cluster config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return 0, fmt.Errorf("failed to create clientset: %v", err)
	}

	// 提取Pod名称的第一部分
	parts := strings.Split(podName, "-")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid pod name format: %s", podName)
	}
	podNamePrefix := parts[0]

	// 生成Service名称
	serviceName := fmt.Sprintf("%s-training-service", podNamePrefix)

	service, err := clientset.CoreV1().Services("default").Get(context.TODO(), serviceName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get service: %v", err)
	}

	// 查找与Pod名称匹配的端口
	for _, port := range service.Spec.Ports {
		if port.Name == fmt.Sprintf("grpc-%s", podName) {
			return port.Port, nil
		}
	}

	return 0, fmt.Errorf("port not found for pod %s", podName)
}

func startGRPCServer(natsConnChan chan *nats.Conn, taskName string) {
	port, err := getPort()
	if err != nil {
		log.Fatalf("failed to get port: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	pb.RegisterTaskManagerServer(grpcServer, &server{})

	log.Printf("gRPC server started on port %d", port)

	// 启动gRPC服务器,等待 `UpdateWorkerConfig` 方法被调用
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// 等待 `userPublicKey` 被设置
	for {
		mutex.RLock()
		if userPublicKey != "" {
			mutex.RUnlock()
			break
		}
		mutex.RUnlock()
		time.Sleep(1 * time.Second)
	}

	natsServers := os.Getenv("NATS_SERVERS")
	// 生成 JWT
	natsJWT := generateNATSJWT(userPublicKey, taskName)

	// 与 NATS 服务器建立连接

	// 将 *jwt.UserClaims 对象编码为字符串格式的 JWT
	jwtString, err := natsJWT.Encode(nil)
	if err != nil {
		log.Fatal(err)
	}

	// 创建 NATS 连接选项
	opts := []nats.Option{
		nats.UserCredentials(jwtString),
	}

	// 建立连接
	nc, err := nats.Connect(natsServers, opts...)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}

	// 将 NATS 连接发送到 channel
	natsConnChan <- nc
}

func generateNATSJWT(userPublicKey string, taskName string) *jwt.UserClaims {
	// 获取全局的 `userPublicKey`
	mutex.RLock()
	pubKey := userPublicKey
	mutex.RUnlock()

	// 获取 Pod 的名称
	podName := os.Getenv("POD_NAME")

	// 创建用户的 JWT
	userJwt := jwt.NewUserClaims(pubKey)
	userJwt.Name = podName
	userJwt.Expires = time.Now().AddDate(1, 0, 0).Unix()

	// 设置用户的权限和订阅
	userJwt.Pub.Allow.Add(taskName + ".>")
	userJwt.Sub.Allow.Add(taskName + ".>")

	return userJwt
}

func main() {

	natsConnChan := make(chan *nats.Conn)
	// 获取 Pod 的名称
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		log.Fatalf("POD_NAME environment variable not set")
	}

	// 从 Pod 名称中提取 taskName
	parts := strings.Split(podName, "-")
	if len(parts) < 2 {
		log.Fatalf("Invalid Pod name format: %s", podName)
	}
	taskName := parts[0]
	// 启动 gRPC 服务器并等待 `publicKey`
	go startGRPCServer(natsConnChan, taskName)

	nc := <-natsConnChan
	defer nc.Close()

	// 创建一个通道来接收终止信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 无限循环,保持进程运行
	for {
		select {
		case <-sigChan:
			// 收到终止信号,退出循环
			log.Println("Received termination signal, exiting...")
			return
		default:
			// 没有收到终止信号,继续等待
			time.Sleep(time.Second)
		}
	}

}

func (s *server) SubscribeToTask(ctx context.Context, req *pb.TaskSubscription) (*pb.SubscriptionResponse, error) {
	// 实现订阅任务的逻辑
	log.Printf("Received task subscription request for task: %s", req.TaskName)

	// 生成订阅主题名称
	taskSubscription := req.TaskName + "-master"

	// 订阅任务消息
	sub, err := s.natsConn.Subscribe(taskSubscription, func(msg *nats.Msg) {
		var message Message
		err := json.Unmarshal(msg.Data, &message)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return
		}

		if message.MsgType == "order" {
			if message.Data == "warmup" {
				s.handleWarmupMessage(req.TaskName, message)
			} else if message.Data == "train start" {
				s.handleTrainStartMessage(req.TaskName, message)
			} else if message.Data == "train stop" {
				// 处理训练停止的逻辑
				log.Printf("Received train stop message for task: %s", req.TaskName)
				s.sub.Unsubscribe()              // 关闭订阅
				s.terminateWorkerProcessAndPod() // 结束worker进程和删除pod
			}
		}
	})
	if err != nil {
		log.Printf("Failed to subscribe to task: %v", err)
		return &pb.SubscriptionResponse{Success: false, Message: "Failed to subscribe to task"}, nil
	}

	s.sub = sub // 将订阅赋值给结构体字段

	// 返回成功响应给master
	log.Printf("Subscribed to task: %s", taskSubscription)
	return &pb.SubscriptionResponse{Success: true, Message: "Task subscribed successfully"}, nil
}

func (s *server) handleTrainStartMessage(taskName string, message Message, sub *nats.Subscription) {
	output, success := s.runTrainScript()
	var responseMsg Message

	if success {
		// 创建成功消息
		responseMsg = Message{
			ID:      message.ID,
			Time:    time.Now().Format(time.RFC3339),
			MsgType: "ACK",
			Data:    "Train completed successfully",
		}

		// 发送消息到 taskName + "-worker" 主题
		workerTopic := taskName + "-worker"
		err := s.publishToNATS(workerTopic, responseMsg)
		if err != nil {
			log.Printf("Failed to publish message to %s: %v", workerTopic, err)
		} else {
			log.Printf("Message published to %s. Terminating worker process and pod.", workerTopic)
			s.sub.Unsubscribe()              // 关闭订阅
			s.terminateWorkerProcessAndPod() // 结束worker进程和删除pod
		}
	} else {
		// 创建失败消息
		responseMsg = Message{
			ID:      message.ID,
			Time:    time.Now().Format(time.RFC3339),
			MsgType: "error",
			Data:    output,
		}

		// 发送消息到 taskName + "-worker" 主题
		workerTopic := taskName + "-worker"
		err := s.publishToNATS(workerTopic, responseMsg)
		if err != nil {
			log.Printf("Failed to publish message to %s: %v", workerTopic, err)
		} else {
			log.Printf("Message published to %s", workerTopic)
		}
	}
}

func (s *server) terminateWorkerProcessAndPod() {
	// 在这里实现结束worker进程和删除pod的逻辑
	// 例如,向Kubernetes API服务器发送请求以删除pod
	// ...
}

func (s *server) runTrainScript() (string, bool) {
	cmd := exec.Command("python", "train_ddp.py")
	output, err := cmd.CombinedOutput()
	lines := strings.Split(string(output), "\n")
	lastLine := lines[len(lines)-1]
	if err != nil {
		log.Printf("Failed to execute train process: %v", err)
		log.Printf("Output: %s", string(output))
		return lastLine, false
	}

	exitCode := cmd.ProcessState.ExitCode()
	if exitCode == 0 {
		log.Println("Train process completed successfully")
		return lastLine, true
	} else {
		log.Printf("Train process exited with code %d", exitCode)
		return lastLine, false
	}
}

func (s *server) handleWarmupMessage(taskName string, message Message) {
	success := s.runWarmupScript()
	id := "0" // 从消息中提取ID,todo
	if success {
		// 创建消息
		responseMsg := Message{
			ID:      id,
			Time:    time.Now().Format(time.RFC3339),
			MsgType: "ACK",
			Data:    "Warmup completed",
		}

		// 发送消息到 taskName + "-worker" 主题
		workerTopic := taskName + "-worker"
		err := s.publishToNATS(workerTopic, responseMsg)
		if err != nil {
			log.Printf("Failed to publish message to %s: %v", workerTopic, err)
		} else {
			log.Printf("Message published to %s", workerTopic)
		}
	}
}

func (s *server) runWarmupScript() bool {
	cmd := exec.Command("python", "warmup.py")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Failed to execute warmup process: %v", err)
		log.Printf("Output: %s", string(output))
		return false
	}

	exitCode := cmd.ProcessState.ExitCode()
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

func (s *server) publishToNATS(topic string, msg *pb.TaskMessage) error {
	// 序列化消息
	msgData, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	// 发布消息到NATS主题
	err = s.natsConn.Publish(topic, msgData)
	if err != nil {
		return fmt.Errorf("failed to publish message to %s: %v", topic, err)
	}

	return nil
}

func (s *server) UpdateWorkerConfig(ctx context.Context, req *pb.UpdateConfigRequest) (*pb.UpdateConfigResponse, error) {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		return nil, fmt.Errorf("POD_NAME environment variable not set")
	}

	//维护一个全局变量，用于存储用户的公钥
	mutex.Lock()
	userPublicKey = req.PublicKey
	mutex.Unlock()
	// 更新worker的配置
	log.Printf("Updating config for worker %s with public key: %s", podName, req.PublicKey)

	return &pb.UpdateConfigResponse{Success: true, Message: "Worker config updated successfully"}, nil
}

// Compare this snippet from workerPart/worker.go:
