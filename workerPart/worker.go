package main

import (
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/nats-io/nats.go"
)

type Message struct {
	ID      string `json:"id"`
	Time    string `json:"time"`
	MsgType string `json:"msgType"`
	Data    string `json:"Data"`
}

func main() {
	// 从环境变量中获取NATS服务器地址和JWT
	natsServers := os.Getenv("NATS_SERVERS")
	natsJWT := os.Getenv("NATS_JWT")

	// 与NATS服务器建立连接
	nc, err := nats.Connect(natsServers, nats.UserJWT(natsJWT, nil))
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// 获取任务名称
	taskName := os.Getenv("TASK_NAME")

	// 订阅任务消息
	taskSubscription := taskName + "-master"
	sub, err := nc.Subscribe(taskSubscription, func(msg *nats.Msg) {
		var taskMsg Message
		err := json.Unmarshal(msg.Data, &taskMsg)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return
		}

		if taskMsg.ID == "all" && taskMsg.MsgType == "2" {
			log.Printf("Received task message: %+v", taskMsg)

			// 跑分布式训练任务
			cmd := exec.Command("python", "train_ddp.py")
			err := cmd.Run()
			if err != nil {
				log.Printf("Failed to run task: %v", err)
				return
			}

			log.Println("Task completed successfully")

			// 发送处理结果到NATS服务器
			resultMsg := Message{
				ID:      taskName,
				Time:    time.Now().Format(time.RFC3339),
				MsgType: "1",
				Data:    "Task completed",
			}
			resultMsgBytes, err := json.Marshal(resultMsg)
			if err != nil {
				log.Printf("Failed to marshal result message: %v", err)
				return
			}
			err = nc.Publish(taskName+"-worker", resultMsgBytes)
			if err != nil {
				log.Printf("Failed to publish result message: %v", err)
			}
		}
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to task: %v", err)
	}

	log.Printf("Subscribed to task: %s", taskSubscription)

	// 持续监听任务消息
	select {}
}
