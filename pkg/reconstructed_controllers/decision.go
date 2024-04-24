/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package reconstructed_controllers

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	myappv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
)

var ackCh chan bool

func init() {
	rand.Seed(time.Now().UnixNano())
	ackCh = make(chan bool)
}

func (c *Controller) DecideAction(message Message, statuswatch *myappv1.StatusWatch) {
	fmt.Println("message.MsgType", message.MsgType)
	switch message.MsgType {
	case "ACK":
		c.processAck(message, statuswatch)
	case "error":
		c.processError(message, statuswatch.Name)
	default:
		log.Printf("Received unknown message type: %s", message.MsgType)
	}
}

func (c *Controller) processAck(message Message, statuswatch *myappv1.StatusWatch) {

	switch message.Data {
	case "Warmup completed":
		fmt.Println("Warmup completed ACK received")
		fmt.Println(message)
		c.processWarmupCompletedACK(statuswatch)
	case "Train completed successfully":
		log.Printf("Training completed successfully for task: %s", statuswatch.Name)
	case "Pod exits successfully":
		log.Printf("Pod exits successfully for task: %s", statuswatch.Name)
		ackCh <- true
	default:
		log.Printf("Unhandled ACK data: %s", message.Data)
	}
}

func (c *Controller) processError(message Message, statusWatchName string) {
	log.Printf("Error received: %s", message.Data)

	// 解析消息时间
	msgTime, err := time.Parse(time.RFC3339, message.Time)
	if err != nil {
		log.Printf("Failed to parse message time: %v", err)
		return
	}

	// 从atomic.Value获取最后一次错误时间
	lastProcTimeVal := c.lastErrorTime.Load()
	fmt.Println("lastProcTimeVal", lastProcTimeVal)
	if lastProcTimeVal != nil {
		lastProcTime := lastProcTimeVal.(time.Time)
		if lastProcTime.Equal(msgTime) {
			log.Println("Error already processed for this timestamp, skipping.")
			return
		}
	}

	// 如果消息时间不同，更新最后错误时间并处理错误
	c.lastErrorTime.Store(msgTime)

	if !atomic.CompareAndSwapInt32(&c.processingError, 0, 1) {
		log.Println("Error already being processed, skipping duplicate message.")
		return
	}
	defer atomic.StoreInt32(&c.processingError, 0)

	c.handleError(message.Data, statusWatchName)
}

func (c *Controller) handleError(errorMsg string, statusWatchName string) {
	switch errorMsg {
	case "Warmup failed", "Train task failed":
		operation := map[string]string{
			"Warmup failed":     "Warmup",
			"Train task failed": "Train",
		}[errorMsg]
		successfulACK = 0
		c.stopAllWorkers(operation, statusWatchName)
		log.Printf("%s detected. Starting diagnostics...", errorMsg)
		time.Sleep(10 * time.Second)
		c.handleDiagnostics(operation, statusWatchName)
	default:
		log.Printf("Unknown error type received: %s", errorMsg)
	}
}

func (c *Controller) stopAllWorkers(stage, statusWatchName string) {
	// 根据statusName的值执行不同的停止逻辑
	// 例如，可以根据不同的错误类型来决定是否记录特定的日志，或者是通知某些特定的工作线程停止
	log.Printf("Stopping all workers for stage: %s, task: %s", stage, statusWatchName)

	c.stopWorkersMessage(stage, statusWatchName)
}

// handleDiagnostics runs diagnostics and takes action based on the results.
func (c *Controller) handleDiagnostics(stage, statusWatchName string) {
	a := rand.Intn(2)
	a = 0
	if a == 0 { // 50% chance to fail
		podName, err := c.getRandomPodName(statusWatchName)
		if err != nil {
			log.Printf("Failed to get random pod name for %s diagnostics: %v", stage, err)
			return
		}
		log.Printf("%s diagnostics finds error pod. Cleaning up pod: %s", stage, podName)
		c.cleanUpPod(statusWatchName, podName)

	} else {
		log.Printf("%s diagnostics passed. No action required.", stage)
		c.restartTask(stage, statusWatchName)
	}
}

func (c *Controller) getRandomPodName(statusWatchName string) (string, error) {
	// 使用lister从缓存中获取StatusWatch对象
	sw, err := c.swLister.StatusWatches("kubeflow").Get(statusWatchName)
	if err != nil {
		return "", fmt.Errorf("failed to get StatusWatch %s: %v", statusWatchName, err)
	}

	// 检查是否有workers定义
	if len(sw.Spec.Workers) == 0 {
		return "", fmt.Errorf("no workers found in StatusWatch %s", statusWatchName)
	}

	// 从workers列表中随机选择一个
	rand.Seed(time.Now().UnixNano()) // 确保随机性
	randomIndex := rand.Intn(len(sw.Spec.Workers))
	randomWorker := sw.Spec.Workers[randomIndex]

	return randomWorker.Name, nil
}

func (c *Controller) cleanUpPod(statusWatchName, podName string) {
	//TODO: 适配联想
	log.Printf("Cleaning up pod %s in the default namespace...", podName)
	c.publishQuitMessage(statusWatchName, podName)
	<-ackCh
	log.Printf("Pod %s cleaned up successfully", podName)
}

// 重启任务函数
func (c *Controller) restartTask(stage, statusWatchName string) {
	log.Printf("Restarting tasks for stage: %s", stage)
	c.restartTaskMessage(stage, statusWatchName)
	// 这里添加实际的任务重启逻辑
}

func (c *Controller) handleMessage(msg *nats.Msg, statuswatch *myappv1.StatusWatch) {
	log.Printf("Received message from topic '%s'", msg.Subject)
	var message Message
	if err := json.Unmarshal(msg.Data, &message); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}
	c.DecideAction(message, statuswatch)
}

func (c *Controller) processWarmupCompletedACK(statuswatch *myappv1.StatusWatch) int {
	//TODO: 细化ACK是从哪个worker发来的
	successfulACK++

	log.Printf("Received ACK from worker. Total ACKs: %d", successfulACK)

	// 检查是否达到了StatusWatch中指定的worker数量
	if successfulACK == int(statuswatch.Spec.Number) {
		log.Println("Warmup completed for all workers")
		log.Println("Starting training...")
		c.publishTrainMessage(statuswatch.Name)
	}
	return successfulACK
}
