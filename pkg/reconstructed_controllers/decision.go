package reconstructed_controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	myappv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (c *Controller) DecideAction(message Message, statuswatch *myappv1.StatusWatch) {
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
		c.processWarmupCompletedACK(statuswatch, successfulACK)
	case "Train completed successfully":
		log.Printf("Training completed successfully for task: %s", statuswatch.Name)
	default:
		log.Printf("Unhandled ACK data: %s", message.Data)
	}
}

func (c *Controller) processError(message Message, statusWatchName string) {
	log.Printf("Error received: %s", message.Data)
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
	if rand.Intn(2) == 0 { // 50% chance to fail
		podName, err := c.getRandomPodName(statusWatchName)
		if err != nil {
			log.Printf("Failed to get random pod name for %s diagnostics: %v", stage, err)
			return
		}
		log.Printf("%s diagnostics failed. Cleaning up pod: %s", stage, podName)
		if err := c.cleanUpPod(podName); err != nil {
			log.Printf("Failed to clean up pod %s: %v", podName, err)
		}
	} else {
		log.Printf("%s diagnostics passed. No action required.", stage)
		c.restartTask(stage, statusWatchName)
	}
}

func (c *Controller) getRandomPodName(statusWatchName string) (string, error) {
	sw, err := c.kubeclientset.CoreV1().Pods("default").Get(context.TODO(), statusWatchName, v1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get StatusWatch %s: %v", statusWatchName, err)
	}

	if len(sw.Spec.Containers) == 0 {
		return "", fmt.Errorf("no workers found in pod %s", statusWatchName)
	}

	// 从 Workers 列表中随机选择一个 Pod 名字
	return sw.Spec.Containers[rand.Intn(len(sw.Spec.Containers))].Name, nil
}

func (c *Controller) cleanUpPod(podName string) error {
	//TODO: 适配联想
	log.Printf("Cleaning up pod %s in the default namespace...", podName)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := c.kubeclientset.CoreV1().Pods("default").Delete(ctx, podName, v1.DeleteOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// If the pod is not found, it's not necessarily an error in some contexts.
			log.Printf("Pod %s not found, no need to clean up.", podName)
			return nil // Return nil as it's not an error that should impact further processing.
		}

		// Log the error and return it so that callers can handle it if necessary.
		log.Printf("Failed to delete pod %s in the default namespace: %v", podName, err)
		return fmt.Errorf("failed to delete pod %s: %w", podName, err)
	}

	log.Printf("Pod %s in the default namespace cleaned up successfully", podName)
	return nil
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

func (c *Controller) processWarmupCompletedACK(statuswatch *myappv1.StatusWatch, successfulSubscriptions int) int {
	//TODO: 细化ACK是从哪个worker发来的
	successfulSubscriptions++
	log.Printf("Received ACK from worker. Total ACKs: %d", successfulSubscriptions)

	// 检查是否达到了StatusWatch中指定的worker数量
	if successfulSubscriptions == int(statuswatch.Spec.Number) {
		log.Println("Warmup completed for all workers")
		log.Println("Starting training...")
		c.publishTrainMessage(statuswatch.Name)
	}
	return successfulSubscriptions
}
