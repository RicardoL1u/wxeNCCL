package reconstructed_controllers

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

func (c *Controller) PublishMessage(topic string, message Message) {
	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error marshaling message: %v", err)
		return
	}
	if err := c.natsConn.Publish(topic, data); err != nil {
		log.Printf("Failed to publish message: %v", err)
	}
}

func (c *Controller) publishReadyMessage(statusName string) {
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    "warmup",
	}
	masterTopic := c.formatMasterTopic(statusName)
	c.PublishMessage(masterTopic, msg)
}

func (c *Controller) publishRetrainMessage(statusName string) {
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    "retrain",
	}
	masterTopic := c.formatMasterTopic(statusName)
	c.PublishMessage(masterTopic, msg)
}

// formatMasterTopic 根据任务名称生成主控通道的NATS主题名称。
func (c *Controller) formatMasterTopic(statusWatchName string) string {
	separator := "-"
	index := strings.LastIndex(statusWatchName, separator)
	if index == -1 {
		// 如果没有找到分隔符，返回默认主题格式
		return statusWatchName + "-master"
	}
	subscription := statusWatchName[:index]
	masterTopic := subscription + "-master"
	return masterTopic
}

func (c *Controller) publishTrainMessage(statusName string) {
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    "train start",
	}
	masterTopic := c.formatMasterTopic(statusName)
	c.PublishMessage(masterTopic, msg)
}

func (c *Controller) stopWorkersMessage(stage, statusWatchName string) {
	data := fmt.Sprintf("Stop all workers stage: %s, task: %s", stage, statusWatchName)

	// 初始化Message结构体实例
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    data,
	}

	masterTopic := c.formatMasterTopic(statusWatchName)
	c.PublishMessage(masterTopic, msg)
}

func (c *Controller) restartTaskMessage(stage, statusWatchName string) {
	data := fmt.Sprintf("restart: %s", stage)

	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    data,
	}
	masterTopic := c.formatMasterTopic(statusWatchName)
	c.PublishMessage(masterTopic, msg)
}
