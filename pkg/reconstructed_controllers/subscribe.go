package reconstructed_controllers

import (
	"log"
	"strings"

	"github.com/nats-io/nats.go"
	myappv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
)

// 初始化订阅
func (c *Controller) InitializeSubscriptions(statuswatch *myappv1.StatusWatch, successfulACK int) {
	workerTopic := c.formatWorkerTopic(statuswatch.Name)
	sub, err := c.natsConn.Subscribe(workerTopic, func(msg *nats.Msg) {
		c.handleMessage(msg, statuswatch, successfulACK)
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to topic '%s': %v", workerTopic, err)
	}
	c.natsSubscriptionMap[statuswatch.Name] = sub
}

// formatMasterTopic 根据任务名称生成主控通道的NATS主题名称。
func (c *Controller) formatWorkerTopic(statusWatchName string) string {
	separator := "-"
	index := strings.LastIndex(statusWatchName, separator)
	if index == -1 {
		// 如果没有找到分隔符，返回默认主题格式
		return statusWatchName
	}
	subscription := statusWatchName[:index]

	masterTopic := subscription + "-worker"
	return masterTopic
}
