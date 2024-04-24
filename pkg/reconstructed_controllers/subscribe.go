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
	"fmt"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
	myappv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
)

// 初始化订阅
func (c *Controller) InitializeSubscriptions(statuswatch *myappv1.StatusWatch) error {
	workerTopic := c.formatWorkerTopic(statuswatch.Name)
	sub, err := c.natsConn.Subscribe(workerTopic, func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
		c.handleMessage(msg, statuswatch)

	})
	if err != nil {
		log.Fatalf("Failed to subscribe to topic '%s': %v", workerTopic, err)
	}
	c.natsSubscriptionMap[statuswatch.Name] = sub
	return nil
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
