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
	"context"
	"log"
	"time"

	pb "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/messageControllerMaster"

	"github.com/nats-io/nkeys"
	myappv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
	"google.golang.org/grpc"
)

func (c *Controller) updateWorkerConfig(conn *grpc.ClientConn, worker myappv1.WorkerSpec, accountUkp, ukp nkeys.KeyPair) bool {
	const maxRetries = 3
	const retryDelay = 5 * time.Second
	const connTimeout = 10 * time.Second

	client := pb.NewTaskManagerClient(conn)

	accountSeed, err := accountUkp.Seed()
	if err != nil {
		log.Printf("Failed to get account seed: %v", err)
		return false
	}

	userPublicKey, err := ukp.PublicKey()
	if err != nil {
		log.Printf("Failed to get user public key: %v", err)
		return false
	}

	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), connTimeout)
		_, err = client.UpdateWorkerConfig(ctx, &pb.UpdateConfigRequest{AccountSecretKey: accountSeed, UsrPublicKey: userPublicKey})
		cancel()
		if err == nil {
			c.infoLogger.Printf("Successfully updated configuration for worker: %s", worker.Name)
			return true
		}
		c.errorLogger.Printf("Attempt %d to update configuration for worker %s failed: %v", i+1, worker.Name, err)
		time.Sleep(retryDelay)
	}

	c.errorLogger.Printf("Unable to update configuration for worker %s despite %d attempts", worker.Name, maxRetries)

	return false
}

func (c *Controller) subscribeToTask(conn *grpc.ClientConn, worker myappv1.WorkerSpec, taskName string) bool {
	const maxRetries = 3
	const retryDelay = 5 * time.Second
	const connTimeout = 10 * time.Second

	client := pb.NewTaskManagerClient(conn)

	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), connTimeout)
		r, err := client.SubscribeToTask(ctx, &pb.TaskSubscription{TaskName: taskName})
		cancel()
		if err == nil {
			c.infoLogger.Printf("Successfully subscribed to task on worker %s: %s", worker.Name, r.GetMessage())
			conn.Close()
			return true
		}
		c.errorLogger.Printf("Subscription attempt %d to task on worker %s failed: %v", i+1, worker.Name, err)
		time.Sleep(retryDelay)
	}

	c.errorLogger.Printf("Unable to subscribe to task on worker %s despite %d attempts", worker.Name, maxRetries)
	return false
}
