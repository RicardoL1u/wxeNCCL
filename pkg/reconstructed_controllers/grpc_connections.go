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
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nkeys"
	myappv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
	"google.golang.org/grpc"
	// Adjust this import path to your actual protobufs
)

func (c *Controller) connectAndSubscribeWorkers(statuswatch *myappv1.StatusWatch, accountUkp nkeys.KeyPair, workers []myappv1.WorkerSpec, ukp nkeys.KeyPair) int {
	var wg sync.WaitGroup
	wg.Add(len(workers))
	mu := sync.Mutex{}
	successfulSubscriptions := 0

	for _, worker := range workers {
		go func(worker myappv1.WorkerSpec) {
			defer wg.Done()
			if c.subscribeWorker(worker, accountUkp, ukp, statuswatch.Name) {
				mu.Lock()
				successfulSubscriptions++
				mu.Unlock()
			}
		}(worker)
	}

	wg.Wait()
	c.infoLogger.Printf("Successfully subscribed workers: %d", successfulSubscriptions)
	return successfulSubscriptions
}

func (c *Controller) subscribeWorker(worker myappv1.WorkerSpec, accountUkp, ukp nkeys.KeyPair, taskName string) bool {
	headlessService := getHeadlessServiceName()
	conn, err := c.connectToWorker(worker, headlessService)
	if err != nil {
		c.errorLogger.Printf("Failed to connect to worker %s: %v", worker.Name, err)
		return false
	}
	defer conn.Close()

	if !c.updateWorkerConfig(conn, worker, accountUkp, ukp) {
		return false
	}
	return c.subscribeToTask(conn, worker, taskName)
}

func (c *Controller) connectToWorker(worker myappv1.WorkerSpec, headlessService string) (*grpc.ClientConn, error) {
	const maxRetries = 3
	const retryDelay = 5 * time.Second
	const connTimeout = 10 * time.Second

	podDNS := fmt.Sprintf("%s.%s.kubeflow.svc.cluster.local", worker.Name, headlessService)
	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), connTimeout)
		conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", podDNS, 8888), grpc.WithInsecure(), grpc.WithBlock())
		cancel()
		if err == nil {
			c.infoLogger.Printf("Successfully connected to worker %s", worker.Name)
			return conn, nil
		}
		c.errorLogger.Printf("Failed to connect to worker %s on attempt %d: %v", worker.Name, i+1, err)
		time.Sleep(retryDelay)
	}
	return nil, fmt.Errorf("failed to connect to worker %s after %d attempts", worker.Name, maxRetries)
}
