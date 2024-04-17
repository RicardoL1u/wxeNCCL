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
			log.Printf("Updated config for worker %s successfully", worker.Name)
			return true
		}
		log.Printf("Failed to update config for worker %s on attempt %d: %v", worker.Name, i+1, err)
		time.Sleep(retryDelay)
	}

	log.Printf("Failed to update config for worker %s after %d attempts", worker.Name, maxRetries)
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
			log.Printf("Subscribed to task on worker %s successfully: %s", worker.PodUUID, r.GetMessage())
			conn.Close()
			return true
		}
		log.Printf("Failed to subscribe to task on worker %s on attempt %d: %v", worker.PodUUID, i+1, err)
		time.Sleep(retryDelay)
	}

	log.Printf("Failed to subscribe to task on worker %s after %d attempts", worker.PodUUID, maxRetries)
	return false
}
