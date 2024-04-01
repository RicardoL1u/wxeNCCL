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

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	pb "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/messageControllerMaster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	clientset "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned"
	"gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned/scheme"
	swScheme "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned/scheme"
	informers "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/informers/externalversions/example.com/v1"
	listers "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/listers/example.com/v1"
	myappv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const controllerAgentName = "statuswatch-controller"

// Controller is the controller implementation for Foo resources
type Controller struct {
	statusWatchStates     map[string]bool
	kubeconfig            string
	publicKeyMap          sync.Map
	natsSubscriptionMap   map[string]*nats.Subscription
	natsConn              *nats.Conn
	taskUpdatedWorkersMap map[string][]myappv1.WorkerSpec
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	swclientset clientset.Interface
	swLister    listers.StatusWatchLister
	swSynced    cache.InformerSynced
	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

type Message struct {
	ID      string `json:"id"`
	Time    string `json:"time"`
	MsgType string `json:"msgType"`
	Data    string `json:"Data"`
}

// NewController returns a new sample controller
func NewController(
	kubeclientset kubernetes.Interface,
	swclientset clientset.Interface,
	statusWatchInformer informers.StatusWatchInformer) *Controller {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for sample-controller types.
	utilruntime.Must(swScheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeconfig:            kubeconfig,
		statusWatchStates:     make(map[string]bool),
		natsSubscriptionMap:   make(map[string]*nats.Subscription),
		taskUpdatedWorkersMap: make(map[string][]myappv1.WorkerSpec),
		kubeclientset:         kubeclientset,
		swclientset:           swclientset,
		swLister:              statusWatchInformer.Lister(),
		swSynced:              statusWatchInformer.Informer().HasSynced,
		workqueue:             workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "GoddessMoments"),
		recorder:              recorder,
	}

	klog.Info("Setting up event handlers")
	// Set up an event handler for when Foo resources change
	statusWatchInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			statusWatch, ok := obj.(*myappv1.StatusWatch)
			if !ok {
				utilruntime.HandleError(fmt.Errorf("expected StatusWatch in workqueue but got %#v", obj))
				return
			}
			// 更新内部映射以反映状态，这里我们选择标记为 true，是新的
			controller.statusWatchStates[statusWatch.Name] = true
			controller.enqueueStatusWatch(statusWatch)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldStatusWatch, okOld := oldObj.(*myappv1.StatusWatch)
			newStatusWatch, okNew := newObj.(*myappv1.StatusWatch)
			if !okOld || !okNew {
				utilruntime.HandleError(fmt.Errorf("expected StatusWatch in workqueue but got old: %#v, new: %#v", oldObj, newObj))
				return
			}
			updatedWorkers := getUpdatedWorkers(oldStatusWatch, newStatusWatch)
			if len(updatedWorkers) > 0 {
				taskName := newStatusWatch.Name // 假设任务名称存储在StatusWatch的Name字段中
				// 更新或添加任务名称对应的更新工作负载列表
				controller.taskUpdatedWorkersMap[taskName] = updatedWorkers
				log.Printf("Updated workers for task '%s': %+v", taskName, updatedWorkers)
			}
			// 更新状态映射
			controller.statusWatchStates[newStatusWatch.Name] = false

			// 将更新的StatusWatch对象加入工作队列
			controller.enqueueStatusWatch(newObj)
		},

		DeleteFunc: controller.enqueueStatusWatch,
	})

	return controller
}

// enqueueGoddessMoment takes a StatusWatch resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than StatusWatch.
func (c *Controller) enqueueStatusWatch(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(workers int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting StatusWatch controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.swSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch two workers to process Foo resources
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Foo resource to be synced.
		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Status Watch resource
// with the current status of the resource.
func (c *Controller) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the statuswatch resource with this namespace/name
	sw, err := c.swLister.StatusWatches(namespace).Get(name)
	if err != nil {
		// The StatusWatch resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			c.handleStatusWatchDeleted(sw)
			utilruntime.HandleError(fmt.Errorf("gm '%s' in work queue no longer exists", key))
			return nil
		}

		return err
	} else {
		// 判断是否为新建任务
		if c.statusWatchStates[sw.Name] {
			c.handleStatusWatchCreated(sw)
		} else {
			c.handleStatusWatchUpdated(sw)
			// 处理更新任务的逻辑
		}
	}

	return nil
}

func (c *Controller) handleStatusWatchCreated(statuswatch *myappv1.StatusWatch) {
	// 处理StatusWatch创建事件的逻辑
	fmt.Printf("Number of workers: %d\n", statuswatch.Spec.Number)
	fmt.Printf("Service IP: %s\n", statuswatch.Spec.ServiceIP)

	// 生成并分发public key
	userPublicKey, err := c.generateAndDistributePublicKey(statuswatch)
	if err != nil {
		log.Printf("Failed to generate and distribute public key: %v", err)
		return
	}

	// 为每个worker建立GRPC连接并分发public Key，并让worker订阅任务主题
	successfulSubscriptions := c.connectAndSubscribeWorkers(statuswatch, userPublicKey, statuswatch.Spec.Workers)

	// 订阅statusWatch.TaskName + "-worker"
	workerTopic := statuswatch.Name + "-worker"
	successfulACK := 0
	sub, err := c.natsConn.Subscribe(workerTopic, func(msg *nats.Msg) {
		// 处理接收到的消息
		log.Printf("Received message from topic '%s': %s", msg.Subject, string(msg.Data))

		// 解析接收到的消息
		var message Message
		err := json.Unmarshal(msg.Data, &message)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			return
		}

		if message.MsgType == "ACK" {
			if message.Data == "Warmup completed" {
				successfulACK = c.processWarmupCompletedACK(statuswatch, message, successfulACK)
			} else if message.Data == "Train completed successfully" {
				log.Printf("Training completed successfully for task: %s", statuswatch.Name)
			}
		} else if message.MsgType == "error" {
			log.Printf("Received error message: %s", message.Data)
			// 处理训练失败的情况,例如重试、记录错误等
			// TODO: 处理错误
		}
	})
	if err != nil {
		log.Fatalf("Failed to subscribe to topic '%s': %v", workerTopic, err)
	}
	c.natsSubscriptionMap[statuswatch.Name] = sub

	// 检查是否达到了StatusWatch中指定的worker数量
	if successfulSubscriptions == int(statuswatch.Spec.Number) {
		log.Println("All workers connected and confirmed")

		// 向statusWatch.TaskName + "-master"发送消息
		c.publishReadyMessage(statuswatch.Name)
	}
}

func (c *Controller) processWarmupCompletedACK(statuswatch *myappv1.StatusWatch, message Message, successfulSubscriptions int) int {
	successfulSubscriptions++
	log.Printf("Received ACK from worker. Total ACKs: %d", successfulSubscriptions)

	// 检查是否达到了StatusWatch中指定的worker数量
	if successfulSubscriptions == int(statuswatch.Spec.Number) {
		log.Println("Warmup completed for all workers")

		// 创建消息
		msg := Message{
			ID:      "all",
			Time:    time.Now().Format(time.RFC3339),
			MsgType: "order",
			Data:    "train start",
		}

		// 发送消息到statusWatch.TaskName + "-master"
		masterTopic := statuswatch.Name + "-master"
		err := c.publishToNATS(masterTopic, msg)
		if err != nil {
			log.Printf("Failed to publish message to %s: %v", masterTopic, err)
		} else {
			log.Printf("Message published to %s", masterTopic)
		}
	}

	return successfulSubscriptions
}

func (c *Controller) generateAndDistributePublicKey(statuswatch *myappv1.StatusWatch) (string, error) {
	// 生成public key
	userPublicKey, _, err := generateUserNKeys()
	if err != nil {
		return "", fmt.Errorf("failed to generate user NKeys: %v", err)
	}

	// 把userPublicKey保存起来,用键值对,如果有status更新,我们就把对应的userPublicKey取出来
	c.publicKeyMap.Store(statuswatch.Name, userPublicKey)

	// 更新 nats-auth 的 Secret
	err = updateNATSAuthSecret(c.kubeconfig, statuswatch.Name, userPublicKey)
	if err != nil {
		return "", fmt.Errorf("failed to update NATS Auth Secret: %v", err)
	}
	fmt.Println("NATS Auth Secret 已更新")

	return userPublicKey, nil
}

func (c *Controller) connectAndSubscribeWorkers(statuswatch *myappv1.StatusWatch, userPublicKey string, workers []myappv1.WorkerSpec) int {
	var wg sync.WaitGroup
	wg.Add(len(workers))

	successfulSubscriptions := 0

	for _, worker := range workers {
		go func(worker myappv1.WorkerSpec) {
			defer wg.Done()

			// 建立GRPC连接并订阅任务主题
			if c.subscribeWorker(statuswatch.Spec.ServiceIP, worker, userPublicKey, statuswatch.Name) {
				successfulSubscriptions++
			}
		}(worker)
	}

	wg.Wait()

	log.Printf("Successfully subscribed workers: %d", successfulSubscriptions)

	return successfulSubscriptions
}

func (c *Controller) subscribeWorker(serviceIP string, worker myappv1.WorkerSpec, userPublicKey string, taskName string) bool {
	// 最大重试次数
	const maxRetries = 3

	// 建立GRPC连接
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", serviceIP, worker.GRPCPort), grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to worker %s: %v", worker.PodUUID, err)
		return false
	}

	// 创建GRPC客户端
	client := pb.NewTaskManagerClient(conn)

	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// 尝试更新worker配置
		_, err = client.UpdateWorkerConfig(ctx, &pb.UpdateConfigRequest{PublicKey: userPublicKey})
		if err != nil {
			log.Printf("Failed to update config for worker %s on attempt %d: %v", worker.PodUUID, i+1, err)
			cancel() // 取消当前的上下文
			continue // 尝试重新执行
		}

		// 尝试让worker订阅任务主题
		r, err := client.SubscribeToTask(ctx, &pb.TaskSubscription{TaskName: taskName})
		cancel() // 确保上下文被取消以避免泄露
		if err != nil {
			log.Printf("Attempt %d: Could not subscribe - %v", i+1, err)
			if i == maxRetries-1 { // 如果是最后一次尝试仍然失败
				conn.Close() // 关闭连接
				return false // 所有尝试都失败了，关闭连接后直接返回false
			}
		} else {
			// 订阅成功
			log.Printf("Subscription Response: %s", r.GetMessage())
			log.Printf("Successfully connected to worker %s on attempt %d", worker.PodUUID, i+1)
			conn.Close() // 成功订阅后，关闭连接
			return true  // 成功订阅，结束函数并返回成功
		}
	}

	// 如果代码执行到这里，意味着所有尝试都失败了
	conn.Close() // 确保在所有重试尝试失败后关闭连接
	return false
}

func (c *Controller) publishReadyMessage(taskName string) {
	masterTopic := taskName + "-master"

	// 创建消息
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    "warmup",
	}

	// 发送消息到NATS主题
	err := c.publishToNATS(masterTopic, msg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", masterTopic, err)
	} else {
		log.Printf("Message published to %s", masterTopic)
	}
}

func generateUserNKeys() (string, nkeys.KeyPair, error) {
	// 创建用户的密钥对
	ukp, err := nkeys.CreateUser()
	if err != nil {
		return "", nil, err
	}

	// 获取公钥
	publicKey, err := ukp.PublicKey()
	if err != nil {
		return "", nil, err
	}

	return publicKey, ukp, nil
}

func (c *Controller) publishToNATS(topic string, msg Message) error {
	// 序列化消息
	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	// 发布消息到NATS主题
	err = c.natsConn.Publish(topic, msgData)
	if err != nil {
		return fmt.Errorf("failed to publish message to %s: %v", topic, err)
	}

	return nil
}

func updateNATSAuthSecret(kubeconfig, statusWatchName, usrPublicKey string) error {
	// 加载kubeconfig
	kubeconfig = os.ExpandEnv(kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Print("failed to build Kubernetes config: %v", err)
		return err
	}

	// 创建Kubernetes客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	// 更新nats-auth的Secret
	secret, err := clientset.CoreV1().Secrets("default").Get(context.Background(), "nats-auth", metav1.GetOptions{})
	if err != nil {
		return err
	}

	// 更新Secret的数据
	secret.Data[statusWatchName+"-account-public-key"] = []byte(usrPublicKey)

	// 更新Secret
	_, err = clientset.CoreV1().Secrets("default").Update(context.Background(), secret, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleStatusWatchUpdated(newStatuswatch *myappv1.StatusWatch) {
	fmt.Printf("New number of workers: %d\n", newStatuswatch.Spec.Number)

	// 比较新旧StatusWatch的变化,找到发生变化的worker
	updatedWorkers := c.taskUpdatedWorkersMap[newStatuswatch.Name]

	// 从publicKeyMap中获取保存的Public Key
	publicKey, ok := c.publicKeyMap.Load(newStatuswatch.Name)
	if !ok {
		log.Printf("Public Key not found for StatusWatch %s", newStatuswatch.Name)
		return
	}

	// 向发生变化的worker发送更新的配置,并重新订阅任务主题
	successfulSubscriptions := c.connectAndSubscribeWorkers(newStatuswatch, publicKey.(string), updatedWorkers)

	// 检查是否所有发生变化的worker都成功订阅了任务主题
	if successfulSubscriptions == len(updatedWorkers) {
		log.Println("All updated workers resubscribed successfully")
		// 向statusWatch.TaskName + "-master"发送消息
		c.publishRetrainMessage(newStatuswatch.Name)
	}
}

func getUpdatedWorkers(oldStatuswatch, newStatuswatch *myappv1.StatusWatch) []myappv1.WorkerSpec {
	oldWorkers := make(map[string]myappv1.WorkerSpec)

	for _, worker := range oldStatuswatch.Spec.Workers {
		oldWorkers[worker.PodUUID] = worker
	}

	var updatedWorkers []myappv1.WorkerSpec
	for _, newWorker := range newStatuswatch.Spec.Workers {
		oldWorker, ok := oldWorkers[newWorker.PodUUID]
		if !ok {
			// 新增的worker
			updatedWorkers = append(updatedWorkers, newWorker)
		} else {
			// 比较worker的配置是否发生变化
			if !reflect.DeepEqual(oldWorker, newWorker) {
				updatedWorkers = append(updatedWorkers, newWorker)
			}
		}
	}

	return updatedWorkers
}

// TODO: 重新训练任务
func (c *Controller) publishRetrainMessage(taskName string) {
	masterTopic := taskName + "-master"

	// 创建消息
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    "retrain",
	}

	// 发送消息到NATS主题
	err := c.publishToNATS(masterTopic, msg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", masterTopic, err)
	} else {
		log.Printf("Message published to %s", masterTopic)
	}
}

func (c *Controller) handleStatusWatchDeleted(statuswatch *myappv1.StatusWatch) {
	// 处理StatusWatch删除事件的逻辑
	fmt.Printf("StatusWatch %s deleted\n", statuswatch.Name)

	//取消NATS订阅
	sub, ok := c.natsSubscriptionMap[statuswatch.Name]
	if ok {
		err := sub.Unsubscribe()
		if err != nil {
			log.Printf("Failed to unsubscribe from task '%s': %v", statuswatch.Name, err)
		}
	}
	delete(c.natsSubscriptionMap, statuswatch.Name)
	//删除publicKey
	c.publicKeyMap.Delete(statuswatch.Name)

	log.Printf("Cleanup completed for StatusWatch %s", statuswatch.Name)
}
