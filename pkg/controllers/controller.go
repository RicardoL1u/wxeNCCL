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
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const controllerAgentName = "statuswatch-controller"

// Controller is the controller implementation for Foo resources
type Controller struct {
	statusWatchStates     map[string]bool
	kubeconfig            *rest.Config
	publicKeyMap          sync.Map
	accountPublicKeyMap   sync.Map
	natsSubscriptionMap   map[string]*nats.Subscription
	natsConn              *nats.Conn
	taskUpdatedWorkersMap map[string][]myappv1.WorkerSpec
	processingError       int32 // 用于表示是否正在处理错误消息，0表示没有，1表示有
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
	statusWatchInformer informers.StatusWatchInformer, cfg *rest.Config) *Controller {
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
		kubeconfig:            cfg,
		statusWatchStates:     make(map[string]bool),
		natsSubscriptionMap:   make(map[string]*nats.Subscription),
		taskUpdatedWorkersMap: make(map[string][]myappv1.WorkerSpec),
		kubeclientset:         kubeclientset,
		swclientset:           swclientset,
		swLister:              statusWatchInformer.Lister(),
		swSynced:              statusWatchInformer.Informer().HasSynced,
		workqueue:             workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "GoddessMoments"),
		recorder:              recorder,
		processingError:       0,
	}

	// 建立 NATS 连接
	natsURL := "nats://10.96.248.35:4222"
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	controller.natsConn = nc

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
			} else {
				return
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
			c.handleStatusWatchDeleted(name)
			utilruntime.HandleError(fmt.Errorf("sw '%s' in work queue no longer exists", key))
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
	accountUkp, ukp, err := c.generateAndDistributePublicKey(statuswatch)
	if err != nil {
		log.Printf("Failed to generate and distribute public key: %v", err)
		return
	}

	log.Printf("Waiting for 10 seconds before connecting and subscribing workers...")
	time.Sleep(10 * time.Second)

	// 为每个worker建立GRPC连接并分发public Key，并让worker订阅任务主题
	successfulSubscriptions := c.connectAndSubscribeWorkers(statuswatch, accountUkp, statuswatch.Spec.Workers, ukp)

	// 订阅statusWatch.TaskName + "-worker"
	separator := "-"
	// 找到最后一个分隔符的索引
	index := strings.LastIndex(statuswatch.Name, separator)
	subscription := statuswatch.Name[:index]
	// 生成订阅主题名称
	taskSubscription := subscription

	log.Printf("Prepare to subscribe task: %s", taskSubscription)
	workerTopic := taskSubscription + "-worker"
	successfulACK := 0
	sub, err := c.natsConn.Subscribe(workerTopic, func(msg *nats.Msg) {
		// 处理接收到的消息
		log.Printf("Received message from topic '%s'", msg.Subject)

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
			// 尝试原子地将processingError标志从0改为1，以进入错误处理逻辑
			if atomic.CompareAndSwapInt32(&c.processingError, 0, 1) {
				// 使用goroutine异步处理错误，避免阻塞
				go func() {
					defer atomic.StoreInt32(&c.processingError, 0) // 处理完成后，原子地将标志重置为0

					log.Printf("Received error message: %s", message.Data)
					switch message.Data {
					case "Warmup failed":
						//让所有worker都停下来
						c.stopAllWorkers("Warmup", statuswatch.Name)
						//处理都停止之后的逻辑
						successfulACK = 0
						log.Println("Warmup failed detected. Starting diagnostics...")
						// 进行Warmup阶段的检测和处理
						time.Sleep(10 * time.Second) // 注意，这里的长时间等待可能不是最佳实践
						c.handleDiagnostics("Warmup", statuswatch.Name)

					case "Train task failed":
						//让所有worker都停下来
						successfulACK = 0
						c.stopAllWorkers("Train", statuswatch.Name)
						log.Println("Training task failed detected. Starting diagnostics...")
						// 进行Train阶段的检测和处理
						time.Sleep(10 * time.Second) // 注意，这里的长时间等待可能不是最佳实践
						c.handleDiagnostics("Train", statuswatch.Name)

					default:
						// 未知的错误类型，可能需要记录或者进一步处理
						log.Printf("Unknown error type received: %s", message.Data)
					}
				}()
			} else {
				log.Println("Error already being processed, skipping duplicate message.")
			}
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

func (c *Controller) stopAllWorkers(stage, taskName string) {
	// 根据statusName的值执行不同的停止逻辑
	// 例如，可以根据不同的错误类型来决定是否记录特定的日志，或者是通知某些特定的工作线程停止
	log.Printf("Stopping all workers for stage: %s, task: %s", stage, taskName)

	// 假设Data字段需要包含停止工作的指令
	data := fmt.Sprintf("Stop all workers stage: %s, task: %s", stage, taskName)

	// 初始化Message结构体实例
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    data,
	}

	separator := "-"
	// 找到最后一个分隔符的索引
	index := strings.LastIndex(taskName, separator)
	subscription := taskName[:index]
	// 生成订阅主题名称
	masterTopic := subscription + "-master"

	err := c.publishToNATS(masterTopic, msg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", masterTopic, err)
	} else {
		log.Printf("Message published to %s", masterTopic)
	}
}

func (c *Controller) handleDiagnostics(stage, statusWatchName string) {
	// 随机决定检测结果
	passed := rand.Intn(2) // 0表示不通过，1表示通过
	if passed == 0 {
		// 不通过，随机给出节点
		podName, err := c.getRandomPodName(statusWatchName)
		if err != nil {
			log.Printf("Failed to get random pod name for %s diagnostics: %v", stage, err)
			return
		}
		log.Printf("%s diagnostics failed. Cleaning up pod: %s", stage, podName)

		// 这里假设 c.cleanUpPodOnNode 实际上是根据 Pod 名称来清理 Pod
		// 你可能需要根据实际情况调整这个函数的名称和实现
		c.cleanUpPod(podName) // 假设这是一个接受 Pod 名称并执行清理操作的函数
	} else {
		// 通过检测
		log.Printf("%s diagnostics passed. No action required.", stage)
		// 重启任务
		c.restartTask(stage, statusWatchName) // 假设这是一个根据阶段重启任务的函数
	}
}

func (c *Controller) getRandomPodName(statusWatchName string) (string, error) {
	// 假设 c.swclientset 已经被初始化为指向您的 CRD 客户端
	sw, err := c.swclientset.StatuswatchV1().StatusWatches("default").Get(context.TODO(), statusWatchName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get StatusWatch %s: %v", statusWatchName, err)
	}

	if len(sw.Spec.Workers) == 0 {
		return "", fmt.Errorf("no workers (Pods) found in StatusWatch %s", statusWatchName)
	}

	// 从 Workers 列表中随机选择一个 Pod 名字
	// 请确保 rand.Seed 在程序初始化时被调用（比如在 main 函数或 init 函数中）
	selectedWorker := sw.Spec.Workers[rand.Intn(len(sw.Spec.Workers))]
	return selectedWorker.Name, nil
}

// cleanUpPod cleans up a pod with the given name in the default namespace.
func (c *Controller) cleanUpPod(podName string) {
	log.Printf("Cleaning up pod %s in the default namespace...", podName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Using "default" namespace directly here
	err := c.kubeclientset.CoreV1().Pods("default").Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		log.Printf("Failed to delete pod %s in the default namespace: %v", podName, err)
		return
	}

	log.Printf("Pod %s in the default namespace cleaned up successfully", podName)
}

// 重启任务函数
func (c *Controller) restartTask(stage, statusWatchName string) {
	log.Printf("Restarting tasks for stage: %s", stage)
	// 将stage写入Data字段，这里使用了简单的字符串格式化
	data := "restart: " + stage // 例如，这会生成 "restart:build-stage" 如果 stage 是 "build-stage"

	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    data,
	}
	// 发送消息到statusWatch.TaskName + "-master"

	separator := "-"
	// 找到最后一个分隔符的索引
	index := strings.LastIndex(statusWatchName, separator)
	subscription := statusWatchName[:index]
	// 生成订阅主题名称
	masterTopic := subscription + "-master"

	err := c.publishToNATS(masterTopic, msg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", masterTopic, err)
	} else {
		log.Printf("Message published to %s", masterTopic)
	}
	// 这里添加实际的任务重启逻辑
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

		separator := "-"
		// 找到最后一个分隔符的索引
		index := strings.LastIndex(statuswatch.Name, separator)
		subscription := statuswatch.Name[:index]
		// 生成订阅主题名称
		masterTopic := subscription + "-master"

		err := c.publishToNATS(masterTopic, msg)
		if err != nil {
			log.Printf("Failed to publish message to %s: %v", masterTopic, err)
		} else {
			log.Printf("Message published to %s", masterTopic)
		}
	}

	return successfulSubscriptions
}

func (c *Controller) generateAndDistributePublicKey(statuswatch *myappv1.StatusWatch) (nkeys.KeyPair, nkeys.KeyPair, error) {
	// 生成public key
	accountUkp, usrUkp, err := generateUserNKeys()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate user NKeys: %v", err)
	}

	// 把userPublicKey保存起来,用键值对,如果有status更新,我们就把对应的userPublicKey取出来
	c.publicKeyMap.Store(statuswatch.Name, usrUkp)
	c.accountPublicKeyMap.Store(statuswatch.Name, accountUkp)

	// 更新 nats-auth 的 Secret
	err = updateNATSAuthSecret(c.kubeconfig, statuswatch.Name, accountUkp, usrUkp)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to update NATS Auth Secret: %v", err)
	}
	fmt.Println("NATS Auth Secret 已更新")

	return accountUkp, usrUkp, nil
}

func (c *Controller) connectAndSubscribeWorkers(statuswatch *myappv1.StatusWatch, accountUkp nkeys.KeyPair, workers []myappv1.WorkerSpec, ukp nkeys.KeyPair) int {
	var wg sync.WaitGroup
	wg.Add(len(workers))
	mu := sync.Mutex{} // 用于保护successfulSubscriptions变量
	successfulSubscriptions := 0

	for _, worker := range workers {
		go func(worker myappv1.WorkerSpec) {
			defer wg.Done()

			// 建立GRPC连接并订阅任务主题
			if c.subscribeWorker(worker, accountUkp, ukp, statuswatch.Name) {
				mu.Lock()
				successfulSubscriptions++
				mu.Unlock()
			}
		}(worker)
	}

	wg.Wait()

	log.Printf("Successfully subscribed workers: %d", successfulSubscriptions)

	return successfulSubscriptions
}

func (c *Controller) subscribeWorker(worker myappv1.WorkerSpec, accountUkp, ukp nkeys.KeyPair, taskName string) bool {
	headlessService := getHeadlessServiceName()

	// 构建 Pod 的 DNS 名称并尝试连接
	conn, err := c.connectToWorker(worker, headlessService)
	if err != nil {
		log.Printf("Failed to connect to worker %s: %v", worker.Name, err)
		return false
	}
	defer conn.Close()

	// 更新工作配置，如果失败则返回false
	if !c.updateWorkerConfig(conn, worker, accountUkp, ukp) {
		return false
	}

	// 订阅任务
	return c.subscribeToTask(conn, worker, taskName)
}

func (c *Controller) connectToWorker(worker myappv1.WorkerSpec, headlessService string) (*grpc.ClientConn, error) {
	const maxRetries = 3
	const retryDelay = 5 * time.Second
	const connTimeout = 10 * time.Second

	podDNS := fmt.Sprintf("%s.%s.default.svc.cluster.local", worker.Name, headlessService)

	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), connTimeout)
		conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", podDNS, 8888), grpc.WithInsecure(), grpc.WithBlock())
		cancel()
		if err == nil {
			return conn, nil
		}
		log.Printf("Failed to connect to worker %s on attempt %d: %v", worker.Name, i+1, err)
		time.Sleep(retryDelay)
	}

	return nil, fmt.Errorf("failed to connect to worker %s after %d attempts", worker.Name, maxRetries)
}

func getHeadlessServiceName() string {
	headlessService := os.Getenv("HEADLESS_SERVICE_NAME")
	if headlessService == "" {
		headlessService = "grpc-service"
		log.Printf("HEADLESS_SERVICE_NAME not set, using default: %s", headlessService)
	}
	return headlessService
}

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

func (c *Controller) publishReadyMessage(taskName string) {

	// 创建消息
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    "warmup",
	}
	separator := "-"
	// 找到最后一个分隔符的索引
	index := strings.LastIndex(taskName, separator)
	subscription := taskName[:index]
	// 生成订阅主题名称
	masterTopic := subscription + "-master"

	// 发送消息到NATS主题
	err := c.publishToNATS(masterTopic, msg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", masterTopic, err)
	} else {
		log.Printf("Message published to %s", masterTopic)
	}
}

func generateUserNKeys() (nkeys.KeyPair, nkeys.KeyPair, error) {
	// 创建用户的密钥对
	accountKP, err := nkeys.CreateAccount()
	if err != nil {
		return nil, nil, err
	}

	userKP, err := nkeys.CreateUser()
	if err != nil {
		return nil, nil, err
	}

	return accountKP, userKP, nil
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

func updateNATSAuthSecret(config *rest.Config, statusWatchName string, accUkp nkeys.KeyPair, ukp nkeys.KeyPair) error {
	// 加载kubeconfig
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	// 更新nats-auth的Secret
	secret, err := clientset.CoreV1().Secrets("default").Get(context.Background(), "nats-auth", metav1.GetOptions{})
	if err != nil {
		return err
	}

	accountPublicKey, err := accUkp.PublicKey()
	if err != nil {
		return fmt.Errorf("failed to get account public key: %w", err)
	}
	userPublicKey, err := ukp.PublicKey()
	if err != nil {
		return fmt.Errorf("failed to get user public key: %w", err)
	}

	// 更新Secret的数据
	secret.Data[statusWatchName+"-account-public-key"] = []byte(accountPublicKey)
	secret.Data[statusWatchName+"-user-public-key"] = []byte(userPublicKey)
	// 更新Secret
	_, err = clientset.CoreV1().Secrets("default").Update(context.Background(), secret, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleStatusWatchUpdated(newStatuswatch *myappv1.StatusWatch) {
	// 比较新旧StatusWatch的变化,找到发生变化的worker
	updatedWorkers := c.taskUpdatedWorkersMap[newStatuswatch.Name]
	//print len(updatedWorkers)
	log.Printf("Updated workers number for task '%s': %v", newStatuswatch.Name, len(updatedWorkers))
	// 从publicKeyMap中获取保存的Public Key
	ukp, ok := c.publicKeyMap.Load(newStatuswatch.Name)
	if !ok {
		log.Printf("Public Key not found for StatusWatch %s", newStatuswatch.Name)
		return
	}

	AccountUkp, ok := c.accountPublicKeyMap.Load(newStatuswatch.Name)
	if !ok {
		log.Printf("Public Key not found for StatusWatch %s", newStatuswatch.Name)
		return
	}
	// 向发生变化的worker发送更新的配置,并重新订阅任务主题

	successfulSubscriptions := c.connectAndSubscribeWorkers(newStatuswatch, AccountUkp.(nkeys.KeyPair), updatedWorkers, ukp.(nkeys.KeyPair))

	// 检查是否所有发生变化的worker都成功订阅了任务主题
	if successfulSubscriptions == len(updatedWorkers) {
		log.Println("All updated workers resubscribed successfully")
		// 向statusWatch.TaskName + "-master"发送消息
		c.publishRetrainMessage(newStatuswatch.Name)
	}
}

func getUpdatedWorkers(oldStatuswatch, newStatuswatch *myappv1.StatusWatch) []myappv1.WorkerSpec {
	oldWorkers := make(map[string]myappv1.WorkerSpec)

	// 使用工作单元的Name作为键
	for _, worker := range oldStatuswatch.Spec.Workers {
		oldWorkers[worker.Name] = worker
	}

	var updatedWorkers []myappv1.WorkerSpec
	for _, newWorker := range newStatuswatch.Spec.Workers {
		oldWorker, ok := oldWorkers[newWorker.Name]
		if !ok {
			// 根据Name找不到旧工作单元，说明是新增的
			updatedWorkers = append(updatedWorkers, newWorker)
		} else {
			// 如果Name相同但PodUUID不同，则认为是更新的
			if oldWorker.PodUUID != newWorker.PodUUID {
				updatedWorkers = append(updatedWorkers, newWorker)
			}
		}
	}

	return updatedWorkers
}

// TODO: 重新训练任务
func (c *Controller) publishRetrainMessage(taskName string) {

	// 创建消息
	msg := Message{
		ID:      "all",
		Time:    time.Now().Format(time.RFC3339),
		MsgType: "order",
		Data:    "retrain",
	}

	separator := "-"
	// 找到最后一个分隔符的索引
	index := strings.LastIndex(taskName, separator)
	subscription := taskName[:index]
	// 生成订阅主题名称
	masterTopic := subscription + "-master"

	// 发送消息到NATS主题
	err := c.publishToNATS(masterTopic, msg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", masterTopic, err)
	} else {
		log.Printf("Message published to %s", masterTopic)
	}
}

func (c *Controller) handleStatusWatchDeleted(name string) {
	// 处理StatusWatch删除事件的逻辑
	log.Printf("StatusWatch %s deleted\n", name)

	//取消NATS订阅
	sub, ok := c.natsSubscriptionMap[name]
	if ok {
		err := sub.Unsubscribe()
		if err != nil {
			log.Printf("Failed to unsubscribe from task '%s': %v", name, err)
		}
	}
	delete(c.natsSubscriptionMap, name)
	//删除publicKey
	c.publicKeyMap.Delete(name)
	c.accountPublicKeyMap.Delete(name)
	log.Printf("Cleanup completed for StatusWatch %s", name)
}
