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
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	clientset "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned"
	"gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned/scheme"
	swScheme "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned/scheme"
	informers "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/informers/externalversions/example.com/v1"
	listers "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/listers/example.com/v1"
	myappv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
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

func getUpdatedWorkers(oldStatuswatch, newStatuswatch *myappv1.StatusWatch) []myappv1.WorkerSpec {
	oldWorkers := mapWorkersByName(oldStatuswatch.Spec.Workers)

	return findUpdates(oldWorkers, newStatuswatch.Spec.Workers)
}

func mapWorkersByName(workers []myappv1.WorkerSpec) map[string]myappv1.WorkerSpec {
	mappedWorkers := make(map[string]myappv1.WorkerSpec)
	for _, worker := range workers {
		mappedWorkers[worker.Name] = worker
	}
	return mappedWorkers
}

func findUpdates(oldWorkers map[string]myappv1.WorkerSpec, newWorkers []myappv1.WorkerSpec) []myappv1.WorkerSpec {
	var updatedWorkers []myappv1.WorkerSpec
	for _, newWorker := range newWorkers {
		if oldWorker, exists := oldWorkers[newWorker.Name]; !exists || oldWorker.PodUUID != newWorker.PodUUID {
			updatedWorkers = append(updatedWorkers, newWorker)
		}
	}
	return updatedWorkers
}

func (c *Controller) handleStatusWatchCreated(statuswatch *myappv1.StatusWatch) {
	fmt.Printf("Number of workers: %d\n", statuswatch.Spec.Number)

	if err := c.setupWorkers(statuswatch); err != nil {
		log.Printf("Failed during worker setup: %v", err)
		return
	}
	successfulACK := 0
	c.InitializeSubscriptions(statuswatch, successfulACK)
	if c.verifyWorkerSubscriptions(statuswatch) {
		c.publishReadyMessage(statuswatch.Name)
	}
}

func (c *Controller) setupWorkers(statuswatch *myappv1.StatusWatch) error {
	accountUkp, ukp, err := c.generateAndDistributePublicKey(statuswatch)
	if err != nil {
		return err
	}

	log.Println("Waiting for 10 seconds before connecting and subscribing workers...")
	time.Sleep(10 * time.Second)

	successfulSubscriptions := c.connectAndSubscribeWorkers(statuswatch, accountUkp, statuswatch.Spec.Workers, ukp)
	if successfulSubscriptions != int(statuswatch.Spec.Number) {
		return fmt.Errorf("not all workers connected: expected %d, got %d", statuswatch.Spec.Number, successfulSubscriptions)
	}

	return nil
}

func (c *Controller) verifyWorkerSubscriptions(statuswatch *myappv1.StatusWatch) bool {
	return c.connectAndSubscribeWorkers(statuswatch, nil, nil, nil) == int(statuswatch.Spec.Number)
}

func (c *Controller) handleStatusWatchDeleted(name string) {
	log.Printf("StatusWatch %s deleted\n", name)

	if sub, ok := c.natsSubscriptionMap[name]; ok {
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("Failed to unsubscribe from task '%s': %v", name, err)
		}
		delete(c.natsSubscriptionMap, name)
	}

	c.publicKeyMap.Delete(name)
	c.accountPublicKeyMap.Delete(name)
	log.Printf("Cleanup completed for StatusWatch %s", name)
}

func (c *Controller) handleStatusWatchUpdated(newStatuswatch *myappv1.StatusWatch) {
	// 比较新旧StatusWatch的变化,找到发生变化的worker
	updatedWorkers := c.taskUpdatedWorkersMap[newStatuswatch.Name]
	log.Printf("Updated workers number for task '%s': %v", newStatuswatch.Name, len(updatedWorkers))

	ukp, _ := c.publicKeyMap.Load(newStatuswatch.Name)
	accountUkp, _ := c.accountPublicKeyMap.Load(newStatuswatch.Name)

	if successfulSubscriptions := c.connectAndSubscribeWorkers(newStatuswatch, accountUkp.(nkeys.KeyPair), updatedWorkers, ukp.(nkeys.KeyPair)); successfulSubscriptions == len(updatedWorkers) {
		log.Println("All updated workers resubscribed successfully")
		c.publishRetrainMessage(newStatuswatch.Name)
	}
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

func getHeadlessServiceName() string {
	headlessService := os.Getenv("HEADLESS_SERVICE_NAME")
	if headlessService == "" {
		headlessService = "grpc-service"
		log.Printf("HEADLESS_SERVICE_NAME not set, using default: %s", headlessService)
	}
	return headlessService
}
