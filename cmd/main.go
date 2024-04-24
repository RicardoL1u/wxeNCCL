package main

import (
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	clientset "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/clientset/versioned"
	informers "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/generated/informers/externalversions"
	statuswatchv1 "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/apis/example.com/v1"
	controllers "gitlab.infini-ai.com/mizar/asterism/fault-tolerance/pkg/reconstructed_controllers"
)

func main() {
	// 创建 scheme 注册我们的自定义资源
	var scheme = runtime.NewScheme()
	utilruntime.Must(statuswatchv1.AddToScheme(scheme))

	// 在集群内部运行,使用 InClusterConfig
	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	// 创建 Kubernetes 标准客户端集
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	// 创建我们自定义资源的客户端集
	swClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building example clientset: %s", err.Error())
	}

	// 创建我们的自定义资源的 informer 工厂
	swInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		swClient,
		time.Second*30,
		informers.WithNamespace("kubeflow"), // 这里指定仅监听 'kubeflow' 命名空间下的资源
	)

	// 初始化控制器
	controller := controllers.NewController(kubeClient, swClient, swInformerFactory.Statuswatch().V1().StatusWatches(), cfg)

	// 开启 informer,开始监听资源事件
	stopCh := make(chan struct{})
	swInformerFactory.Start(stopCh)

	// 运行控制器
	if err = controller.Run(2, stopCh); err != nil {
		klog.Fatalf("Error running controller: %s", err.Error())
	}
}
