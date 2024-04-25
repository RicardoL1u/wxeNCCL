package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	// 使用 kubeconfig 获取 Kubernetes 配置
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatal(err)
	}

	// 创建 dynamic client
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// 定义 PyTorchJob 的 JSON
	pytorchJob := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "kubeflow.org/v1",
			"kind":       "PyTorchJob",
			"metadata": map[string]interface{}{
				"name":      "pytorchjob-example",
				"namespace": "kubeflow",
			},
			"spec": map[string]interface{}{
				"pytorchReplicaSpecs": map[string]interface{}{
					"Master": map[string]interface{}{
						"replicas":      1,
						"restartPolicy": "OnFailure",
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"containers": []map[string]interface{}{
									{
										"name":  "pytorch",
										"image": "pytorch/pytorch:1.7.1",
										"command": []string{
											"python", "-c",
											"import torch; print('PyTorch Version:', torch.__version__); print('Hello, PyTorch!')",
										},
									},
								},
							},
						},
					},
					"Worker": map[string]interface{}{
						"replicas":      2,
						"restartPolicy": "OnFailure",
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"containers": []map[string]interface{}{
									{
										"name":  "pytorch",
										"image": "pytorch/pytorch:1.7.1",
										"command": []string{
											"python", "-c",
											"import torch; print('PyTorch Version:', torch.__version__); print('Hello, PyTorch from Worker!')",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// 创建 PyTorchJob
	gvr := schema.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "pytorchjobs"}
	result, err := dynamicClient.Resource(gvr).Namespace("kubeflow").Create(context.TODO(), pytorchJob, v1.CreateOptions{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Created PyTorchJob %q.\n", result.GetName())
}
