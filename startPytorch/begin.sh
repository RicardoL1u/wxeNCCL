#!/bin/bash

kubectl delete pytorchjob test -n kubeflow  
kubectl delete service ddp-master -n kubeflow 
kubectl delete pvc task-manager-pvc -n kubeflow 
kubectl delete service grpc-service -n kubeflow 
kubectl delete statuswatch test-statuswatch -n kubeflow 
kubectl delete ServiceAccount test-service-account -n kubeflow
kubectl delete Role pod-reader -n kubeflow
kubectl delete RoleBinding read-pods -n kubeflow