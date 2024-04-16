#!/bin/bash

kubectl delete statefulset test
kubectl delete service ddp-master
kubectl delete pvc task-manager-pvc
kubectl delete service grpc-service
kubectl delete statuswatch test-statuswatch
go run start/start.go -taskName="test" -taskWorker=4