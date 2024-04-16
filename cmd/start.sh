#!/bin/bash

kubectl delete deployment statuswatch-controller
kubectl apply -f masterPod.yaml