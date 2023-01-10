#!/bin/sh

# Kill any previous demo pod instances first
kubectl delete pod dbspmanager

# Login to the demo pod terminal with an environment variable pointing to the Redpanda brokers
kubectl run -i --tty dbspmanager --image=localhost:5001/dbspmanager --restart=Never --env="REDPANDA_BROKERS=`kubectl get po redpanda-0 --template '{{.status.podIP}}'`:9093"
