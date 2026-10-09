#!/bin/bash

# AI Monitoring Agent Deployment Script
# This script applies all the necessary Kubernetes manifests for the AI Monitoring Agent

echo "Deploying AI Monitoring Agent with optimized storage configuration..."

# Check if kubectl is available
if ! command -v kubectl &> /dev/null
then
    echo "kubectl could not be found. Please install kubectl and try again."
    exit 1
fi

# Check if we can connect to the cluster
if ! kubectl cluster-info &> /dev/null
then
    echo "Cannot connect to Kubernetes cluster. Please ensure you have proper kubeconfig."
    exit 1
fi

echo "Applying ConfigMap..."
kubectl apply -f configmap.yaml

echo "Applying Service..."
kubectl apply -f service.yaml

echo "Applying Deployment..."
kubectl apply -f deployment.yaml

echo "Applying Horizontal Pod Autoscaler..."
kubectl apply -f hpa.yaml

echo "Checking deployment status..."
kubectl get deployments -n mercury ai-monitoring-agent

echo "Deployment completed!"
echo ""
echo "Monitor the deployment with:"
echo "  kubectl get pods -n mercury"
echo "  kubectl logs -n mercury -l app=ai-monitoring-agent --follow"
echo ""
echo "Access the dashboard at: https://mercury.fabmailers.in/ai-agent"