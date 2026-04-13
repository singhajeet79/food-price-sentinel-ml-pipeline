#!/bin/bash

NAMESPACE="food-price-sentinel"
APP_NAME="food-price-sentinel"

case $1 in
  down)
    echo "--- 🛑 Shutting down Food Price Sentinel ---"
    
    # 1. Disable ArgoCD Auto-Sync (Prevents Argo from fighting the script)
    echo "Pausing ArgoCD auto-sync..."
    kubectl patch app $APP_NAME -n argocd --type merge -p '{"spec":{"syncPolicy":null}}'

    # 2. Suspend CronJobs
    echo "Suspending MLOps CronJobs..."
    kubectl patch cronjob retrain -n $NAMESPACE -p '{"spec" : {"suspend" : true}}'
    kubectl patch cronjob drift-check -n $NAMESPACE -p '{"spec" : {"suspend" : true}}'

    # 3. Scale Deployments to 0
    echo "Scaling deployments to zero..."
    kubectl scale deployment --all -n $NAMESPACE --replicas=0
    
    echo "✅ Project is paused. You can now power off Aiven services."
    ;;

  up)
    echo "--- 🚀 Starting up Food Price Sentinel ---"
    echo "Ensure Aiven Services are ACTIVE before proceeding."
    read -p "Press [Enter] if Aiven is ready..."

    # 1. Scale core infrastructure back up (API and Consumer)
    echo "Scaling deployments back to 1..."
    kubectl scale deployment api consumer producer-food producer-energy producer-fertilizer -n $NAMESPACE --replicas=1

    # 2. Resume CronJobs
    echo "Resuming MLOps CronJobs..."
    kubectl patch cronjob retrain -n $NAMESPACE -p '{"spec" : {"suspend" : false}}'
    kubectl patch cronjob drift-check -n $NAMESPACE -p '{"spec" : {"suspend" : false}}'

    # 3. Re-enable ArgoCD Auto-Sync
    echo "Re-enabling ArgoCD GitOps sync..."
    kubectl patch app $APP_NAME -n argocd --type merge -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'

    echo "✅ Project is restoring. Check logs with: kubectl get pods -n $NAMESPACE"
    ;;

  *)
    echo "Usage: ./sentinel-power.sh [up|down]"
    ;;
esac
