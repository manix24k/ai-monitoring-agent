# AI Monitoring Agent - Storage Optimized Deployment

## Changes Made

### 1. Reduced Storage Requirements
- Changed ephemeral storage requests from 5Gi to 3Gi
- Changed ephemeral storage limits from 10Gi to 5Gi
- Set emptyDir volume size limit to 5Gi

### 2. Optimized Model Handling
- Removed model download from Docker build process to avoid build-time storage issues
- Added runtime model download mechanism in the application
- Implemented model download on first access to reduce storage pressure during startup

### 3. Environment Configuration
- Added OLLAMA_MODELS environment variable to explicitly set model storage path
- Created models directory in Dockerfile

## Deployment Instructions

1. Apply the updated Kubernetes manifests:
   ```bash
   kubectl apply -f k8s/configmap.yaml
   kubectl apply -f k8s/service.yaml
   kubectl apply -f k8s/deployment.yaml
   kubectl apply -f k8s/hpa.yaml
   ```

2. The Phi-3 model will be automatically downloaded on first access to the dashboard or API

3. Monitor pod status:
   ```bash
   kubectl get pods -n mercury
   kubectl describe pod -n mercury <pod-name>
   ```

## Key Improvements

1. **Reduced Storage Footprint**: From 10Gi to 5Gi maximum storage requirement
2. **Deferred Model Loading**: Model downloads only when needed, not during build
3. **Better Resource Management**: Explicit storage limits prevent eviction
4. **Maintained Functionality**: All AI features work as expected

## Troubleshooting

If you encounter storage issues:
1. Check available node storage: `kubectl describe nodes | grep -A 3 -B 3 ephemeral-storage`
2. Monitor pod storage usage: `kubectl top pod -n mercury <pod-name> --containers`
3. Check logs for model download progress: `kubectl logs -n mercury <pod-name>`