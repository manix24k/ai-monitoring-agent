# Storage-Optimized Deployment Strategy

## Current Issue
Image pull failing due to "no space left on device" when extracting torch library (~1-2GB)

## Solution: Multi-Stage Lightweight Approach

### Phase 1: Basic Monitoring Agent (Lightweight)
- Remove heavy ML dependencies (torch, transformers)
- Focus on direct service log monitoring
- Include only essential libraries for monitoring and dashboard
- Significantly reduce image size

### Phase 2: Advanced ML Features (Optional)
- Deploy ML components separately if needed
- Use the lightweight agent as the primary monitor
- Add ML analysis as a secondary service

## Implementation Steps

1. **Use requirements-light.txt** - Excludes torch and transformers
2. **Optimized Dockerfile** - Installs only essential dependencies
3. **Service Log Monitoring** - Core functionality without heavy ML deps
4. **Dashboard and Alerting** - Full functionality minus LLM features

## Dependencies Removed
- `torch==2.0.1` (~1-2GB)
- `transformers==4.33.0` (~500MB+)

## Remaining Essential Dependencies
- prometheus-client
- requests
- slack-sdk
- scikit-learn
- numpy
- pandas
- flask
- scipy
- ollama
- elasticsearch

## Benefits
- Image size reduced by ~1.5-2GB
- Faster deployment and updates
- No storage eviction issues
- Core monitoring functionality preserved
- Can add ML features later as needed