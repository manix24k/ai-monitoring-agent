# AI SRE Agent

An autonomous Site Reliability Engineering agent for Kubernetes-based microservices. It monitors production workloads, detects anomalies, performs root cause analysis via local LLMs, generates code fixes, opens GitHub PRs, and sends consolidated Slack reports — all without human intervention.

---

## Table of Contents

- [Overview](#overview)
- [Architecture Map](#architecture-map)
- [Components](#components)
- [Full Workflow](#full-workflow)
  - [1. Anomaly Detection & RCA](#1-anomaly-detection--rca)
  - [2. CodeXA — Automated Code Fix Pipeline](#2-codexa--automated-code-fix-pipeline)
  - [3. K8s Agent — Infrastructure Auto-Repair](#3-k8s-agent--infrastructure-auto-repair)
  - [4. Rollout Monitor](#4-rollout-monitor)
  - [5. Slack Notifications](#5-slack-notifications)
- [Tech Stack](#tech-stack)
- [Kubernetes Resources](#kubernetes-resources)
- [Deployment](#deployment)
- [Secrets Reference](#secrets-reference)
- [Environment Variables](#environment-variables)
- [API Endpoints](#api-endpoints)
- [Namespaces & Services Monitored](#namespaces--services-monitored)
- [LLM Strategy](#llm-strategy)

---

## Overview

AI SRE Agent (`ai-sre-agent`) is a production-grade autonomous monitoring and remediation system deployed at Fabhotels. The same agent stack — Flask dashboard, CodeXA engine, K8s agent, Rollout monitor, Ollama, Redis, and all supporting components — is deployed identically in both the `jupiter` and `venus` namespaces.

**What it does:**

| Capability | Description |
|---|---|
| Anomaly Detection | ML-based detection of latency, error rate, CPU, memory spikes |
| Root Cause Analysis (RCA) | 3-tier LLM-powered RCA: pattern matching → learned cache → full LLM |
| CodeXA Auto-Fix | Detects code-level exceptions, generates LLM fixes, opens GitHub PRs |
| Infrastructure Repair | Detects K8s manifest issues (missing secrets, port mismatches, YAML errors), auto-patches |
| Rollout Monitor | Detects stuck/partial deployment rollouts and clears stale warnings |
| Slack Reports | One consolidated doc-style message per service (not per issue) |
| Self-Learning | Stores RCA outcomes in Redis; reuses patterns to avoid repeat LLM calls |
| Web Dashboard | Real-time monitoring UI at `/ai-agent` with metrics, issues, and fix status |

---

## Architecture Map

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                          FABHOTELS KUBERNETES CLUSTER                           ║
║                              (jupiter / venus)                                  ║
║                                                                                  ║
║  ┌─────────────── jupiter/venus namespace ────────────────────────────────────┐ ║
║  │                                                                             │ ║
║  │   ┌────────────────────────────────────────────────────────────────────┐   │ ║
║  │   │                    ai-monitoring-agent (Pod)                        │   │ ║
║  │   │                                                                     │   │ ║
║  │   │  ┌─────────────────┐    ┌──────────────────┐    ┌───────────────┐  │   │ ║
║  │   │  │  Flask Dashboard │    │  main.py (Agent)  │    │  k8s_agent.py │  │   │ ║
║  │   │  │  web_dashboard  │    │  Prometheus/ES    │    │  LLM K8s Fix  │  │   │ ║
║  │   │  │  port :5000     │    │  Anomaly Detect   │    │  YAML Repair  │  │   │ ║
║  │   │  └────────┬────────┘    └────────┬─────────┘    └───────┬───────┘  │   │ ║
║  │   │           │                      │                       │          │   │ ║
║  │   │           ▼                      ▼                       ▼          │   │ ║
║  │   │  ┌─────────────────┐    ┌──────────────────┐    ┌───────────────┐  │   │ ║
║  │   │  │  CodeXA Engine  │    │  RolloutMonitor   │    │  InfraFix     │  │   │ ║
║  │   │  │  detect→analyze │    │  Watches rolling  │    │  Audit K8s    │  │   │ ║
║  │   │  │  →fix→PR        │    │  deployments      │    │  manifests    │  │   │ ║
║  │   │  └────────┬────────┘    └──────────────────┘    └───────────────┘  │   │ ║
║  │   │           │                                                          │   │ ║
║  │   │  ┌────────▼────────┐    ┌──────────────────┐    ┌───────────────┐  │   │ ║
║  │   │  │  SlackNotifier  │    │  LearningEngine   │    │  OtelCollect  │  │   │ ║
║  │   │  │  Batch 2-min    │    │  Redis patterns   │    │  Sidecar      │  │   │ ║
║  │   │  │  per service    │    │  L0/L1 cache      │    │  port :4317   │  │   │ ║
║  │   │  └─────────────────┘    └──────────────────┘    └───────────────┘  │   │ ║
║  │   └──────────────────────────────────────────────────────────────────────┘   │ ║
║  │                                                                             │ ║
║  │   ┌──────────────────┐    ┌──────────────────┐                             │ ║
║  │   │  Ollama (Pod)    │    │  Redis (Pod)      │                             │ ║
║  │   │  :11434          │    │  :6380            │                             │ ║
║  │   │  qwen2.5:1.5b    │    │  RCA patterns     │                             │ ║
║  │   │  qwen2.5:7b-q4   │    │  learning store   │                             │ ║
║  │   └──────────────────┘    └──────────────────┘                             │ ║
║  └─────────────────────────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════════════╝

External Integrations:
  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
  │ Elasticsearch│  │  Prometheus  │  │   GitHub     │  │   Jenkins    │
  │  (external)  │  │ (monitoring) │  │ fabhotelstech│  │ ci.fabmailers│
  │ Logs & traces│  │ Metrics      │  │ PRs & code   │  │ AI-AGENT-BOT │
  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
  │    ArgoCD    │  │    Slack     │  │   SigNoz     │
  │ GitOps deploy│  │  #codexa-    │  │ Distributed  │
  │ sync status  │  │  alerts      │  │ tracing      │
  └──────────────┘  └──────────────┘  └──────────────┘
```

---

## Components

### Core Agent (`main.py`)
The `AIMonitoringAgent` class is the central orchestrator. It runs continuous monitoring loops, collects metrics from Prometheus, retrieves logs from Elasticsearch, feeds data to the anomaly detector and root cause analyzer, and updates the learning engine with outcomes.

### Web Dashboard (`web_dashboard.py`)
Flask web application serving the UI at port 5000. Also hosts all background worker threads (CodeXA pipeline, K8s-agent scan loops, infra-audit). All Flask routes live here — `codexa/app.py` is the FastAPI layer for CodeXA API only.

### CodeXA Engine (`codexa/`)
Four-stage pipeline for automated code issue detection and remediation:
- `codexa/services/detector.py` — polls the agent API + Elasticsearch for code-level exceptions
- `codexa/services/analyzer.py` — clones the affected repo, collects context, runs LLM analysis
- `codexa/services/fixer.py` — parses LLM output, generates unified diffs
- `codexa/services/git_ops.py` — commits, pushes, and opens PRs on GitHub

### K8s Agent (`k8s_agent.py`)
Scans all pods in watched namespaces for unhealthy states. Uses Ollama LLM to diagnose failures, generates Kubernetes YAML fixes, and opens PRs against the `k8s-manifest` repo. All Ollama calls are serialized through a semaphore to prevent connection drops from concurrent requests.

### Rollout Monitor (`rollout_monitor.py`)
Watches Kubernetes deployments for partial rollouts (e.g., 1/2 pods ready during a rolling update). Issues a `warning`-level alert if a rollout is stuck. Warning-level issues expire after 60 seconds once resolved; critical issues expire after 600 seconds.

### Infra Fix (`infra_fix.py`)
Audits Kubernetes manifests for structural issues:
- `VaultSecretMissing` — referenced secrets don't exist
- `PortMismatch` — service port doesn't match container port
- `YamlSyntax` — malformed manifest YAML
- `SelectorMismatch` — deployment selector doesn't match pod labels

### Learning Engine (`learning_engine.py`)
Stores RCA results, error signatures, and fix outcomes in Redis. Enables a 3-tier lookup:
- **L0** — exact signature match (instant, no LLM)
- **L1** — learned pattern match (fast, no LLM)
- **L2** — full LLM analysis (slow, with Ollama)

### Exact RCA (`exact_rca.py`)
Three-tier root cause analysis system. Tries L0/L1 first; escalates to L2 only when necessary. Feeds results back into the learning store for future L0/L1 hits.

### Slack Notifier (`slack_notifier.py`)
Sends Slack Block Kit messages. The `send_codexa_service_report()` method sends ONE consolidated message per service (never per issue) after a 2-minute batching window.

---

## Full Workflow

### 1. Anomaly Detection & RCA

```
Every 60s:
  Prometheus ──► anomaly_detector.py
                  │ Scikit-learn IsolationForest
                  │ Detects: latency spikes, error rate spikes,
                  │          CPU/memory anomalies
                  ▼
              root_cause_analyzer.py
                  │ Correlates: metrics + ES logs + K8s state
                  │ Queries Elasticsearch for stack traces
                  ▼
              exact_rca.py (3-tier RCA)
                  │ L0: exact Redis cache hit? ──► return cached result
                  │ L1: learned pattern match? ──► return pattern result
                  │ L2: call Ollama (qwen2.5:1.5b) for full LLM analysis
                  ▼
              learning_engine.py
                  │ Store result in Redis for future L0/L1 reuse
                  ▼
              web_dashboard.py
                  │ Store incident in memory
                  │ Expose via /api/incidents
                  ▼
              slack_notifier.py (if configured)
                  └─► Send consolidated service report
```

### 2. CodeXA — Automated Code Fix Pipeline

```
Every 60s — CodeXA worker loop in web_dashboard.py:

  DETECT
  ──────
  codexa/services/detector.py
    │ Poll /ai-agent/api/issues (internal)
    │ OR query Elasticsearch directly for stack traces
    │ Filter: only code-level exceptions (NullPointerException,
    │         BeanCreationException, SQL errors, etc.)
    │ Deduplicate by error signature + service
    ▼
  Issue stored with status: DETECTED

  ANALYZE
  ───────
  codexa/services/analyzer.py
    │ Clone repo: github.com/fabhotelstech/<service-repo>
    │ Checkout branch: azure_migration
    │ Collect context:
    │   - Full stack trace from logs
    │   - Relevant source files (max 2 files, by class name in trace)
    │   - Recent error occurrences
    │ Build LLM prompt with context
    │ Call Ollama: qwen2.5:7b-instruct-q4_K_M (via semaphore)
    │ LLM returns: root cause, affected lines, suggested fix
    ▼
  Issue status: ANALYZED

  FIX
  ───
  codexa/services/fixer.py
    │ Parse LLM output for code blocks
    │ Generate unified diff (before/after)
    │ Confidence check: minimum 0.6 (60%)
    │ Apply changes to local workspace
    ▼
  Issue status: FIX_READY
    │
    ├─► _slack_enqueue(issue, fix) — start 2-min batch timer

  PR
  ──
  codexa/services/git_ops.py
    │ Commit changes to workspace clone
    │ Push to azure_migration branch on GitHub
    │ gh pr create → fabhotelstech/<service-repo>
    │ PR title: [CodeXA] <service>: <exception_type>
    │ PR body: root cause, evidence, diff, auto-generated notice
    ▼
  Issue status: PR_CREATED
    │
    ├─► _slack_enqueue(issue, fix, pr_url=...) — update batch

  DEPLOY (external — not automated by this agent)
  ──────
  Developer reviews PR on GitHub
    └─► Merge PR
          └─► Jenkins picks up (or manual trigger)
                └─► ArgoCD syncs new image to venus namespace
```

### 3. K8s Agent — Infrastructure Auto-Repair

```
Every 120s — k8s_agent scan loop:

  kubernetes_service_monitor.py
    │ kubectl get pods -n venus (+ other watched namespaces)
    │ Identify unhealthy pods:
    │   CrashLoopBackOff, OOMKilled, ImagePullBackOff,
    │   Pending (no node), Error state
    ▼
  For each failing pod (serialized via semaphore):
    │
    ├─► Collect evidence:
    │     kubectl logs <pod> --tail=100
    │     kubectl describe pod <pod>
    │     kubectl get events -n <ns>
    │
    ├─► Build LLM prompt with evidence
    │
    ├─► Call Ollama: qwen2.5:1.5b
    │     Returns: diagnosis + suggested YAML change
    │
    └─► If fix identified:
          Clone k8s-manifest repo (azure branch)
          Apply YAML patch
          Push + open PR on fabhotelstech/k8s-manifest
          PR triggers Jenkins → ArgoCD applies to cluster
```

### 4. Rollout Monitor

```
Every 30s — rollout_monitor.py:

  kubectl get deployments -n venus (+ watched ns)
    │
    └─► For each deployment:
          Check: desired_replicas vs ready_replicas
          If partial (e.g., 1/2 ready):
            │
            ├─► Age check:
            │     < 5 min  → grace period, no alert
            │     5-10 min → WARNING: "partial rollout detected"
            │     > 10 min → CRITICAL: "rollout stuck"
            │
            └─► On recovery (all pods ready):
                  Warning issues: expire after 60s
                  Critical issues: expire after 600s
```

### 5. Slack Notifications

```
_slack_enqueue() called at FIX_READY or PR_CREATED:

  First issue for a service:
    └─► Start 2-minute threading.Timer for this service key (ns/svc)

  Subsequent issues within 2 minutes:
    └─► Add to same buffer (deduplicated by issue ID)

  Timer fires after 2 minutes:
    └─► _slack_flush(svc_key)
          │ Pop all buffered (issue, fix) tuples
          │ Collect all PR URLs for this service
          ▼
        slack_notifier.send_codexa_service_report()
          │ Build ONE Slack Block Kit message:
          │   Header: service name + namespace + total issues
          │   Per issue: exception type, file:line, root cause,
          │              fix description, before/after code, PR link
          │   Footer: dashboard URL + timestamp
          ▼
        POST to SLACK_WEBHOOK_URL
        (one message, no matter how many issues were found)

Infra audit path (separate — hourly cooldown):
  After _infra_audit_results update:
    └─► Direct send (no timer) if last sent > 1 hour ago
          send_codexa_service_report(..., infra_issues=[...])
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Python 3.9 (slim) |
| Web Framework | Flask 2.3.2 |
| CodeXA API | FastAPI 0.104.0 + Uvicorn 0.24.0 |
| ML / Anomaly Detection | Scikit-learn 1.3.0, NumPy, Pandas, SciPy |
| LLM Integration | Ollama (local), Requests for HTTP |
| Caching & State | Redis 5.0.8 via redis-py |
| Task Queue | Celery 4.4.0 (background jobs) |
| Kubernetes | kubectl (in-cluster via ServiceAccount) |
| Git Operations | GitPython + gh CLI |
| Observability | OpenTelemetry Collector sidecar (OTLP :4317/:4318) |
| Metrics Export | Prometheus Client 0.17.1 |
| Log Source | Elasticsearch 8.9.0 |
| Notifications | Slack Incoming Webhooks (Block Kit) |
| Container Registry | Azure Container Registry (fabhotelsdev.azurecr.io) |
| CI/CD | Jenkins + ArgoCD (GitOps) |

---

## Kubernetes Resources

### Namespaces: `jupiter` / `venus`

The same set of resources is deployed in both namespaces.

```
jupiter/venus/
├── Deployment: ai-monitoring-agent
│     Image:    fabhotelsdev.azurecr.io/fabhotels-development/ai-monitoring-agent:<tag>
│     Replicas: 1
│     Requests: 4Gi memory, 1700m CPU, 2Gi ephemeral
│     Limits:   7Gi memory, 2500m CPU, 5Gi ephemeral
│     Sidecar:  otel-collector-sidecar (otel/opentelemetry-collector-contrib:0.102.1)
│
├── Service: ai-monitoring-agent (ClusterIP)
│     80    → 5000  (Flask dashboard)
│     11434 → 11434 (Ollama passthrough)
│     4317  → 4317  (OTLP gRPC)
│     4318  → 4318  (OTLP HTTP)
│
├── Deployment: ollama
│     Image:    ollama/ollama:latest
│     Requests: 3 CPU, 5Gi memory
│     Limits:   4 CPU, 8Gi memory
│     PVC:      ollama-models (10Gi)
│     Models pre-pulled on startup:
│               qwen2.5:1.5b
│               qwen2.5:7b-instruct-q4_K_M
│
├── Deployment: ai-monitoring-agent-redis
│     Image:    redis:7.2-alpine
│     Port:     6380 (external) → 6379 (internal)
│     Requests: 100m CPU, 128Mi memory
│     Limits:   500m CPU, 512Mi memory
│
├── PersistentVolumeClaim: ai-monitoring-agent-otel-pvc-v2
│     Mount: /var/otel
│     Used for: RCA learning store + CodeXA workspace
│
├── Secrets: ai-agent-service-github, ai-monitoring-agent-jenkins,
│            ai-agent-argocd-secret, ai-monitoring-agent-secrets,
│            slack-secret, codex-gpt-secret
│
├── ServiceAccount: ai-monitoring-agent
├── ClusterRole: ai-monitoring-agent-role
│     Resources: pods, pods/log, services, namespaces,
│                deployments, replicasets, events
│     Verbs:     get, list, watch
└── ClusterRoleBinding: ai-monitoring-agent-binding
```

---

## Deployment

This project uses **push-only deployment** — code is pushed here, Jenkins builds the image, ArgoCD deploys it. Do not build locally.

### 1. Push code changes

```bash
git clone git@github.com:fabhotelstech/ai-sre-agent.git
cd ai-sre-agent
# make changes
git push origin main
```

### 2. Jenkins builds the image

Jenkins job `AI-AGENT-BOT` picks up the push, builds the Docker image, and tags it.

### 3. ArgoCD syncs

ArgoCD watches `fabhotelstech/k8s-manifest` and applies changes to both `jupiter` and `venus` namespaces automatically.

### 4. Create required secrets (first-time setup)

Run for both `jupiter` and `venus` namespaces:

```bash
for NS in jupiter venus; do

  # GitHub token for CodeXA PR creation
  kubectl create secret generic ai-agent-service-github -n $NS \
    --from-literal=GITHUB_TOKEN=ghp_...

  # Jenkins credentials
  kubectl create secret generic ai-monitoring-agent-jenkins -n $NS \
    --from-literal=JENKINS_JOB_TOKEN=... \
    --from-literal=JENKINS_USER=... \
    --from-literal=JENKINS_TOKEN=...

  # ArgoCD credentials
  kubectl create secret generic ai-agent-argocd-secret -n $NS \
    --from-literal=argocd-url=https://argocd.fabhotels.com \
    --from-literal=argocd-token=...

  # Elasticsearch + LLM API credentials
  kubectl create secret generic ai-monitoring-agent-secrets -n $NS \
    --from-literal=elasticsearch-username=... \
    --from-literal=elasticsearch-password=... \
    --from-literal=AI_API_KEY=...

  # Slack notifications (optional — pod starts without this)
  kubectl create secret generic slack-secret -n $NS \
    --from-literal=SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... \
    --from-literal=SLACK_CHANNEL=#codexa-alerts

done
```

### 5. Verify deployment

```bash
# Check agent pods in both namespaces
kubectl get pods -n jupiter
kubectl get pods -n venus

# Check agent logs (run for whichever namespace you need)
kubectl logs -l app=ai-monitoring-agent -n jupiter -f
kubectl logs -l app=ai-monitoring-agent -n venus -f

# Port-forward dashboard (jupiter or venus)
kubectl port-forward svc/ai-monitoring-agent 5000:80 -n jupiter
# or: kubectl port-forward svc/ai-monitoring-agent 5001:80 -n venus
# Open: http://localhost:5000/ai-agent
```

---

## Secrets Reference

| Secret Name | Key | Used By |
|---|---|---|
| `ai-agent-service-github` | `GITHUB_TOKEN` | CodeXA PR creation, K8s-agent PRs |
| `ai-monitoring-agent-jenkins` | `JENKINS_JOB_TOKEN`, `JENKINS_USER`, `JENKINS_TOKEN` | Jenkins job dispatch |
| `ai-agent-argocd-secret` | `argocd-url`, `argocd-token` | ArgoCD sync status |
| `ai-monitoring-agent-secrets` | `elasticsearch-username`, `elasticsearch-password`, `AI_API_KEY`, `GEMINI_API_KEY` | Elasticsearch auth, LLM API |
| `slack-secret` | `SLACK_WEBHOOK_URL`, `SLACK_CHANNEL` | Slack notifications (optional) |
| `codex-gpt-secret` | `CODEX_GPT_API_KEY`, `CODEX_GPT_BASE_URL`, `CODEX_GPT_MODEL` | Fallback LLM (optional) |

All secrets are marked `optional: true` in `deployment.yaml` — the pod starts normally if they are absent.

---

## Environment Variables

### Core

| Variable | Value | Description |
|---|---|---|
| `FLASK_ENV` | `production` | Flask mode |
| `AI_API_URL` | `http://20.244.11.93/v1` | Internal LLM API endpoint |
| `AI_MODEL_NAME` | `qwen3` | Active model name shown in dashboard |
| `AI_API_TIMEOUT` | `20` | LLM API request timeout (seconds) |

### Ollama (Local LLM)

| Variable | Value | Description |
|---|---|---|
| `OLLAMA_ENABLED` | `true` | Enable local Ollama inference |
| `OLLAMA_MODEL` | `qwen2.5:1.5b` | Fast model for RCA + K8s analysis |
| `OLLAMA_HOST` | `http://ollama:11434` | Ollama service address |

### CodeXA Pipeline

| Variable | Value | Description |
|---|---|---|
| `CODEXA_LLM_PROVIDER` | `ollama` | LLM backend for code fixes |
| `CODEXA_LLM_MODEL` | `qwen2.5:7b-instruct-q4_K_M` | Larger model for code analysis |
| `CODEXA_LLM_HOST` | `http://ollama:11434` | Ollama endpoint for CodeXA |
| `CODEXA_LLM_TIMEOUT` | `900` | 15-minute timeout for complex fixes |
| `CODEXA_LLM_MAX_TOKENS` | `2048` | Max output tokens per fix |
| `CODEXA_LLM_TEMPERATURE` | `0.1` | Low temperature for deterministic output |
| `CODEXA_GITHUB_ORG` | `fabhotelstech` | GitHub org for source repos |
| `CODEXA_GITHUB_BRANCH` | `azure_migration` | Branch CodeXA reads and opens PRs against |
| `CODEXA_WORKSPACE` | `/var/otel/codexa-workspace` | Local clone directory (on PVC) |
| `CODEXA_MAX_FILES` | `2` | Max source files per analysis |
| `CODEXA_POLL_INTERVAL` | `60` | Seconds between CodeXA scan cycles |
| `CODEXA_VERIFY_TIMEOUT` | `0` | Build verification timeout (0 = disabled) |
| `CODEXA_AGENT_URL` | `http://127.0.0.1:5000` | Internal agent API base |
| `CODEXA_AGENT_PREFIX` | `/ai-agent` | API path prefix |

### Jenkins

| Variable | Value | Description |
|---|---|---|
| `JENKINS_URL` | `https://ci.fabmailers.in` | Jenkins instance URL |
| `JENKINS_DISPATCHER_JOB` | `AI-AGENT-BOT` | Job name for AI-triggered builds |
| `JENKINS_BOT_MAX_CONCURRENT` | `1` | Max concurrent bot-triggered jobs |
| `JENKINS_IMAGE_AUTO_REBUILD_ENABLED` | `false` | Auto-trigger image rebuilds on fix |

### Redis

| Variable | Value | Description |
|---|---|---|
| `REDIS_URL` | `redis://ai-monitoring-agent-redis.jupiter.svc.cluster.local:6380/0` | Redis connection URL |
| `LEARNING_STORE_PATH` | `/var/otel/rca-learning` | File-based fallback for learning store |

---

## API Endpoints

All endpoints are prefixed with `/ai-agent`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Web dashboard |
| `GET` | `/health` | Health check |
| `GET` | `/api/status` | Agent status |
| `GET` | `/api/metrics` | Metrics data for charts |
| `GET` | `/api/incidents` | Recent incidents |
| `GET` | `/api/alerts/distribution` | Alert distribution |
| `GET` | `/api/configuration` | Current configuration |
| `POST` | `/api/configuration` | Update configuration |
| `POST` | `/api/feedback` | Submit feedback |
| `GET` | `/api/learning/stats` | ML learning statistics |
| `POST` | `/api/incident/<id>/resolve` | Resolve an incident |
| `GET` | `/api/codexa/issues` | All CodeXA-detected issues |
| `GET` | `/api/codexa/issue/<id>` | Issue detail + fix status |
| `POST` | `/api/codexa/analyze/<id>` | Trigger manual analysis |
| `GET` | `/api/k8s-agent/status` | K8s-agent scan results |
| `GET` | `/api/infra-audit/results` | Infrastructure audit results |
| `GET` | `/api/rollout/status` | Rollout monitor status |

---

## Namespaces & Services Monitored

| Namespace | Role |
|---|---|
| `jupiter` / `venus` | Both run the identical agent stack — ai-monitoring-agent pod, Ollama, Redis, all secrets, and all monitoring components |

**External systems accessed:**

| System | Location |
|---|---|
| Elasticsearch | external cluster |
| Prometheus | `monitoring` namespace |
| Redis | `jupiter` / `venus` namespace |
| Ollama | `jupiter` / `venus` namespace |
| SigNoz (tracing) | `signoz-prod11` namespace |
| Jenkins | `https://ci.fabmailers.in` |
| GitHub | `github.com/fabhotelstech` |
| ArgoCD | Configured via `ARGOCD_URL` secret |
| Slack | Configured via `SLACK_WEBHOOK_URL` secret |

---

## LLM Strategy

The agent uses a **local-first, tiered LLM strategy** with no mandatory cloud dependency:

```
Task                     Model                            Notes
───────────────────────  ───────────────────────────────  ──────────────────────────────────
Quick RCA / K8s diag     qwen2.5:1.5b                     Fast, low memory, CPU-friendly
Code fix generation      qwen2.5:7b-instruct-q4_K_M       Better code reasoning, quantized
Cloud fallback (opt.)    Gemini 2.0 Flash                 Only if GEMINI_API_KEY set
Alt fallback (opt.)      CodeXA GPT (custom endpoint)     If CODEX_GPT_* secrets set
```

**Key constraints:**
- All Ollama calls go through `http://ollama:11434` (in-cluster service)
- K8s-agent serializes LLM calls via `threading.Semaphore(1)` to prevent connection drops under concurrent pod analysis
- CodeXA uses a 15-minute timeout to allow complex multi-file analysis
- No external LLM calls are made unless the optional secret keys are configured

---

## Noise Filtering

The agent filters known infrastructure noise to prevent false alerts in the dashboard:

- `fabhotels-signoz-prod-otel-collector.signoz-prod11.svc.cluster.local` — SigNoz DNS lookup errors (not application errors)

Noise is filtered at four independent points:
1. `_record_service_status_issues()` — ES-sourced service status ingestion
2. `_collect_es_actionable_service_issues()` — ES log line scan
3. `_extract_error_from_logs()` — pod log line scan
4. `_fetch_pod_actual_error()` — cached pod log result

---

## License

MIT License
