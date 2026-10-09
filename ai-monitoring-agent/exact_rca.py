#!/usr/bin/env python3
"""Production exact RCA with L0 learning, L1 patterns, L2 LLM."""

import asyncio
import hashlib
import json
import os
import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from celery import Celery
except Exception:  # pragma: no cover
    Celery = None

try:
    import redis as redis_lib
except Exception:  # pragma: no cover
    redis_lib = None

from learning_store import (
    build_error_signature,
    check_learned_patterns,
    find_similar_learning,
    record_l0_hit,
    record_llm_call,
    save_rca_result,
)

_CELERY_APP = None
_REDIS_CLIENT = None
_METRICS_LOCK = threading.Lock()
_LLM_LATENCY_BUCKETS = [0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0]
_RCA_METRICS = {
    'rca_l0_hits_total': 0,
    'rca_l1_hits_total': 0,
    'rca_llm_calls_total': 0,
    'rca_fallback_total': 0,
    'rca_llm_latency_seconds_count': 0,
    'rca_llm_latency_seconds_sum': 0.0,
    'rca_llm_latency_seconds_bucket': {str(b): 0 for b in _LLM_LATENCY_BUCKETS},
    'rca_llm_latency_seconds_bucket_inf': 0,
}
_RCA_MODEL_METRICS: Dict[str, Dict[str, Any]] = {}


def _active_ai_model() -> str:
    return str(os.getenv("AI_MODEL_NAME", "qwen3") or "qwen3").strip().lower() or "qwen3"


def _empty_metric_bucket() -> Dict[str, Any]:
    return {
        'rca_l0_hits_total': 0,
        'rca_l1_hits_total': 0,
        'rca_llm_calls_total': 0,
        'rca_fallback_total': 0,
        'rca_llm_latency_seconds_count': 0,
        'rca_llm_latency_seconds_sum': 0.0,
        'rca_llm_latency_seconds_bucket': {str(b): 0 for b in _LLM_LATENCY_BUCKETS},
        'rca_llm_latency_seconds_bucket_inf': 0,
    }


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _inc_metric(name: str, delta: int = 1, llm_model: Optional[str] = None) -> None:
    model = str(llm_model or _active_ai_model()).strip().lower() or "qwen3"
    with _METRICS_LOCK:
        _RCA_METRICS[name] = int(_RCA_METRICS.get(name, 0) or 0) + int(delta or 0)
        bucket = _RCA_MODEL_METRICS.get(model)
        if not isinstance(bucket, dict):
            bucket = _empty_metric_bucket()
            _RCA_MODEL_METRICS[model] = bucket
        bucket[name] = int(bucket.get(name, 0) or 0) + int(delta or 0)


def _observe_llm_latency(seconds: float, llm_model: Optional[str] = None) -> None:
    value = max(0.0, float(seconds or 0.0))
    model = str(llm_model or _active_ai_model()).strip().lower() or "qwen3"
    with _METRICS_LOCK:
        _RCA_METRICS['rca_llm_latency_seconds_count'] = int(_RCA_METRICS.get('rca_llm_latency_seconds_count', 0) or 0) + 1
        _RCA_METRICS['rca_llm_latency_seconds_sum'] = float(_RCA_METRICS.get('rca_llm_latency_seconds_sum', 0.0) or 0.0) + value
        for bucket in _LLM_LATENCY_BUCKETS:
            if value <= bucket:
                key = str(bucket)
                buckets = _RCA_METRICS.get('rca_llm_latency_seconds_bucket', {})
                buckets[key] = int(buckets.get(key, 0) or 0) + 1
                _RCA_METRICS['rca_llm_latency_seconds_bucket'] = buckets
        _RCA_METRICS['rca_llm_latency_seconds_bucket_inf'] = int(_RCA_METRICS.get('rca_llm_latency_seconds_bucket_inf', 0) or 0) + 1

        bucket = _RCA_MODEL_METRICS.get(model)
        if not isinstance(bucket, dict):
            bucket = _empty_metric_bucket()
            _RCA_MODEL_METRICS[model] = bucket
        bucket['rca_llm_latency_seconds_count'] = int(bucket.get('rca_llm_latency_seconds_count', 0) or 0) + 1
        bucket['rca_llm_latency_seconds_sum'] = float(bucket.get('rca_llm_latency_seconds_sum', 0.0) or 0.0) + value
        for lat_bucket in _LLM_LATENCY_BUCKETS:
            if value <= lat_bucket:
                key = str(lat_bucket)
                model_buckets = bucket.get('rca_llm_latency_seconds_bucket', {})
                model_buckets[key] = int(model_buckets.get(key, 0) or 0) + 1
                bucket['rca_llm_latency_seconds_bucket'] = model_buckets
        bucket['rca_llm_latency_seconds_bucket_inf'] = int(bucket.get('rca_llm_latency_seconds_bucket_inf', 0) or 0) + 1


def get_rca_runtime_metrics(llm_model: Optional[str] = None) -> Dict[str, Any]:
    model = str(llm_model or "").strip().lower()
    with _METRICS_LOCK:
        source = _RCA_METRICS
        if model:
            source = _RCA_MODEL_METRICS.get(model, _empty_metric_bucket())
        count = int(source.get('rca_llm_latency_seconds_count', 0) or 0)
        total = float(source.get('rca_llm_latency_seconds_sum', 0.0) or 0.0)
        return {
            'rca_l0_hits_total': int(source.get('rca_l0_hits_total', 0) or 0),
            'rca_l1_hits_total': int(source.get('rca_l1_hits_total', 0) or 0),
            'rca_llm_calls_total': int(source.get('rca_llm_calls_total', 0) or 0),
            'rca_fallback_total': int(source.get('rca_fallback_total', 0) or 0),
            'rca_llm_latency_seconds_count': count,
            'rca_llm_latency_seconds_sum': total,
            'rca_llm_latency_seconds_avg': (total / count) if count > 0 else 0.0,
            'rca_llm_latency_seconds_bucket': dict(source.get('rca_llm_latency_seconds_bucket', {}) or {}),
            'rca_llm_latency_seconds_bucket_inf': int(source.get('rca_llm_latency_seconds_bucket_inf', 0) or 0),
        }


def get_rca_prometheus_metrics() -> str:
    m = get_rca_runtime_metrics()
    lines = [
        '# HELP rca_l0_hits_total Learned pattern direct hits',
        '# TYPE rca_l0_hits_total counter',
        f"rca_l0_hits_total {m['rca_l0_hits_total']}",
        '# HELP rca_l1_hits_total Pattern match direct hits',
        '# TYPE rca_l1_hits_total counter',
        f"rca_l1_hits_total {m['rca_l1_hits_total']}",
        '# HELP rca_llm_calls_total Total LLM calls for RCA',
        '# TYPE rca_llm_calls_total counter',
        f"rca_llm_calls_total {m['rca_llm_calls_total']}",
        '# HELP rca_fallback_total Total deterministic fallback responses',
        '# TYPE rca_fallback_total counter',
        f"rca_fallback_total {m['rca_fallback_total']}",
        '# HELP rca_llm_latency_seconds LLM response latency seconds',
        '# TYPE rca_llm_latency_seconds histogram',
    ]
    for bucket in _LLM_LATENCY_BUCKETS:
        lines.append(f'rca_llm_latency_seconds_bucket{{le="{bucket}"}} {int(m["rca_llm_latency_seconds_bucket"].get(str(bucket), 0) or 0)}')
    lines.append(f'rca_llm_latency_seconds_bucket{{le="+Inf"}} {int(m.get("rca_llm_latency_seconds_bucket_inf", 0) or 0)}')
    lines.append(f'rca_llm_latency_seconds_sum {float(m.get("rca_llm_latency_seconds_sum", 0.0) or 0.0)}')
    lines.append(f'rca_llm_latency_seconds_count {int(m.get("rca_llm_latency_seconds_count", 0) or 0)}')
    return "\n".join(lines) + "\n"


def _get_redis_client():
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    if redis_lib is None:
        return None
    redis_url = os.getenv('REDIS_URL', '').strip()
    if not redis_url:
        return None
    try:
        _REDIS_CLIENT = redis_lib.Redis.from_url(
            redis_url,
            socket_connect_timeout=0.8,
            socket_timeout=1.2,
            decode_responses=True,
        )
        _REDIS_CLIENT.ping()
        return _REDIS_CLIENT
    except Exception:
        _REDIS_CLIENT = None
        return None


def _exact_cache_key(service_name: str, namespace: str, logs: Any, events: Any, describe: Any, metrics: Any) -> str:
    digest = hashlib.sha1(
        json.dumps(
            {
                'service_name': service_name,
                'namespace': namespace,
                'logs': logs,
                'events': events,
                'describe': describe,
                'metrics': metrics,
            },
            default=str,
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
    ).hexdigest()
    return f"ai-agent:exact-rca:cache:{digest}"


def _exact_cache_get(cache_key: str) -> Optional[Dict[str, Any]]:
    client = _get_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(cache_key)
        if not raw:
            return None
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _exact_cache_set(cache_key: str, payload: Dict[str, Any], ttl_seconds: int = 300) -> None:
    client = _get_redis_client()
    if client is None:
        return
    try:
        client.setex(cache_key, max(60, int(ttl_seconds or 300)), json.dumps(payload, default=str, separators=(',', ':')))
    except Exception:
        return


def _normalize_lines(raw: Any) -> List[str]:
    lines: List[str] = []
    if raw is None:
        return lines
    if isinstance(raw, str):
        return [line.rstrip() for line in raw.splitlines() if str(line).strip()]
    if isinstance(raw, dict):
        for key in ("logs", "lines", "entries", "content", "message", "body"):
            if key in raw:
                lines.extend(_normalize_lines(raw.get(key)))
        return [line for line in lines if line]
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                text = _normalize_text(
                    item.get("message") or item.get("body") or item.get("log") or item.get("line") or item.get("text") or ""
                )
            else:
                text = _normalize_text(item)
            if text:
                lines.append(text)
        return lines
    text = _normalize_text(raw)
    return [text] if text else []


def _extract_signals(logs_lines: List[str], events_lines: List[str]) -> List[str]:
    signals: List[str] = []
    regex = re.compile(
        r"error|exception|failed|crashloop|oom|imagepull|refused|timeout|deadline exceeded|no route",
        re.IGNORECASE,
    )
    for line in logs_lines + events_lines:
        if regex.search(str(line or "")):
            compact = _normalize_text(line)
            if compact and compact not in signals:
                signals.append(compact)
    return signals[:5000]


def _chunk_lines(lines: List[str], chunk_size: int = 120, overlap: int = 20) -> List[List[str]]:
    if len(lines) <= chunk_size:
        return [lines]
    chunks: List[List[str]] = []
    step = max(1, chunk_size - overlap)
    index = 0
    while index < len(lines):
        chunk = lines[index:index + chunk_size]
        if chunk:
            chunks.append(chunk)
        if index + chunk_size >= len(lines):
            break
        index += step
    return chunks


def _confidence_rank(conf: str) -> int:
    value = str(conf or "").strip().lower()
    return {"high": 3, "medium": 2, "low": 1}.get(value, 0)


def _extract_component(line: str, default_component: str) -> str:
    text = str(line or "")
    patterns = [
        r"pod[\s:/]+([a-z0-9.-]+)",
        r"container[\s:/]+([a-z0-9.-]+)",
        r"\b([a-z0-9.-]+)-[a-f0-9]{8,}\b",
    ]
    lower = text.lower()
    for pattern in patterns:
        match = re.search(pattern, lower)
        if match:
            return str(match.group(1) or "").strip() or default_component
    return default_component


def _build_result(
    exact_issue: str,
    root_cause: str,
    affected_component: str,
    fix_command: str,
    fix_explanation: str,
    confidence: str,
    evidence_lines: List[str],
    eta_minutes: int,
    error_signature: str,
    service_name: str,
    namespace: str,
) -> Dict[str, Any]:
    digest_source = json.dumps(
        {
            "service_name": service_name,
            "namespace": namespace,
            "exact_issue": exact_issue,
            "root_cause": root_cause,
            "error_signature": error_signature,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    rca_id = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()
    return {
        "rca_id": rca_id,
        "error_signature": str(error_signature or ""),
        "exact_issue": _normalize_text(exact_issue),
        "root_cause": _normalize_text(root_cause),
        "affected_component": _normalize_text(affected_component),
        "fix_command": _normalize_text(fix_command),
        "fix_explanation": _normalize_text(fix_explanation),
        "confidence": str(confidence or "low").strip().lower(),
        "evidence_lines": [_normalize_text(line) for line in (evidence_lines or []) if _normalize_text(line)][:6],
        "eta_minutes": max(1, int(eta_minutes or 1)),
    }


def _pattern_match_rca(
    service_name: str,
    namespace: str,
    logs_lines: List[str],
    events_lines: List[str],
    describe_lines: List[str],
    error_signature: str,
) -> Optional[Dict[str, Any]]:
    evidence = logs_lines + events_lines + describe_lines
    if not evidence:
        return None

    def _find(pattern: str) -> Optional[str]:
        for line in evidence:
            if re.search(pattern, str(line or ""), re.IGNORECASE):
                return _normalize_text(line)
        return None

    checks: List[Tuple[str, str, str, str, str, int]] = [
        (
            r"oomkilled|out of memory|killed process.*oom",
            "Container memory exhausted (OOMKill)",
            "Pod/container memory limit is lower than runtime working set",
            f"kubectl -n {namespace} set resources deploy/{service_name} --limits=memory=2Gi --requests=memory=1Gi",
            "Raises memory request/limit to prevent OOM termination and restart loops",
            12,
        ),
        (
            r"crashloopbackoff|back-off restarting failed container",
            "Pod repeatedly crashing (CrashLoopBackOff)",
            "Application process exits on startup or health check cycle",
            f"kubectl -n {namespace} rollout restart deploy/{service_name}",
            "Restarts pods after startup issue/config fix is applied",
            8,
        ),
        (
            r"imagepullbackoff|errimagepull|failed to pull image|pull access denied",
            "Container image pull failed",
            "Image/tag or registry credential is invalid/unavailable",
            f"kubectl -n {namespace} describe pod $(kubectl -n {namespace} get pod -l app={service_name} -o name | head -n 1)",
            "Shows exact image pull failure and registry auth/tag error",
            15,
        ),
        (
            r"connection refused|dial tcp .*: connect: connection refused|no route to host",
            "Dependency connection refused",
            "Service cannot connect to downstream endpoint from cluster network",
            f"kubectl -n {namespace} exec deploy/{service_name} -- sh -c 'nc -vz <dependency-host> <port>'",
            "Validates network reachability to failing dependency",
            20,
        ),
        (
            r"context deadline exceeded|timed out|timeout",
            "Dependency/request timeout",
            "Slow downstream call or blocked IO caused deadline breach",
            f"kubectl -n {namespace} logs deploy/{service_name} --since=20m | grep -Ei 'timeout|deadline exceeded'",
            "Confirms timeout signatures and helps identify slow path",
            18,
        ),
    ]

    for pattern, fallback_issue, root_cause, fix_cmd, fix_expl, eta in checks:
        hit = _find(pattern)
        if hit:
            return _build_result(
                exact_issue=hit or fallback_issue,
                root_cause=root_cause,
                affected_component=_extract_component(hit or fallback_issue, service_name),
                fix_command=fix_cmd,
                fix_explanation=fix_expl,
                confidence="high",
                evidence_lines=[hit] if hit else [fallback_issue],
                eta_minutes=eta,
                error_signature=error_signature,
                service_name=service_name,
                namespace=namespace,
            )
    return None


def _ollama_generate(prompt: str, temperature: float = 0.05) -> str:
    start = time.monotonic()
    _inc_metric('rca_llm_calls_total', 1)
    host = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434").strip().rstrip("/")
    model = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b").strip() or "qwen2.5:1.5b"
    timeout = int(os.getenv("AI_API_TIMEOUT", "45") or "45")
    try:
        response = requests.post(
            f"{host}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": float(temperature), "num_thread": 4, "num_ctx": 4096, "top_p": 0.9},
            },
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return str(payload.get("response", "") or "").strip() if isinstance(payload, dict) else ""
    finally:
        _observe_llm_latency(time.monotonic() - start)


def _first_json_object(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass
    first = raw.find("{")
    last = raw.rfind("}")
    if first == -1 or last == -1 or last <= first:
        return {}
    try:
        parsed = json.loads(raw[first:last + 1])
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _llm_system_prompt() -> str:
    return (
        "You are a production SRE RCA engine.\n"
        "Find exact issue from logs line-by-line.\n"
        "Rules:\n"
        "1) exact_issue must be exact error line copied verbatim from provided logs/events/describe.\n"
        "2) Never give generic answers like 'service degraded'.\n"
        "3) Include only strict JSON.\n"
        "4) evidence_lines must quote exact lines from input.\n"
        "5) If uncertain, lower confidence.\n"
        "Output schema:"
        "{\"exact_issue\":\"...\",\"root_cause\":\"...\",\"affected_component\":\"...\","
        "\"fix_command\":\"...\",\"fix_explanation\":\"...\",\"confidence\":\"high|medium|low\","
        "\"evidence_lines\":[\"line1\",\"line2\"],\"eta_minutes\":10}"
    )


def _learning_hint_text(similar_learnings: List[Dict[str, Any]]) -> str:
    if not similar_learnings:
        return "SIMILAR PAST ISSUES FROM YOUR INFRA: []"
    rows = []
    for item in similar_learnings[:3]:
        rows.append(
            {
                "exact_issue": item.get("exact_issue", ""),
                "root_cause": item.get("root_cause", ""),
                "fix_command": item.get("fix_command", ""),
                "confidence_score": item.get("confidence_score", 0.0),
                "occurrence_count": item.get("occurrence_count", 0),
            }
        )
    return (
        "SIMILAR PAST ISSUES FROM YOUR INFRA:\n"
        f"{json.dumps(rows, ensure_ascii=True)}\n"
        "Use these as hints. If current issue matches a past pattern, prioritize that root cause."
    )


async def _analyze_chunk_with_llm(
    service_name: str,
    namespace: str,
    chunk_lines: List[str],
    events_lines: List[str],
    describe_lines: List[str],
    metrics: Any,
    similar_learnings: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not chunk_lines:
        return None
    numbered = [f"{idx + 1}: {line}" for idx, line in enumerate(chunk_lines)]
    prompt = (
        f"{_llm_system_prompt()}\n\n"
        f"Service: {namespace}/{service_name}\n"
        f"Metrics: {json.dumps(metrics, default=str)}\n"
        f"{_learning_hint_text(similar_learnings)}\n\n"
        f"Events:\n{chr(10).join(events_lines[:120])}\n\n"
        f"Describe:\n{chr(10).join(describe_lines[:160])}\n\n"
        "Analyze log lines line-by-line and pick exact evidence:\n"
        + "\n".join(numbered)
    )
    try:
        raw = await asyncio.to_thread(_ollama_generate, prompt, 0.05)
    except Exception:
        return None
    parsed = _first_json_object(raw)
    if not parsed:
        return None
    if not str(parsed.get("exact_issue", "") or "").strip():
        return None
    return parsed


async def _final_merge_with_llm(
    service_name: str,
    namespace: str,
    findings: List[Dict[str, Any]],
    events_lines: List[str],
    describe_lines: List[str],
    metrics: Any,
    similar_learnings: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    prompt = (
        f"{_llm_system_prompt()}\n\n"
        f"Service: {namespace}/{service_name}\n"
        f"Metrics: {json.dumps(metrics, default=str)}\n"
        f"{_learning_hint_text(similar_learnings)}\n\n"
        f"Candidate findings from chunks:\n{json.dumps(findings[:8], ensure_ascii=True)}\n\n"
        f"Events summary:\n{chr(10).join(events_lines[:80])}\n\n"
        f"Describe summary:\n{chr(10).join(describe_lines[:120])}\n\n"
        "Select the single best RCA and return strict JSON only."
    )
    try:
        raw = await asyncio.to_thread(_ollama_generate, prompt, 0.05)
    except Exception:
        return None
    parsed = _first_json_object(raw)
    return parsed if parsed else None


async def get_exact_rca(
    service_name: str,
    namespace: str,
    logs: Any,
    events: Any,
    describe: Any,
    metrics: Any,
    force_llm: bool = False,
) -> Dict[str, Any]:
    """Complete RCA flow:
    1) build_error_signature
    2) L0 learned pattern check
    3) extract_signals
    4) L1 pattern match
    5) L2 LLM with learning hints
    6) save_rca_result
    7) return
    """
    service_name = str(service_name or "").strip() or "unknown-service"
    namespace = str(namespace or "").strip() or "default"
    active_model = _active_ai_model()

    cache_key = _exact_cache_key(service_name, namespace, logs, events, describe, metrics)
    cached = _exact_cache_get(cache_key)
    if isinstance(cached, dict) and cached.get('exact_issue'):
        return cached

    logs_lines = _normalize_lines(logs)
    events_lines = _normalize_lines(events)
    describe_lines = _normalize_lines(describe)

    error_signature = build_error_signature(logs_lines, events_lines, metrics)

    learned = await asyncio.to_thread(check_learned_patterns, error_signature, service_name, namespace, active_model)
    similar_learnings: List[Dict[str, Any]] = []
    if isinstance(learned, dict):
        if isinstance(learned.get("similar"), list):
            similar_learnings = learned.get("similar", [])
        if learned.get("from_learning") and isinstance(learned.get("result"), dict):
            l0_result = dict(learned.get("result") or {})
            l0_result.setdefault("rca_id", l0_result.get("id", ""))
            l0_result.setdefault("error_signature", error_signature)
            l0_result["from_learning"] = True
            await asyncio.to_thread(record_l0_hit, namespace, active_model)
            _inc_metric('rca_l0_hits_total', 1, active_model)
            await asyncio.to_thread(
                save_rca_result,
                l0_result,
                {"service_name": service_name, "namespace": namespace, "error_signature": error_signature, "llm_model": active_model},
            )
            if not force_llm:
                _exact_cache_set(cache_key, l0_result, ttl_seconds=300)
                return l0_result
            similar_learnings = [l0_result] + similar_learnings

    signals = _extract_signals(logs_lines, events_lines)

    seeded_pattern_result = _pattern_match_rca(service_name, namespace, signals or logs_lines, events_lines, describe_lines, error_signature)
    if seeded_pattern_result is not None and not force_llm:
        seeded_pattern_result["from_learning"] = False
        _inc_metric('rca_l1_hits_total', 1, active_model)
        await asyncio.to_thread(
            save_rca_result,
            seeded_pattern_result,
            {"service_name": service_name, "namespace": namespace, "error_signature": error_signature, "llm_model": active_model},
        )
        _exact_cache_set(cache_key, seeded_pattern_result, ttl_seconds=300)
        return seeded_pattern_result

    await asyncio.to_thread(record_llm_call, namespace, active_model)

    lines_for_chunk = logs_lines if logs_lines else signals
    chunks = _chunk_lines(lines_for_chunk, chunk_size=120, overlap=20) if lines_for_chunk else [[]]
    findings: List[Dict[str, Any]] = []
    if chunks and any(chunks):
        tasks = [
            _analyze_chunk_with_llm(
                service_name,
                namespace,
                chunk,
                events_lines,
                describe_lines,
                metrics,
                similar_learnings,
            )
            for chunk in chunks
        ]
        outputs = await asyncio.gather(*tasks, return_exceptions=True)
        for item in outputs:
            if isinstance(item, dict) and str(item.get("exact_issue", "") or "").strip():
                findings.append(item)

    merged: Optional[Dict[str, Any]] = None
    if findings:
        findings = sorted(
            findings,
            key=lambda x: (
                _confidence_rank(str(x.get("confidence", "low") or "low")),
                len(x.get("evidence_lines", []) if isinstance(x.get("evidence_lines", []), list) else []),
            ),
            reverse=True,
        )
        merged = await _final_merge_with_llm(
            service_name,
            namespace,
            findings,
            events_lines,
            describe_lines,
            metrics,
            similar_learnings,
        )
        if merged is None:
            merged = findings[0]

    if merged is None and seeded_pattern_result is not None:
        merged = dict(seeded_pattern_result)

    if merged is None:
        similar = await asyncio.to_thread(find_similar_learning, error_signature, service_name, namespace, active_model)
        if similar and str(similar.get("exact_issue", "") or "").strip():
            fallback = dict(similar)
            fallback.setdefault("error_signature", error_signature)
            fallback.setdefault("rca_id", fallback.get("id", ""))
            fallback["from_learning"] = True
            await asyncio.to_thread(record_l0_hit, namespace, active_model)
            _inc_metric('rca_l0_hits_total', 1, active_model)
            await asyncio.to_thread(
                save_rca_result,
                fallback,
                {"service_name": service_name, "namespace": namespace, "error_signature": error_signature, "llm_model": active_model},
            )
            _exact_cache_set(cache_key, fallback, ttl_seconds=300)
            return fallback

        pod_counts = metrics.get("pod_counts", {}) if isinstance(metrics, dict) and isinstance(metrics.get("pod_counts", {}), dict) else {}
        total_pods = int(pod_counts.get("total", 0) or 0)
        running_pods = int(pod_counts.get("running", 0) or 0)
        ready_pods = int(pod_counts.get("ready", 0) or 0)

        if total_pods == 0:
            merged = {
                "exact_issue": f"No pods found for {namespace}/{service_name}; deployment replicas may be set to 0",
                "root_cause": "Deployment has zero desired replicas or workload selector mismatch, so no pod is scheduled",
                "affected_component": service_name,
                "fix_command": f"kubectl -n {namespace} get deploy {service_name} -o yaml | egrep 'replicas|selector' && kubectl -n {namespace} scale deploy/{service_name} --replicas=1",
                "fix_explanation": "Validates deployment desired replicas/selector and scales replicas up so pods are created",
                "confidence": "high",
                "evidence_lines": [f"No pods found for {namespace}/{service_name}"],
                "eta_minutes": 5,
            }
            _inc_metric('rca_l1_hits_total', 1, active_model)
        elif total_pods > 0 and ready_pods == 0 and running_pods == 0:
            merged = {
                "exact_issue": f"Pods exist but none are running/ready for {namespace}/{service_name}",
                "root_cause": "Pods are failing before readiness due to startup/runtime issue",
                "affected_component": service_name,
                "fix_command": f"kubectl -n {namespace} get pods -l app={service_name} && kubectl -n {namespace} describe pods -l app={service_name}",
                "fix_explanation": "Inspects pod events and startup errors to isolate why pods never become ready",
                "confidence": "medium",
                "evidence_lines": [f"pod_counts total={total_pods} running={running_pods} ready={ready_pods}"],
                "eta_minutes": 10,
            }
            _inc_metric('rca_l1_hits_total', 1, active_model)

        if merged is None:
            exit_line = ''
            exit_code = ''
            reason_error = False
            for line in describe_lines + events_lines + logs_lines:
                text = str(line or '')
                if not text:
                    continue
                if re.search(r'(?i)reason\s*[:=]\s*error', text):
                    reason_error = True
                match = re.search(r'(?i)exit\s*code\s*[:=]\s*([1-9][0-9]*)', text)
                if match:
                    exit_code = str(match.group(1) or '').strip()
                    exit_line = _normalize_text(text)
                    break
            if exit_code:
                merged = {
                    "exact_issue": exit_line or f"Container terminated with non-zero exit code {exit_code}",
                    "root_cause": (
                        f"Container process exits with code {exit_code}; startup/runtime command fails before stable readiness"
                    ),
                    "affected_component": _extract_component(exit_line or service_name, service_name),
                    "fix_command": (
                        f"kubectl -n {namespace} logs deploy/{service_name} --previous --tail=300 && "
                        f"kubectl -n {namespace} describe pods -l app={service_name}"
                    ),
                    "fix_explanation": "Collects previous-crash logs and pod events to isolate exact failing command/config/dependency",
                    "confidence": "medium" if reason_error else "low",
                    "evidence_lines": [exit_line] if exit_line else [f"exit_code={exit_code}"],
                    "eta_minutes": 8,
                }
                _inc_metric('rca_l1_hits_total', 1, active_model)

        fallback_line = ""
        for line in logs_lines + events_lines + describe_lines:
            if re.search(r"error|exception|failed|timeout|refused", str(line or ""), re.IGNORECASE):
                fallback_line = _normalize_text(line)
                break
        if merged is None:
            if not fallback_line:
                fallback_line = f"No explicit error line found for {namespace}/{service_name}"
            merged = {
                "exact_issue": fallback_line,
                "root_cause": "Insufficient deterministic evidence to isolate exact failure",
                "affected_component": _extract_component(fallback_line, service_name),
                "fix_command": f"kubectl -n {namespace} logs deploy/{service_name} --since=30m",
                "fix_explanation": "Collect full logs and correlate with events/describe for manual RCA",
                "confidence": "low",
                "evidence_lines": [fallback_line],
                "eta_minutes": 20,
            }
            _inc_metric('rca_fallback_total', 1, active_model)

    result = _build_result(
        exact_issue=str(merged.get("exact_issue", "") or ""),
        root_cause=str(merged.get("root_cause", "") or ""),
        affected_component=str(merged.get("affected_component", service_name) or service_name),
        fix_command=str(merged.get("fix_command", f"kubectl -n {namespace} describe deploy/{service_name}") or ""),
        fix_explanation=str(merged.get("fix_explanation", "Apply targeted fix using exact issue evidence") or ""),
        confidence=str(merged.get("confidence", "medium") or "medium").lower(),
        evidence_lines=merged.get("evidence_lines", []) if isinstance(merged.get("evidence_lines", []), list) else [],
        eta_minutes=int(merged.get("eta_minutes", 15) or 15),
        error_signature=error_signature,
        service_name=service_name,
        namespace=namespace,
    )
    result["from_learning"] = False

    await asyncio.to_thread(
        save_rca_result,
        result,
        {"service_name": service_name, "namespace": namespace, "error_signature": error_signature, "llm_model": active_model},
    )
    _exact_cache_set(cache_key, result, ttl_seconds=300)
    return result


def _get_celery_app():
    global _CELERY_APP
    if _CELERY_APP is not None:
        return _CELERY_APP
    if Celery is None:
        return None
    broker = os.getenv("CELERY_BROKER_URL", "").strip() or os.getenv("REDIS_URL", "").strip()
    if not broker:
        return None
    try:
        app = Celery("exact_rca", broker=broker, backend=broker)
        app.conf.update(
            task_serializer="json",
            result_serializer="json",
            accept_content=["json"],
            task_default_queue="ai-monitoring-agent",
            task_ignore_result=False,
        )
        _CELERY_APP = app
        return _CELERY_APP
    except Exception:
        _CELERY_APP = None
        return None


def _run_exact_rca_sync(service_name: str, namespace: str, logs: Any, events: Any, describe: Any, metrics: Any) -> Dict[str, Any]:
    return asyncio.run(get_exact_rca(service_name, namespace, logs, events, describe, metrics))


if _get_celery_app() is not None:

    @_get_celery_app().task(name="ai_monitoring_agent.get_exact_rca_task")
    def get_exact_rca_task(service_name: str, namespace: str, logs: Any, events: Any, describe: Any, metrics: Any):
        return _run_exact_rca_sync(service_name, namespace, logs, events, describe, metrics)

else:

    def get_exact_rca_task(service_name: str, namespace: str, logs: Any, events: Any, describe: Any, metrics: Any):
        return _run_exact_rca_sync(service_name, namespace, logs, events, describe, metrics)
