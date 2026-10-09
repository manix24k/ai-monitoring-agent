#!/usr/bin/env python3
"""Self-learning storage and retrieval for RCA patterns."""

import hashlib
import json
import os
import re
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

try:
    import redis as redis_lib
except Exception:  # pragma: no cover
    redis_lib = None

try:
    from celery import Celery
except Exception:  # pragma: no cover
    Celery = None


INDEX_NAME = "rca-learnings"
_REDIS_CLIENT = None
_CELERY_APP = None
_FILE_LOCK = threading.Lock()


def _normalize_model_name(value: Any) -> str:
    model = str(value or "").strip().lower()
    return model or "qwen3"


def _entry_model_name(row: Dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return "qwen3"
    return _normalize_model_name(row.get("llm_model", ""))


def _model_stats_suffix(llm_model: str) -> str:
    model = _normalize_model_name(llm_model)
    safe = re.sub(r"[^a-z0-9._-]", "_", model)
    return safe or "qwen3"


def _learning_store_dir() -> str:
    return os.getenv("LEARNING_STORE_PATH", "/var/otel/rca-learning").strip() or "/var/otel/rca-learning"


def _learning_store_file() -> str:
    return os.path.join(_learning_store_dir(), "learnings.json")


def _rca_results_file() -> str:
    return os.path.join(_learning_store_dir(), "rca_results.json")


def _ensure_learning_store_dir() -> bool:
    path = _learning_store_dir()
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except Exception:
        return False


def _read_json_file(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def _write_json_file_atomic(path: str, payload: Any) -> bool:
    if not _ensure_learning_store_dir():
        return False
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, separators=(",", ":"))
        os.replace(tmp, path)
        return True
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False


def _file_get_all_learnings() -> List[Dict[str, Any]]:
    with _FILE_LOCK:
        loaded = _read_json_file(_learning_store_file())
        if not isinstance(loaded, list):
            return []
        return [row for row in loaded if isinstance(row, dict)]


def _file_upsert_learning(entry: Dict[str, Any]) -> None:
    if not isinstance(entry, dict) or not str(entry.get("id", "") or "").strip():
        return
    with _FILE_LOCK:
        rows = _read_json_file(_learning_store_file())
        items = rows if isinstance(rows, list) else []
        out = []
        replaced = False
        for row in items:
            if not isinstance(row, dict):
                continue
            if str(row.get("id", "") or "") == str(entry.get("id", "") or ""):
                out.append(dict(entry))
                replaced = True
            else:
                out.append(row)
        if not replaced:
            out.append(dict(entry))
        _write_json_file_atomic(_learning_store_file(), out)


def _file_set_rca_result(entry: Dict[str, Any]) -> None:
    if not isinstance(entry, dict):
        return
    rid = str(entry.get("id", "") or entry.get("rca_id", "")).strip()
    if not rid:
        return
    with _FILE_LOCK:
        loaded = _read_json_file(_rca_results_file())
        mapping = loaded if isinstance(loaded, dict) else {}
        mapping[rid] = dict(entry)
        _write_json_file_atomic(_rca_results_file(), mapping)


def _file_get_rca_result(rca_id: str) -> Optional[Dict[str, Any]]:
    rid = str(rca_id or "").strip()
    if not rid:
        return None
    with _FILE_LOCK:
        loaded = _read_json_file(_rca_results_file())
        mapping = loaded if isinstance(loaded, dict) else {}
        item = mapping.get(rid)
        return item if isinstance(item, dict) else None


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _redis_client():
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    if redis_lib is None:
        return None
    redis_url = os.getenv("REDIS_URL", "").strip()
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


def _es_base_url() -> str:
    url = os.getenv("ELASTICSEARCH_URL", "").strip()
    if url:
        return url.rstrip("/")
    hosts = os.getenv("ELASTICSEARCH_HOSTS", "").strip()
    if hosts:
        first = hosts.split(",")[0].strip()
        if first:
            return first.rstrip("/")
    return ""


def _es_headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_key = os.getenv("ELASTICSEARCH_API_KEY", "").strip()
    username = os.getenv("ELASTICSEARCH_USERNAME", "").strip()
    password = os.getenv("ELASTICSEARCH_PASSWORD", "").strip()
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    elif username and password:
        import base64

        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
        headers["Authorization"] = f"Basic {token}"
    return headers


def ensure_learning_index() -> bool:
    base = _es_base_url()
    if not base:
        return False
    try:
        head = requests.head(f"{base}/{INDEX_NAME}", headers=_es_headers(), timeout=3)
        if head.status_code in (200, 201):
            return True
        mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "service_name": {"type": "keyword"},
                    "namespace": {"type": "keyword"},
                    "error_signature": {"type": "keyword"},
                    "anomaly_flags": {"type": "keyword"},
                    "exact_issue": {"type": "text"},
                    "root_cause": {"type": "text"},
                    "fix_command": {"type": "text"},
                    "was_correct": {"type": "boolean"},
                    "operator_correction": {"type": "text"},
                    "confidence_score": {"type": "float"},
                    "occurrence_count": {"type": "integer"},
                    "last_seen": {"type": "date"},
                    "service_type": {"type": "keyword"},
                    "resolved_in_minutes": {"type": "integer"},
                    "created_at": {"type": "date"},
                    "llm_model": {"type": "keyword"},
                }
            }
        }
        put = requests.put(f"{base}/{INDEX_NAME}", headers=_es_headers(), data=json.dumps(mapping), timeout=5)
        return put.status_code in (200, 201)
    except Exception:
        return False


def _es_get_learning_by_id(learning_id: str) -> Optional[Dict[str, Any]]:
    base = _es_base_url()
    if not base:
        for row in _file_get_all_learnings():
            if str(row.get("id", "") or "") == str(learning_id or ""):
                return row
        return None
    try:
        response = requests.get(f"{base}/{INDEX_NAME}/_doc/{learning_id}", headers=_es_headers(), timeout=4)
        if response.status_code != 200:
            return None
        payload = response.json()
        source = payload.get("_source", {}) if isinstance(payload, dict) else {}
        return source if isinstance(source, dict) else None
    except Exception:
        for row in _file_get_all_learnings():
            if str(row.get("id", "") or "") == str(learning_id or ""):
                return row
        return None


def _es_index_learning(entry: Dict[str, Any]) -> None:
    _file_upsert_learning(entry)
    base = _es_base_url()
    if not base or not isinstance(entry, dict) or not entry.get("id"):
        return
    if not ensure_learning_index():
        return
    try:
        requests.put(
            f"{base}/{INDEX_NAME}/_doc/{entry['id']}",
            headers=_es_headers(),
            data=json.dumps(entry, default=str),
            timeout=5,
        )
    except Exception:
        return


def _cache_learned_pattern(entry: Dict[str, Any], ttl_seconds: int = 86400) -> None:
    client = _redis_client()
    if client is None or not isinstance(entry, dict):
        return
    signature = str(entry.get("error_signature", "") or "").strip()
    if not signature:
        return
    key = f"learned:{signature}"
    model = _normalize_model_name(entry.get("llm_model", ""))
    model_key = f"learned:{model}:{signature}"
    try:
        payload = json.dumps(entry, default=str, separators=(",", ":"))
        ttl = max(300, int(ttl_seconds))
        client.setex(key, ttl, payload)
        client.setex(model_key, ttl, payload)
    except Exception:
        return


def build_error_signature(logs: Any, events: Any, metrics: Any) -> str:
    log_lines = []
    for item in logs if isinstance(logs, list) else [logs]:
        text = str(item.get("message") if isinstance(item, dict) else item or "")
        if text.strip():
            log_lines.append(text.strip())
    event_lines = []
    for item in events if isinstance(events, list) else [events]:
        text = str(item.get("message") if isinstance(item, dict) else item or "")
        if text.strip():
            event_lines.append(text.strip())

    exception_regex = re.compile(r"([A-Za-z0-9_.$]+(?:Exception|Error)|OOMKilled|CrashLoopBackOff|ImagePullBackOff|ErrImagePull)")
    top_exception = ""
    for line in log_lines:
        match = exception_regex.search(line)
        if match:
            top_exception = match.group(1)
            break
    top_event = ""
    for line in event_lines:
        lower = line.lower()
        if any(token in lower for token in ("oom", "crashloop", "imagepull", "failed", "timeout", "refused")):
            top_event = line
            break

    metric_signal = ""
    if isinstance(metrics, dict):
        metric_signal = json.dumps({
            "status": metrics.get("status", ""),
            "pod_counts": metrics.get("pod_counts", {}),
        }, sort_keys=True, default=str)

    raw = f"{top_exception}|{top_event}|{metric_signal}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def find_similar_learning(error_signature: str, service_name: str, namespace: str, llm_model: Optional[str] = None) -> Optional[Dict[str, Any]]:
    signature = str(error_signature or "").strip()
    if not signature:
        return None
    selected_model = _normalize_model_name(llm_model) if llm_model is not None else ""
    client = _redis_client()
    if client is not None:
        try:
            raw = None
            if selected_model:
                raw = client.get(f"learned:{selected_model}:{signature}")
            if not raw:
                raw = client.get(f"learned:{signature}")
            if raw:
                loaded = json.loads(raw)
                if isinstance(loaded, dict):
                    if selected_model and _entry_model_name(loaded) != selected_model:
                        return None
                    return loaded
        except Exception:
            pass

    # Durable local file fallback on PVC.
    best = None
    best_score = -1.0
    for row in _file_get_all_learnings():
        if not isinstance(row, dict):
            continue
        if str(row.get("error_signature", "") or "") != signature:
            continue
        if selected_model and _entry_model_name(row) != selected_model:
            continue
        score = float(row.get("confidence_score", 0.0) or 0.0)
        if str(row.get("service_name", "") or "") == str(service_name or ""):
            score += 0.2
        if str(row.get("namespace", "") or "") == str(namespace or ""):
            score += 0.2
        if score > best_score:
            best_score = score
            best = row
    if isinstance(best, dict):
        return best

    base = _es_base_url()
    if not base:
        return None
    if not ensure_learning_index():
        return None
    query = {
        "size": 1,
        "sort": [
            {"confidence_score": {"order": "desc"}},
            {"occurrence_count": {"order": "desc"}},
        ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"error_signature": signature}},
                ],
                "should": [
                    {"term": {"service_name": str(service_name or "")}},
                    {"term": {"namespace": str(namespace or "")}},
                ],
            }
        },
    }
    if selected_model:
        query["query"]["bool"]["must"].append({"term": {"llm_model": selected_model}})
    try:
        response = requests.post(
            f"{base}/{INDEX_NAME}/_search",
            headers=_es_headers(),
            data=json.dumps(query),
            timeout=5,
        )
        if response.status_code != 200:
            return None
        payload = response.json()
        hits = payload.get("hits", {}).get("hits", []) if isinstance(payload, dict) else []
        if not hits:
            return None
        source = hits[0].get("_source", {}) if isinstance(hits[0], dict) else {}
        return source if isinstance(source, dict) else None
    except Exception:
        return None


def check_learned_patterns(error_signature: str, service_name: str, namespace: str, llm_model: Optional[str] = None) -> Dict[str, Any]:
    found = find_similar_learning(error_signature, service_name, namespace, llm_model=llm_model)
    if not found:
        return {"result": None, "confidence": 0.0, "from_learning": False, "similar": []}

    score = float(found.get("confidence_score", 0.0) or 0.0)
    result = {
        "rca_id": found.get("id", ""),
        "error_signature": found.get("error_signature", error_signature),
        "exact_issue": found.get("exact_issue", ""),
        "root_cause": found.get("root_cause", ""),
        "affected_component": found.get("service_name", service_name),
        "fix_command": found.get("fix_command", ""),
        "fix_explanation": "Reused high-confidence learned pattern from infra history",
        "confidence": "high" if score >= 0.8 else ("medium" if score >= 0.5 else "low"),
        "evidence_lines": [found.get("exact_issue", "")],
        "eta_minutes": int(found.get("resolved_in_minutes", 10) or 10),
    }
    return {
        "result": result if score >= 0.8 else None,
        "confidence": score,
        "from_learning": score >= 0.8,
        "similar": [found] if score >= 0.5 else [],
    }


def save_learning(rca_result: Dict[str, Any], feedback: Dict[str, Any], service_info: Dict[str, Any]) -> Dict[str, Any]:
    service_name = str(service_info.get("service_name", "") or "")
    namespace = str(service_info.get("namespace", "") or "")
    error_signature = str(service_info.get("error_signature", "") or rca_result.get("error_signature", ""))
    feedback_value = str(feedback.get("feedback", "") or "").strip().lower()
    was_correct = feedback_value == "correct"
    if feedback_value == "edited":
        was_correct = True
    correction = str(feedback.get("operator_fix", "") or "")

    llm_model = _normalize_model_name(
        service_info.get("llm_model", "") or rca_result.get("llm_model", "") or feedback.get("llm_model", "")
    )

    base_learning_id = str(rca_result.get("rca_id", "") or "")
    if not base_learning_id:
        digest = hashlib.sha1(f"{namespace}:{service_name}:{error_signature}".encode("utf-8")).hexdigest()
        base_learning_id = digest
    learning_id = f"{base_learning_id}::{llm_model}"

    existing = _es_get_learning_by_id(learning_id) or {}
    occurrence_count = int(existing.get("occurrence_count", 0) or 0) + 1
    previous_conf = float(existing.get("confidence_score", 0.5) or 0.5)
    if feedback_value == "correct":
        confidence = min(1.0, previous_conf + 0.1)
    elif feedback_value == "wrong":
        confidence = max(0.0, previous_conf - 0.2)
    elif feedback_value == "edited":
        confidence = max(0.95, min(1.0, previous_conf + 0.3))
    else:
        confidence = previous_conf

    entry = {
        "id": learning_id,
        "service_name": service_name,
        "namespace": namespace,
        "error_signature": error_signature,
        "anomaly_flags": service_info.get("anomaly_flags", []),
        "exact_issue": correction if feedback_value == "edited" and correction else str(rca_result.get("exact_issue", "") or ""),
        "root_cause": correction if feedback_value == "edited" and correction else str(rca_result.get("root_cause", "") or ""),
        "fix_command": correction if feedback_value == "edited" and correction else str(rca_result.get("fix_command", "") or ""),
        "was_correct": was_correct,
        "operator_correction": correction,
        "confidence_score": confidence,
        "occurrence_count": occurrence_count,
        "last_seen": _now_iso(),
        "service_type": str(service_info.get("service_type", "") or "unknown"),
        "resolved_in_minutes": int(feedback.get("resolved_in_minutes", 0) or 0),
        "created_at": str(existing.get("created_at", _now_iso()) or _now_iso()),
        "llm_model": llm_model,
    }

    _es_index_learning(entry)
    _cache_learned_pattern(entry, ttl_seconds=86400)
    return entry


def save_rca_result(rca_result: Dict[str, Any], service_info: Dict[str, Any]) -> Dict[str, Any]:
    llm_model = _normalize_model_name(service_info.get("llm_model", "") or rca_result.get("llm_model", ""))
    entry = {
        "id": str(rca_result.get("rca_id", "") or hashlib.sha1(json.dumps(rca_result, sort_keys=True).encode("utf-8")).hexdigest()),
        "service_name": str(service_info.get("service_name", "") or ""),
        "namespace": str(service_info.get("namespace", "") or ""),
        "error_signature": str(service_info.get("error_signature", "") or rca_result.get("error_signature", "")),
        "anomaly_flags": service_info.get("anomaly_flags", []),
        "exact_issue": str(rca_result.get("exact_issue", "") or ""),
        "root_cause": str(rca_result.get("root_cause", "") or ""),
        "fix_command": str(rca_result.get("fix_command", "") or ""),
        "was_correct": bool(rca_result.get("from_learning", False)),
        "operator_correction": "",
        "confidence_score": float({"high": 0.9, "medium": 0.6, "low": 0.3}.get(str(rca_result.get("confidence", "low") or "low").lower(), 0.3)),
        "occurrence_count": 1,
        "last_seen": _now_iso(),
        "service_type": str(service_info.get("service_type", "") or "unknown"),
        "resolved_in_minutes": int(service_info.get("resolved_in_minutes", 0) or 0),
        "created_at": _now_iso(),
        "llm_model": llm_model,
    }
    _es_index_learning(entry)
    _cache_learned_pattern(entry, ttl_seconds=86400)
    _file_set_rca_result({
        "id": entry["id"],
        "service_name": entry["service_name"],
        "namespace": entry["namespace"],
        "error_signature": entry["error_signature"],
        "exact_issue": entry["exact_issue"],
        "root_cause": entry["root_cause"],
        "fix_command": entry["fix_command"],
        "confidence_score": entry["confidence_score"],
        "created_at": entry["created_at"],
        "llm_model": entry["llm_model"],
    })
    client = _redis_client()
    if client is not None:
        try:
            client.setex(
                f"rca:result:{entry['id']}",
                3 * 86400,
                json.dumps({
                    'service_name': entry['service_name'],
                    'namespace': entry['namespace'],
                    'error_signature': entry['error_signature'],
                    'exact_issue': entry['exact_issue'],
                    'root_cause': entry['root_cause'],
                    'fix_command': entry['fix_command'],
                    'confidence_score': entry['confidence_score'],
                    'created_at': entry['created_at'],
                    'llm_model': entry['llm_model'],
                }, default=str, separators=(",", ":")),
            )
        except Exception:
            pass
    return entry


def get_saved_rca_result(rca_id: str) -> Optional[Dict[str, Any]]:
    rid = str(rca_id or '').strip()
    if not rid:
        return None
    client = _redis_client()
    if client is not None:
        try:
            raw = client.get(f"rca:result:{rid}")
            if raw:
                loaded = json.loads(raw)
                return loaded if isinstance(loaded, dict) else None
        except Exception:
            pass
    local = _file_get_rca_result(rid)
    if isinstance(local, dict):
        return local
    return _es_get_learning_by_id(rid)


def update_confidence(learning_id: str, was_correct: bool) -> Optional[Dict[str, Any]]:
    current = _es_get_learning_by_id(str(learning_id or ""))
    if not current:
        return None
    conf = float(current.get("confidence_score", 0.5) or 0.5)
    conf = min(1.0, conf + 0.1) if bool(was_correct) else max(0.0, conf - 0.2)
    current["confidence_score"] = conf
    current["was_correct"] = bool(was_correct)
    current["last_seen"] = _now_iso()
    _es_index_learning(current)
    _cache_learned_pattern(current, ttl_seconds=86400)
    _file_upsert_learning(current)
    return current


def get_top_patterns(namespace: str, limit: int = 10, llm_model: Optional[str] = None) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit or 10), 100))
    ns = str(namespace or "").strip()
    selected_model = _normalize_model_name(llm_model) if llm_model is not None else ""

    # Prefer local durable store when ES is unavailable.
    file_rows = _file_get_all_learnings()
    if file_rows:
        filtered = [
            row for row in file_rows
            if isinstance(row, dict)
            and (not ns or str(row.get("namespace", "") or "") == ns)
            and (not selected_model or _entry_model_name(row) == selected_model)
        ]
        filtered.sort(
            key=lambda row: (
                float(row.get("confidence_score", 0.0) or 0.0),
                int(row.get("occurrence_count", 0) or 0),
                str(row.get("last_seen", "") or ""),
            ),
            reverse=True,
        )
        if filtered:
            return filtered[:limit]

    base = _es_base_url()
    if not base or not ensure_learning_index():
        return []
    must = []
    if ns:
        must.append({"term": {"namespace": ns}})
    if selected_model:
        must.append({"term": {"llm_model": selected_model}})
    query = {
        "size": limit,
        "sort": [
            {"confidence_score": {"order": "desc"}},
            {"occurrence_count": {"order": "desc"}},
            {"last_seen": {"order": "desc"}},
        ],
        "query": {"bool": {"must": must}} if must else {"match_all": {}},
    }
    try:
        response = requests.post(f"{base}/{INDEX_NAME}/_search", headers=_es_headers(), data=json.dumps(query), timeout=5)
        if response.status_code != 200:
            return []
        payload = response.json()
        hits = payload.get("hits", {}).get("hits", []) if isinstance(payload, dict) else []
        return [h.get("_source", {}) for h in hits if isinstance(h, dict) and isinstance(h.get("_source", {}), dict)]
    except Exception:
        return []


def record_l0_hit(namespace: str, llm_model: Optional[str] = None) -> None:
    client = _redis_client()
    if client is None:
        return
    day = datetime.utcnow().strftime("%Y%m%d")
    model_suffix = _model_stats_suffix(llm_model or "")
    try:
        client.incr(f"rca:stats:l0:{day}")
        client.incr(f"rca:stats:l0:{day}:{namespace}")
        client.incr(f"rca:stats:l0:{day}:model:{model_suffix}")
        client.incr(f"rca:stats:l0:{day}:{namespace}:model:{model_suffix}")
        client.expire(f"rca:stats:l0:{day}", 7 * 86400)
        client.expire(f"rca:stats:l0:{day}:{namespace}", 7 * 86400)
        client.expire(f"rca:stats:l0:{day}:model:{model_suffix}", 7 * 86400)
        client.expire(f"rca:stats:l0:{day}:{namespace}:model:{model_suffix}", 7 * 86400)
    except Exception:
        return


def record_llm_call(namespace: str, llm_model: Optional[str] = None) -> None:
    client = _redis_client()
    if client is None:
        return
    day = datetime.utcnow().strftime("%Y%m%d")
    model_suffix = _model_stats_suffix(llm_model or "")
    try:
        client.incr(f"rca:stats:llm:{day}")
        client.incr(f"rca:stats:llm:{day}:{namespace}")
        client.incr(f"rca:stats:llm:{day}:model:{model_suffix}")
        client.incr(f"rca:stats:llm:{day}:{namespace}:model:{model_suffix}")
        client.expire(f"rca:stats:llm:{day}", 7 * 86400)
        client.expire(f"rca:stats:llm:{day}:{namespace}", 7 * 86400)
        client.expire(f"rca:stats:llm:{day}:model:{model_suffix}", 7 * 86400)
        client.expire(f"rca:stats:llm:{day}:{namespace}:model:{model_suffix}", 7 * 86400)
    except Exception:
        return


def get_learning_stats(llm_model: Optional[str] = None) -> Dict[str, Any]:
    base = _es_base_url()
    total = 0
    top_issues = []
    namespaces = set()
    accuracy_rate = 0.0
    selected_model = _normalize_model_name(llm_model) if llm_model is not None else ""

    file_rows = _file_get_all_learnings()
    if file_rows:
        issue_counts: Dict[str, int] = {}
        correct = 0
        filtered_total = 0
        for row in file_rows:
            if not isinstance(row, dict):
                continue
            if selected_model and _entry_model_name(row) != selected_model:
                continue
            filtered_total += 1
            issue = str(row.get("exact_issue", "") or "").strip()
            if issue:
                issue_counts[issue] = issue_counts.get(issue, 0) + int(row.get("occurrence_count", 1) or 1)
            ns = str(row.get("namespace", "") or "").strip()
            if ns:
                namespaces.add(ns)
            if bool(row.get("was_correct", False)):
                correct += 1
        total = filtered_total
        top_issues = [k for k, _ in sorted(issue_counts.items(), key=lambda item: item[1], reverse=True)[:5]]
        accuracy_rate = (correct / total) * 100 if total > 0 else 0.0

    elif base and ensure_learning_index():
        try:
            count_query = {"match_all": {}}
            if selected_model:
                count_query = {"bool": {"must": [{"term": {"llm_model": selected_model}}]}}
            count_resp = requests.post(
                f"{base}/{INDEX_NAME}/_count",
                headers=_es_headers(),
                data=json.dumps({"query": count_query}),
                timeout=4,
            )
            if count_resp.status_code == 200:
                total = int(count_resp.json().get("count", 0) or 0)
        except Exception:
            total = 0

        try:
            query_clause = {"match_all": {}}
            if selected_model:
                query_clause = {"bool": {"must": [{"term": {"llm_model": selected_model}}]}}
            agg_query = {
                "size": 0,
                "query": query_clause,
                "aggs": {
                    "top_issues": {"terms": {"field": "exact_issue.keyword", "size": 5}},
                    "namespaces": {"terms": {"field": "namespace", "size": 50}},
                    "correct": {"filter": {"term": {"was_correct": True}}},
                },
            }
            agg_resp = requests.post(f"{base}/{INDEX_NAME}/_search", headers=_es_headers(), data=json.dumps(agg_query), timeout=6)
            if agg_resp.status_code == 200:
                payload = agg_resp.json()
                buckets = payload.get("aggregations", {}).get("top_issues", {}).get("buckets", [])
                top_issues = [b.get("key", "") for b in buckets if isinstance(b, dict)]
                ns_buckets = payload.get("aggregations", {}).get("namespaces", {}).get("buckets", [])
                namespaces = {b.get("key", "") for b in ns_buckets if isinstance(b, dict) and b.get("key")}
                correct = int(payload.get("aggregations", {}).get("correct", {}).get("doc_count", 0) or 0)
                accuracy_rate = (correct / total) * 100 if total > 0 else 0.0
        except Exception:
            pass

    client = _redis_client()
    day = datetime.utcnow().strftime("%Y%m%d")
    l0_hits = 0
    llm_calls = 0
    if client is not None:
        try:
            if selected_model:
                model_suffix = _model_stats_suffix(selected_model)
                l0_hits = int(client.get(f"rca:stats:l0:{day}:model:{model_suffix}") or 0)
                llm_calls = int(client.get(f"rca:stats:llm:{day}:model:{model_suffix}") or 0)
            else:
                l0_hits = int(client.get(f"rca:stats:l0:{day}") or 0)
                llm_calls = int(client.get(f"rca:stats:llm:{day}") or 0)
        except Exception:
            pass

    return {
        "total_learnings": total,
        "l0_hits_today": l0_hits,
        "llm_calls_today": llm_calls,
        "top_issues": top_issues,
        "accuracy_rate": round(accuracy_rate, 2),
        "namespaces_covered": sorted([n for n in namespaces if n]),
        "llm_model": selected_model,
    }


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
        app = Celery("learning_store", broker=broker, backend=broker)
        app.conf.update(
            task_serializer="json",
            result_serializer="json",
            accept_content=["json"],
            task_default_queue="ai-monitoring-agent",
            beat_schedule={
                "rebuild-pattern-index-hourly": {
                    "task": "ai_monitoring_agent.rebuild_pattern_index",
                    "schedule": 3600.0,
                },
                "pattern-decay-daily": {
                    "task": "ai_monitoring_agent.pattern_decay",
                    "schedule": 86400.0,
                },
            },
        )
        _CELERY_APP = app
        return _CELERY_APP
    except Exception:
        _CELERY_APP = None
        return None


if _get_celery_app() is not None:

    @_get_celery_app().task(name="ai_monitoring_agent.process_rca_feedback")
    def process_rca_feedback(rca_id: str, feedback_data: Dict[str, Any]):
        existing = get_saved_rca_result(rca_id) or {}
        rca_result = {
            'rca_id': rca_id,
            'error_signature': feedback_data.get('error_signature', '') or existing.get('error_signature', ''),
            'exact_issue': existing.get('exact_issue', ''),
            'root_cause': existing.get('root_cause', ''),
            'fix_command': existing.get('fix_command', ''),
        }
        return save_learning(
            rca_result=rca_result,
            feedback=feedback_data,
            service_info={
                "service_name": feedback_data.get("service_name", "") or existing.get('service_name', ''),
                "namespace": feedback_data.get("namespace", "") or existing.get('namespace', ''),
                "error_signature": feedback_data.get("error_signature", "") or existing.get('error_signature', ''),
                "llm_model": feedback_data.get("llm_model", "") or existing.get('llm_model', ''),
            },
        )

    @_get_celery_app().task(name="ai_monitoring_agent.rebuild_pattern_index")
    def rebuild_pattern_index():
        patterns = get_top_patterns(namespace="", limit=500)
        for item in patterns:
            score = float(item.get("confidence_score", 0.0) or 0.0)
            if score >= 0.8:
                _cache_learned_pattern(item, ttl_seconds=86400)
        return {"rebuilt": len(patterns)}

    @_get_celery_app().task(name="ai_monitoring_agent.pattern_decay")
    def pattern_decay():
        base = _es_base_url()
        if not base or not ensure_learning_index():
            return {"updated": 0}
        cutoff = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        query = {
            "script": {
                "source": "if (ctx._source.confidence_score != null) { ctx._source.confidence_score = Math.max(0.0, ctx._source.confidence_score - 0.05); }",
                "lang": "painless",
            },
            "query": {
                "range": {"last_seen": {"lt": cutoff}}
            },
        }
        try:
            response = requests.post(
                f"{base}/{INDEX_NAME}/_update_by_query",
                headers=_es_headers(),
                data=json.dumps(query),
                timeout=20,
            )
            if response.status_code != 200:
                return {"updated": 0}
            payload = response.json()
            return {"updated": int(payload.get("updated", 0) or 0)}
        except Exception:
            return {"updated": 0}

else:

    def process_rca_feedback(rca_id: str, feedback_data: Dict[str, Any]):
        existing = get_saved_rca_result(rca_id) or {}
        rca_result = {
            'rca_id': rca_id,
            'error_signature': feedback_data.get('error_signature', '') or existing.get('error_signature', ''),
            'exact_issue': existing.get('exact_issue', ''),
            'root_cause': existing.get('root_cause', ''),
            'fix_command': existing.get('fix_command', ''),
        }
        return save_learning(
            rca_result=rca_result,
            feedback=feedback_data,
            service_info={
                "service_name": feedback_data.get("service_name", "") or existing.get('service_name', ''),
                "namespace": feedback_data.get("namespace", "") or existing.get('namespace', ''),
                "error_signature": feedback_data.get("error_signature", "") or existing.get('error_signature', ''),
                "llm_model": feedback_data.get("llm_model", "") or existing.get('llm_model', ''),
            },
        )

    def rebuild_pattern_index():
        return {"rebuilt": 0}

    def pattern_decay():
        return {"updated": 0}


def enqueue_feedback_task(rca_id: str, feedback_data: Dict[str, Any]) -> Optional[str]:
    app = _get_celery_app()
    if app is None:
        return None
    try:
        task = app.send_task(
            'ai_monitoring_agent.process_rca_feedback',
            args=[str(rca_id or ''), dict(feedback_data or {})],
            queue='ai-monitoring-agent',
        )
        return str(getattr(task, 'id', '') or '')
    except Exception:
        return None
