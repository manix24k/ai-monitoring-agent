"""Issue detection service - polls AI Monitoring Agent for code issues."""

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

from ..config import CodeXAConfig
from ..models import CodeIssue, IssueStatus, IssueType
from ..db.repository import IssueRepository

logger = logging.getLogger("codexa.detector")

# Try to import Elasticsearch client for direct log access
try:
    import sys
    import os
    # Add parent directory to path if needed
    parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    from elasticsearch_client import ElasticsearchClient, ELASTICSEARCH_AVAILABLE
except ImportError:
    ElasticsearchClient = None
    ELASTICSEARCH_AVAILABLE = False

# Patterns to identify code-side issues (not infrastructure)
# Ordered from most specific to least specific
CODE_ISSUE_PATTERNS = [
    # Java NPE - most common
    (r"NullPointerException|NPE|null pointer", IssueType.NULL_POINTER),
    (r"NullPointer|null reference|Cannot invoke.*on null", IssueType.NULL_POINTER),

    # SQL/Database exceptions
    (r"SQLException|DataAccessException|JDBCException|HibernateException", IssueType.SQL_EXCEPTION),
    (r"DataIntegrityViolationException|CannotAcquireLockException|DeadlockLoserDataAccessException", IssueType.SQL_EXCEPTION),
    (r"QueryTimeoutException|PessimisticLockingFailureException|OptimisticLockingFailureException", IssueType.SQL_EXCEPTION),
    (r"BadSqlGrammarException|InvalidDataAccessResourceUsageException|UncategorizedSQLException", IssueType.SQL_EXCEPTION),
    (r"TransactionException|TransactionSystemException|TransactionTimedOutException", IssueType.SQL_EXCEPTION),
    (r"CannotCreateTransactionException|InvalidIsolationLevelException", IssueType.SQL_EXCEPTION),
    (r"EntityNotFoundException|NoResultException|NonUniqueResultException", IssueType.SQL_EXCEPTION),

    # Index/bounds
    (r"IndexOutOfBoundsException|ArrayIndexOutOfBoundsException|StringIndexOutOfBoundsException", IssueType.INDEX_OUT_OF_BOUNDS),
    (r"ArrayStoreException|NegativeArraySizeException", IssueType.INDEX_OUT_OF_BOUNDS),

    # Arguments/state
    (r"IllegalArgumentException|IllegalStateException", IssueType.ILLEGAL_ARGUMENT),
    (r"InvalidParameterException|MissingServletRequestParameterException|MissingPathVariableException", IssueType.ILLEGAL_ARGUMENT),
    (r"TypeMismatchException|MethodArgumentTypeMismatchException", IssueType.ILLEGAL_ARGUMENT),
    (r"NoSuchElementException|EmptyResultDataAccessException|IncorrectResultSizeDataAccessException", IssueType.ILLEGAL_ARGUMENT),

    # Parsing/Format
    (r"NumberFormatException|DateTimeParseException|ParseException", IssueType.NUMBER_FORMAT),
    (r"DateTimeException|DateTimeFormatterException", IssueType.NUMBER_FORMAT),

    # Class loading
    (r"ClassNotFoundException|NoClassDefFoundError|ClassCastException", IssueType.CLASS_NOT_FOUND),
    (r"LinkageError|IncompatibleClassChangeError|AbstractMethodError", IssueType.CLASS_NOT_FOUND),
    (r"NoSuchMethodError|NoSuchFieldError|InstantiationError", IssueType.CLASS_NOT_FOUND),

    # Spring Bean/Context issues
    (r"NoSuchBeanDefinitionException|BeanCreationException|UnsatisfiedDependencyException", IssueType.NO_SUCH_BEAN),
    (r"BeanInstantiationException|BeanInitializationException|BeanNotOfRequiredTypeException", IssueType.NO_SUCH_BEAN),
    (r"NoUniqueBeanDefinitionException|BeanCurrentlyInCreationException", IssueType.NO_SUCH_BEAN),
    (r"ApplicationContextException|ContextRefreshException|CannotLoadBeanClassException", IssueType.NO_SUCH_BEAN),
    (r"Failed to start bean|Failed to instantiate|Failed to refresh", IssueType.NO_SUCH_BEAN),

    # Configuration
    (r"Could not resolve placeholder|ConfigurationException|PropertyNotFoundException", IssueType.CONFIG_ERROR),
    (r"ConversionFailedException|ConversionException|PropertyAccessException", IssueType.CONFIG_ERROR),
    (r"BindException|MissingPropertyException|MissingResourceException", IssueType.CONFIG_ERROR),

    # Validation
    (r"ValidationException|ConstraintViolationException|MethodArgumentNotValidException", IssueType.VALIDATION_ERROR),
    (r"BindException|WebExchangeBindException", IssueType.VALIDATION_ERROR),

    # JSON/Serialization
    (r"JsonParseException|JsonMappingException|JsonProcessingException", IssueType.RUNTIME_ERROR),
    (r"SerializationException|DeserializationException|InvalidTypeIdException", IssueType.RUNTIME_ERROR),
    (r"MismatchedInputException|UnrecognizedPropertyException", IssueType.RUNTIME_ERROR),

    # HTTP/REST
    (r"HttpMessageNotReadableException|HttpMediaTypeNotSupportedException", IssueType.RUNTIME_ERROR),
    (r"HttpRequestMethodNotSupportedException|ResponseStatusException", IssueType.RUNTIME_ERROR),
    (r"MethodNotAllowedException|UnsupportedMediaTypeException", IssueType.RUNTIME_ERROR),
    (r"ResourceNotFoundException|AccessDeniedException|AuthenticationException", IssueType.RUNTIME_ERROR),
    (r"HttpClientErrorException|HttpServerErrorException", IssueType.RUNTIME_ERROR),
    (r"RestClientException|WebClientResponseException|ConnectException", IssueType.RUNTIME_ERROR),

    # Concurrent/Threading
    (r"ConcurrentModificationException|InterruptedException|ExecutionException", IssueType.RUNTIME_ERROR),
    (r"RejectedExecutionException|TimeoutException", IssueType.RUNTIME_ERROR),

    # IO
    (r"IOException|FileNotFoundException|EOFException|SocketException", IssueType.RUNTIME_ERROR),

    # Security
    (r"SecurityException|AccessControlException|PermissionException", IssueType.RUNTIME_ERROR),
    (r"AuthenticationCredentialsNotFoundException|InsufficientAuthenticationException", IssueType.RUNTIME_ERROR),

    # Python specific
    (r"TypeError|ValueError|KeyError|AttributeError", IssueType.RUNTIME_ERROR),
    (r"ImportError|ModuleNotFoundError|NameError", IssueType.CLASS_NOT_FOUND),
    (r"ZeroDivisionError|OverflowError|FloatingPointError", IssueType.NUMBER_FORMAT),

    # Node.js specific
    (r"ReferenceError|SyntaxError|URIError|RangeError", IssueType.RUNTIME_ERROR),

    # Generic (catch-all - should be last)
    (r"\bRuntimeException\b|\bException\b|\bError\b|\bThrowable\b", IssueType.RUNTIME_ERROR),
    (r"FATAL|CRITICAL|SEVERE", IssueType.RUNTIME_ERROR),
]

# Patterns to EXCLUDE for CodeXA detector.
# CodeXA only handles code-level exceptions (NPE, ClassNotFound, etc.).
# Infrastructure connectivity failures (MongoDB/Redis/Kafka down, network timeouts)
# are handled by the AI-agent infra pipeline, not CodeXA.
EXCLUDE_PATTERNS = [
    # Kubernetes infra
    r"\boomkilled\b",
    r"\boutofmemory\b",
    r"\bimagepullbackoff\b|\berrimagepull\b",

    # Telemetry/observability collector noise
    r"fabhotels-signoz-prod-otel-collector",
    r"io\.opentelemetry\.exporter\.internal\.http\.HttpExporter\s*-\s*Failed to export",
    r"otel\.javaagent.*Failed to export (logs|span|spans|metrics|traces)",

    # MongoDB connectivity — driver-level connection/timeout failures (infra)
    r"MongoExceptionTranslator|org\.springframework\.data\.mongodb\.core\.MongoExceptionTranslator",
    r"MongoTimeoutException|MongoSocketReadException|MongoSocketWriteException|MongoSocketOpenException",
    r"MongoNetworkException|MongoServerUnavailableException|MongoNotPrimaryException",
    r"MongoConnectionPoolClearedExc|MongoWaitQueueFullException",
    r"Timed out after \d+ ms while waiting for a server",
    r"com\.mongodb\.MongoException.*timed out|MongoException.*connection",

    # Redis connectivity (infra)
    r"RedisConnectionException|JedisConnectionException|JedisException.*[Cc]onnection",
    r"io\.lettuce\.core\.RedisConnectionException",
    r"Cannot get a resource from the pool|ERR max number of clients reached",
    r"RedisCommandTimeoutException|RedisConnectionFailureException",
    r"org\.springframework\.data\.redis\.RedisConnectionFailureException",

    # Kafka / messaging broker (infra)
    r"org\.apache\.kafka\.common\.errors\.(NetworkException|DisconnectException|BrokerNotAvailableException|LeaderNotAvailableException|NotLeaderOrFollowerException|TimeoutException)",
    r"LEADER_NOT_AVAILABLE|BROKER_NOT_AVAILABLE|NETWORK_EXCEPTION",
    r"Failed to update metadata after \d+",
    r"org\.apache\.kafka\.clients\.(producer|consumer|admin).*TimeoutException",
    r"org\.springframework\.kafka\.KafkaException.*connect|KafkaProducer.*cannot send",

    # RabbitMQ / AMQP broker (infra)
    r"com\.rabbitmq\.client\.(AlreadyClosedException|ShutdownSignalException)",
    r"AmqpConnectException|AmqpIOException|RabbitMQ.*connection",

    # Elasticsearch/OpenSearch connectivity (infra, NOT query/index errors)
    r"org\.elasticsearch\.client.*NoNodeAvailableException|NoNodeAvailableException",
    r"org\.elasticsearch\.transport\.ConnectTransportException",

    # Pure infra network failures — only Netty/JDK frames, no user code reachable
    # NOTE: generic ConnectException is NOT here — it's in INFRA_IF_NO_USER_CODE_PATTERNS
    # so we only skip it when there is truly no user-code frame in the stack trace.
    r"io\.netty\.channel\.AbstractChannel\$AnnotatedConnectException",
    r"sun\.nio\.ch\.Net\.pollConnect|NioSocketChannel.*Connection refused",
]

# Patterns that indicate an infra issue ONLY when no user-code frame exists in
# the stack trace.  If a user-code frame (e.g. com.casa.myservice.SomeClient:45)
# is present alongside one of these, the error is a code bug (wrong URL, wrong
# port hardcoded, missing null-check before HTTP call, etc.) and must NOT be
# filtered — CodeXA should analyze it and generate a PR.
INFRA_IF_NO_USER_CODE_PATTERNS = [
    r"java\.net\.ConnectException",
    r"ECONNREFUSED",
    r"\bConnection refused\b",
    r"\bconnect timed out\b",
    r"\bConnection timed out\b",
    r"Unable to connect to.*:\d+",
    r"Failed to connect to .*(host|server|broker)",
]

# Services to completely ignore (not code issues)
IGNORED_SERVICES = [
    r"fabhotels-signoz-prod-otel-collector",
]

# Additional stack trace patterns for different languages
STACK_TRACE_PATTERNS = [
    # Java: at com.example.Class.method(File.java:123)
    re.compile(r"at\s+([\w.$]+)\.([\w<>]+)\(([\w]+\.java):(\d+)\)"),
    # Python: File "/path/to/file.py", line 123
    re.compile(r'File\s+"([^"]+\.py)",\s+line\s+(\d+)'),
    # Node.js: at Object.<anonymous> (/path/to/file.js:123:45)
    re.compile(r"at\s+[\w.<>]+\s+\(([^:]+\.js):(\d+):\d+\)"),
    # Go: /path/to/file.go:123
    re.compile(r"([^\s]+\.go):(\d+)"),
]

# Framework/library patterns to filter from stack traces (noise)
FRAMEWORK_NOISE_PATTERNS = [
    r"node_modules",
    r"webpack",
    r"__webpack",
    r"java\.lang\.",
    r"java\.util\.",
    r"sun\.reflect\.",
    r"org\.springframework\.aop",
    r"org\.springframework\.cglib",
    r"com\.sun\.proxy",
    r"\$Proxy",
    r"jdk\.internal",
    r"reactor\.core",
    r"io\.netty",
]

# Patterns for message normalization (to avoid duplicate fingerprints)
NORMALIZATION_PATTERNS = [
    # URLs
    (re.compile(r'https?://[^\s<>"\']+'), '<url>'),
    # UUIDs
    (re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'), '<uuid>'),
    # Request paths like /api/v1/users/123
    (re.compile(r'(?<=/)[0-9]+(?=/|$|\s)'), '<id>'),
    # IP addresses
    (re.compile(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'), '<ip>'),
    # Timestamps ISO format
    (re.compile(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?'), '<ts>'),
    # Unix timestamps (13 digits for ms, 10 for seconds)
    (re.compile(r'\b1[0-9]{12}\b'), '<ts_ms>'),
    (re.compile(r'\b1[0-9]{9}\b'), '<ts_s>'),
    # Hex values (like memory addresses, hashes)
    (re.compile(r'\b0x[0-9a-fA-F]+\b'), '<hex>'),
    (re.compile(r'\b[0-9a-fA-F]{32,}\b'), '<hash>'),
    # Email addresses
    (re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'), '<email>'),
    # Generic long numbers (order IDs, transaction IDs, etc.)
    (re.compile(r'\b\d{6,}\b'), '<n>'),
]


def _load_allowed_services() -> set:
    """Load CODEXA_ALLOWED_SERVICES env var (comma-separated service names).

    Empty = all services allowed (normal mode).
    Non-empty = demo/restricted mode — only listed services are processed.
    Matching is fuzzy: dashes, underscores, and dots are stripped before compare.
    """
    raw = os.environ.get("CODEXA_ALLOWED_SERVICES", "").strip()
    if not raw:
        return set()
    return {re.sub(r"[-_.]", "", s.strip().lower()) for s in raw.split(",") if s.strip()}


class IssueDetector:
    """Detects code issues from AI Monitoring Agent and Elasticsearch logs."""

    def __init__(self, config: CodeXAConfig, repository: IssueRepository, es_client=None):
        self.config = config
        self.repository = repository
        self._es_client = es_client  # ElasticsearchClient instance
        self._allowed_services: set = _load_allowed_services()
        if self._allowed_services:
            logger.info(f"CodeXA DEMO MODE: monitoring only → {self._allowed_services}")

    def set_elasticsearch_client(self, es_client):
        """Set Elasticsearch client for direct log access."""
        self._es_client = es_client

    def _is_allowed_service(self, service_name: str) -> bool:
        """Return True if this service should be processed.

        When CODEXA_ALLOWED_SERVICES is empty (normal mode) every service passes.
        In demo/restricted mode only exact matches (after stripping dashes/underscores)
        are allowed. bus-booking does NOT match bus-booking-search-service.
        """
        if not self._allowed_services:
            return True
        norm = re.sub(r"[-_.]", "", (service_name or "").strip().lower())
        return norm in self._allowed_services

    @staticmethod
    def _is_generic_exception_label(label: str) -> bool:
        value = str(label or '').strip().lower()
        return value in {'error', 'exception', 'npe', 'runtimeexception'}

    @classmethod
    def _has_user_code_frame(cls, text: str) -> bool:
        """Return True if the stack trace contains at least one non-framework frame.

        Used to distinguish:
        - Infra ConnectException (only Netty/JDK frames) → skip
        - Code ConnectException (user's class in stack, e.g. wrong hardcoded URL) → keep
        """
        java_pat = STACK_TRACE_PATTERNS[0]
        for match in java_pat.finditer(text):
            fqn = match.group(1)
            if not cls._is_framework_class(fqn):
                return True
        return False

    @classmethod
    def _is_infra_connectivity_only(cls, text: str) -> bool:
        """Return True if the text matches a connection-refused/timeout pattern
        AND has no user-code frame — meaning this is pure infra, not a code bug.
        """
        matched = any(
            re.search(p, text, re.IGNORECASE)
            for p in INFRA_IF_NO_USER_CODE_PATTERNS
        )
        if not matched:
            return False
        # Has a user-code frame → developer's code is in the call chain → code bug
        return not cls._has_user_code_frame(text)

    @staticmethod
    def _has_strong_code_signal(text: str) -> bool:
        raw = str(text or '')
        lower = raw.lower()
        return (
            'caused by:' in lower
            or 'traceback' in lower
            or re.search(r"\bat\s+[\w.$]+\([\w.$]+:\d+\)", raw) is not None
            or re.search(r"\b\w+\.(java|kt|py|js|ts|go):\d+\b", raw) is not None
            or re.search(r"\b\w+(Exception|Error)\b", raw) is not None
        )

    @staticmethod
    def _clean_log_text(text: str) -> str:
        raw = str(text or "")
        # Strip ANSI color codes from app logs.
        return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", raw)

    def _is_non_error_info_log(self, text: str) -> bool:
        cleaned = self._clean_log_text(text)
        lower = cleaned.lower()

        if "status=true" in lower and "errors=null" in lower:
            return True

        is_info = re.search(r"\binfo\b", cleaned, re.IGNORECASE) is not None
        has_error_marker = re.search(
            r"\b(error|exception|traceback|fatal|severe|critical|caused by|warn)\b",
            cleaned,
            re.IGNORECASE,
        ) is not None
        return bool(is_info and not has_error_marker)

    def _find_existing_issue(self, issue: CodeIssue) -> Optional[CodeIssue]:
        """Best-effort dedupe across repository versions."""
        signature = str(getattr(issue, "error_signature", "") or "").strip()
        getter = getattr(self.repository, "get_issue_by_signature", None)
        if callable(getter) and signature:
            try:
                return getter(signature)
            except Exception:
                pass

        # Backward-compatible fallback for older repository implementations.
        checker = getattr(self.repository, "issue_exists", None)
        if callable(checker):
            try:
                exists = checker(
                    str(getattr(issue, "service_name", "") or ""),
                    str(getattr(issue, "exception_type", "") or ""),
                    str(getattr(issue, "exception_message", "") or ""),
                )
                if exists:
                    return issue
            except Exception:
                pass
        return None

    async def detect_issues(self, window_minutes: int = 30) -> List[CodeIssue]:
        """Poll monitoring agent and Elasticsearch for code issues."""
        logger.info("Starting issue detection cycle...")
        detected = []
        # Track signatures saved THIS cycle so Phase 1/1b/2 don't duplicate each other
        saved_this_cycle: set = set()
        effective_window = max(5, min(int(window_minutes or 30), 24 * 60))

        def _save_new(issue):
            sig = str(getattr(issue, 'error_signature', '') or '')
            existing = self._find_existing_issue(issue)
            if existing:
                # Only increment if this log is genuinely newer than last recorded
                new_ts = getattr(issue, 'last_seen_at', None) or getattr(issue, 'detected_at', None)
                if hasattr(self.repository, 'increment_occurrence'):
                    self.repository.increment_occurrence(
                        str(getattr(existing, 'id', '') or ''), new_ts=new_ts)
                return
            if sig and sig in saved_this_cycle:
                return  # same issue seen in another phase this cycle
            self.repository.save_issue(issue)
            if sig:
                saved_this_cycle.add(sig)
            detected.append(issue)

        try:
            # Phase 1: Fetch from AI Monitoring Agent API
            services = await self._fetch_services(effective_window)
            logger.info(f"Fetched {len(services)} services from monitoring agent")

            for svc_key, svc_data in services.items():
                for issue in self._analyze_service(svc_key, svc_data):
                    _save_new(issue)

            # Phase 1b: Fetch detailed service issues from AI-agent issue feed.
            detailed_issues = await self._fetch_detailed_service_issues(services, effective_window)
            for issue in detailed_issues:
                _save_new(issue)

            # Phase 2: Direct Elasticsearch scan for error logs
            if self._es_client is not None:
                for issue in self._scan_elasticsearch_logs(minutes=effective_window):
                    _save_new(issue)

            logger.info(f"Detection cycle complete. {len(detected)} new issues found.")

        except Exception as e:
            logger.error(f"Detection failed: {e}")

        return detected

    async def _fetch_detailed_service_issues(self, services: Dict[str, Any], window_minutes: int) -> List[CodeIssue]:
        """Fetch per-service detailed issues from AI-agent /api/service-issues."""
        if not isinstance(services, dict) or not services:
            return []

        base_url = f"{self.config.monitoring_agent.url}{self.config.monitoring_agent.api_prefix}/api/service-issues"
        timeout = aiohttp.ClientTimeout(total=45)
        sem = asyncio.Semaphore(10)
        issues: List[CodeIssue] = []

        candidates = []
        for svc_key, svc_data in services.items():
            if not isinstance(svc_data, dict):
                continue
            namespace = str(svc_data.get("namespace", "") or "").strip().lower()
            service_name = str(svc_data.get("name", "") or "").strip()
            if not service_name and "/" in str(svc_key):
                ns_part, name_part = str(svc_key).split("/", 1)
                namespace = namespace or ns_part.strip().lower()
                service_name = name_part.strip()
            if not namespace or not service_name:
                continue
            if namespace not in {"jupiter", "venus"}:
                continue
            # Keep requests focused on services that currently carry error/issue signal.
            recent_errors = svc_data.get("recent_errors", []) if isinstance(svc_data.get("recent_errors", []), list) else []
            svc_state = str(svc_data.get("status", "") or "").strip().lower()
            has_signal = bool(recent_errors) or svc_state in {"degraded", "down", "pending", "warning"}
            if has_signal:
                repo_name = str(svc_data.get("repo_name", "") or "").strip()
                candidates.append((namespace, service_name, repo_name))

        # Cap per cycle to avoid overloading dashboard API.
        candidates = candidates[:120]

        async def _fetch_one(session: aiohttp.ClientSession, namespace: str, service_name: str, repo_name: str) -> List[CodeIssue]:
            params = {
                "namespace": namespace,
                "service": service_name,
                "window_minutes": max(5, int(window_minutes or 30)),
                "limit": 300,
            }
            async with sem:
                try:
                    async with session.get(base_url, params=params) as response:
                        if response.status != 200:
                            return []
                        payload = await response.json()
                except Exception:
                    return []

            rows = payload.get("errors", []) if isinstance(payload, dict) else []
            if not isinstance(rows, list):
                return []
            out: List[CodeIssue] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                parsed = self._parse_service_issue_row(row, service_name, namespace, repo_name=repo_name)
                if parsed:
                    out.append(parsed)
            return out

        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [_fetch_one(session, ns, svc, repo_name) for ns, svc, repo_name in candidates]
            if not tasks:
                return []
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for batch in results:
            if isinstance(batch, Exception):
                continue
            if isinstance(batch, list):
                issues.extend(batch)
        return issues

    def _parse_service_issue_row(self, row: Dict[str, Any], service_name: str, namespace: str, repo_name: str = "") -> Optional[CodeIssue]:
        """Parse /api/service-issues row into CodeIssue."""
        if not self._is_allowed_service(service_name):
            return None
        message = str(row.get("message", "") or "").strip()
        root_cause = str(row.get("root_cause", "") or "").strip()
        issue_text = str(row.get("issue", "") or "").strip()
        reason = str(row.get("reason", "") or "").strip()
        stacktrace = str(row.get("stacktrace", "") or "").strip()

        combined = " ".join([issue_text, reason, root_cause, message, stacktrace]).strip()
        if not combined:
            return None
        if self._is_non_error_info_log(combined):
            return None

        # Hard infra exclusions (always skip)
        for pattern in EXCLUDE_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return None
        # Conditional: ConnectException/refused only infra if NO user-code frame exists
        if self._is_infra_connectivity_only(combined):
            return None

        issue_type = IssueType.OTHER
        exception_type = ""
        for pattern, itype in CODE_ISSUE_PATTERNS:
            match = re.search(pattern, combined, re.IGNORECASE)
            if match:
                issue_type = itype
                exception_type = match.group(0)
                break
        if not exception_type:
            exc_match = re.search(r"\b(\w+(?:Exception|Error))\b", combined)
            if exc_match:
                exception_type = exc_match.group(1)
            else:
                return None

        file_path, line_number, _, _ = self._extract_location_info(f"{combined}\n{stacktrace}", service_name=service_name)

        if self._is_generic_exception_label(exception_type):
            evidence = f"{combined}\n{stacktrace}"
            if not self._has_strong_code_signal(evidence) and not file_path and not line_number:
                return None

        incoming_fp = str(row.get("fingerprint", "") or "").strip()
        signature = incoming_fp or self._generate_fingerprint(
            service_name,
            namespace,
            exception_type,
            combined,
            stacktrace or root_cause,
        )

        detected_at = datetime.utcnow()
        for ts_key in ("last_seen_at", "detected_at", "timestamp"):
            ts_raw = str(row.get(ts_key, "") or "").strip()
            if not ts_raw:
                continue
            try:
                detected_at = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).replace(tzinfo=None)
                break
            except Exception:
                continue

        return CodeIssue(
            service_name=service_name,
            namespace=namespace,
            error_signature=signature,
            exception_type=exception_type,
            exception_message=(message or root_cause or issue_text)[:500],
            stack_trace=(stacktrace or root_cause)[:2000],
            log_evidence=combined[:1000],
            file_path=file_path,
            line_number=line_number,
            issue_type=issue_type,
            confidence=0.9 if stacktrace else 0.8,
            status=IssueStatus.PENDING,
            detected_at=detected_at,
            repo_name=str(repo_name or service_name),
        )

    def _scan_elasticsearch_logs(self, minutes: int = 60) -> List[CodeIssue]:
        """Scan Elasticsearch for error logs across ALL user indices.

        Strategy:
        1. Enumerate every user index via _cat/indices to build a reliable
           service-name → [indices] map (no dependency on document fields).
        2. Search each service's indices for exception/error patterns.
        3. Stamp the service name from the index name so it's always correct
           even when kubernetes.* metadata is absent from documents.
        """
        issues = []
        if self._es_client is None:
            return issues

        try:
            now = datetime.now()
            start_time = int((now - timedelta(minutes=minutes)).timestamp() * 1000)
            end_time = int(now.timestamp() * 1000)

            query_string = (
                "message:*exception* OR message:*Exception* OR "
                "message:*error* OR message:*Error* OR "
                "log:*exception* OR log:*Exception* OR "
                "message:\"Caused by\" OR message:\"caused by\""
            )

            # --- Step 1: enumerate all user indices from ES ---
            service_indices = {}
            if hasattr(self._es_client, 'list_service_indices'):
                service_indices = self._es_client.list_service_indices()
                logger.info(f"ES index discovery found {len(service_indices)} services: "
                            f"{sorted(service_indices.keys())}")

            if service_indices:
                # --- Step 2a: search per service using its own indices ---
                # Cap: max 2 unique issues per service to avoid flooding
                MAX_ISSUES_PER_SERVICE = 2
                seen_sigs: set = set()
                for svc_name, idx_list in service_indices.items():
                    index_pattern = ",".join(idx_list)
                    try:
                        logs = self._es_client.search_logs(
                            query_string=query_string,
                            start_time=start_time,
                            end_time=end_time,
                            limit=50,
                            index=index_pattern,
                        )
                    except Exception as _e:
                        logger.debug(f"ES search failed for {svc_name}: {_e}")
                        continue

                    svc_issue_count = 0
                    for log_entry in logs:
                        if svc_issue_count >= MAX_ISSUES_PER_SERVICE:
                            break
                        log_entry['_detected_service'] = svc_name
                        issue = self._parse_elasticsearch_log(log_entry)
                        if issue and issue.error_signature not in seen_sigs:
                            seen_sigs.add(issue.error_signature)
                            issues.append(issue)
                            svc_issue_count += 1

                logger.info(f"ES per-service scan → {len(issues)} issues from "
                            f"{len(service_indices)} services (cap={MAX_ISSUES_PER_SERVICE}/svc)")
            else:
                # --- Step 2b: fallback — search all indices at once ---
                logger.warning("ES index discovery returned nothing, falling back to *-*,-.*")
                logs = self._es_client.search_logs(
                    query_string=query_string,
                    start_time=start_time,
                    end_time=end_time,
                    limit=500,
                    index="*-*,-.*",
                )
                logger.info(f"ES fallback search returned {len(logs)} logs")
                for log_entry in logs:
                    issue = self._parse_elasticsearch_log(log_entry)
                    if issue:
                        issues.append(issue)

        except Exception as e:
            logger.error(f"Elasticsearch scan failed: {e}", exc_info=True)

        return issues

    def _parse_elasticsearch_log(self, log_entry: Dict[str, Any]) -> Optional[CodeIssue]:
        """Parse an Elasticsearch log entry into a CodeIssue."""
        try:
            # Extract message - handle JSON encoded logs
            message = self._extract_message(log_entry)
            if not message:
                return None
            if self._is_non_error_info_log(message):
                return None

            # Use pre-stamped service name (from index) when available; fall back
            # to field extraction for the older single-query path.
            service_name = (str(log_entry.get('_detected_service') or '').strip()
                            or self._extract_service_name(log_entry))
            namespace = self._extract_namespace(log_entry)

            if not service_name:
                return None

            if not self._is_allowed_service(service_name):
                return None

            # Hard infra exclusions (always skip)
            for pattern in EXCLUDE_PATTERNS:
                if re.search(pattern, message, re.IGNORECASE):
                    return None
            # Conditional: ConnectException/refused only infra if NO user-code frame
            if self._is_infra_connectivity_only(message):
                return None

            # Check if it's a code issue
            issue_type = IssueType.OTHER
            exception_type = ""

            for pattern, itype in CODE_ISSUE_PATTERNS:
                match = re.search(pattern, message, re.IGNORECASE)
                if match:
                    issue_type = itype
                    exception_type = match.group(0)
                    break

            if issue_type == IssueType.OTHER:
                exc_match = re.search(r"\b(\w+(?:Exception|Error))\b", message)
                if exc_match:
                    exception_type = exc_match.group(1)
                else:
                    return None

            # Extract stack trace from message
            stack_trace = self._extract_stack_trace(message)

            # Extract file/line info
            file_path, line_number, class_name, method_name = self._extract_location_info(message + " " + stack_trace, service_name=service_name)

            if self._is_generic_exception_label(exception_type):
                evidence = f"{message}\n{stack_trace}"
                if not self._has_strong_code_signal(evidence) and not file_path and not line_number:
                    return None

            # Generate stable fingerprint for deduplication
            signature = self._generate_fingerprint(
                service_name, namespace, exception_type, message, stack_trace
            )

            # Extract the log's actual timestamp so re-scans of the same log
            # entry don't increment occurrence_count (dedup via timestamp check).
            ts_raw = str(log_entry.get('@timestamp', '')
                         or log_entry.get('timestamp', '')
                         or log_entry.get('detected_at', '')).strip()
            log_ts = datetime.utcnow()
            if ts_raw:
                try:
                    log_ts = datetime.fromisoformat(ts_raw.replace('Z', '+00:00')).replace(tzinfo=None)
                except Exception:
                    pass

            return CodeIssue(
                service_name=service_name,
                namespace=namespace,
                error_signature=signature,
                exception_type=exception_type,
                exception_message=message[:500],
                stack_trace=stack_trace[:2000],
                log_evidence=message[:1000],
                file_path=file_path,
                line_number=line_number,
                issue_type=issue_type,
                confidence=0.85 if stack_trace else 0.7,
                status=IssueStatus.PENDING,
                detected_at=log_ts,
                last_seen_at=log_ts,
            )
        except Exception as e:
            logger.error(f"Error parsing ES log: {e}")
            return None

    def _extract_message(self, log_entry: Dict[str, Any]) -> str:
        """Extract message from log entry, handling JSON and nested structures."""
        message = ""

        # Try direct message field
        if "message" in log_entry:
            msg = log_entry["message"]
            if isinstance(msg, str):
                message = msg
                # Try to parse if it's JSON encoded
                if msg.startswith("{") or msg.startswith("["):
                    try:
                        parsed = json.loads(msg)
                        if isinstance(parsed, dict):
                            message = parsed.get("message", "") or parsed.get("msg", "") or parsed.get("error", "") or str(parsed)
                            # Also check for nested stack trace
                            if "stackTrace" in parsed:
                                message += "\n" + str(parsed["stackTrace"])
                            if "stack_trace" in parsed:
                                message += "\n" + str(parsed["stack_trace"])
                            if "exception" in parsed:
                                message += "\n" + str(parsed["exception"])
                    except json.JSONDecodeError:
                        pass

        # Try log field (common in filebeat)
        if not message and "log" in log_entry:
            log_val = log_entry["log"]
            if isinstance(log_val, str):
                message = log_val
            elif isinstance(log_val, dict):
                message = log_val.get("message", "") or log_val.get("msg", "")

        # Try error field
        if not message and "error" in log_entry:
            err = log_entry["error"]
            if isinstance(err, str):
                message = err
            elif isinstance(err, dict):
                message = err.get("message", "") or err.get("msg", "") or str(err)
                if "stack_trace" in err:
                    message += "\n" + str(err["stack_trace"])

        # Try exception field
        if not message and "exception" in log_entry:
            exc = log_entry["exception"]
            if isinstance(exc, str):
                message = exc
            elif isinstance(exc, dict):
                message = exc.get("message", "") or str(exc)

        return message.strip()

    def _extract_service_name(self, log_entry: Dict[str, Any]) -> str:
        """Extract service name from log entry."""
        # Try kubernetes container name
        kubernetes = log_entry.get("kubernetes", {})
        if isinstance(kubernetes, dict):
            container = kubernetes.get("container", {})
            if isinstance(container, dict) and container.get("name"):
                return str(container["name"])

            labels = kubernetes.get("labels", {})
            if isinstance(labels, dict):
                for key in ("app", "app.kubernetes.io/name", "k8s-app", "component"):
                    if labels.get(key):
                        return str(labels[key])

            pod = kubernetes.get("pod", {})
            if isinstance(pod, dict) and pod.get("name"):
                # Extract service name from pod name (remove hash suffix)
                pod_name = str(pod["name"])
                return pod_name.rsplit("-", 2)[0]

        # Try service field
        service = log_entry.get("service", {})
        if isinstance(service, dict) and service.get("name"):
            return str(service["name"])

        # Try flat dot-notation fields (some Elastic Agent / fluentbit setups)
        for field in ["kubernetes.container.name", "container_name", "service.name",
                      "kubernetes.labels.app", "kubernetes.labels.k8s-app"]:
            if log_entry.get(field):
                return str(log_entry[field])

        # Last resort: derive from the ES index name (e.g. trip-management-search-2026.06.24)
        idx = str(log_entry.get("_index", "") or "")
        if idx:
            # Strip trailing date YYYY.MM.DD
            svc = re.sub(r"-\d{4}\.\d{2}\.\d{2}$", "", idx).strip("-")
            if svc:
                return svc

        return ""

    def _extract_namespace(self, log_entry: Dict[str, Any]) -> str:
        """Extract namespace from log entry."""
        kubernetes = log_entry.get("kubernetes", {})
        if isinstance(kubernetes, dict):
            for field in ("namespace", "namespace_name"):
                if kubernetes.get(field):
                    return str(kubernetes[field])

        # Try flat fields
        for field in ["kubernetes.namespace", "kubernetes.namespace_name", "kubernetes_namespace"]:
            if log_entry.get(field):
                return str(log_entry[field])

        return "unknown"

    def _extract_stack_trace(self, message: str) -> str:
        """Extract stack trace from log message."""
        lines = message.split("\n")
        stack_lines = []
        in_stack = False

        for line in lines:
            # Check for stack trace start patterns
            if re.search(r"\bat\s+[\w.$]+\.", line):  # Java: at com.example.Class.method
                in_stack = True
                stack_lines.append(line)
            elif re.search(r'File\s+"[^"]+\.py"', line):  # Python: File "/path/file.py"
                in_stack = True
                stack_lines.append(line)
            elif re.search(r"at\s+[\w.<>]+\s+\([^)]+\.js:", line):  # Node.js
                in_stack = True
                stack_lines.append(line)
            elif re.search(r"^\s+at\s+", line):  # Generic stack trace line
                stack_lines.append(line)
            elif in_stack and re.search(r"^\s+(Caused by:|\.\.\.|\d+ more)", line):
                stack_lines.append(line)
            elif in_stack and line.strip() == "":
                continue
            elif in_stack and not line.startswith("\t") and not line.startswith(" " * 4):
                in_stack = False

        return "\n".join(stack_lines)

    # Framework/library package prefixes to skip when choosing a file to fix.
    # We prefer the first frame from user's own code (com.casa.*, etc.)
    _FRAMEWORK_PREFIXES = (
        "org.springframework.", "org.hibernate.", "org.apache.",
        "org.aspectj.", "org.jboss.", "org.slf4j.", "org.objectweb.",
        "java.", "javax.", "jakarta.", "sun.", "com.sun.",
        "io.netty.", "reactor.", "io.micrometer.", "io.opentelemetry.",
        "com.zaxxer.", "com.mongodb.", "redis.", "io.lettuce.",
        "net.bytebuddy.", "jdk.", "ch.qos.",
    )

    @classmethod
    def _is_framework_class(cls, class_fqn: str) -> bool:
        return any(class_fqn.startswith(p) for p in cls._FRAMEWORK_PREFIXES)

    def _extract_location_info(self, text: str, service_name: str = "") -> tuple:
        """Extract file path, line number, class name, method name from text.

        Priority order for Java frames:
        1. Frame whose package contains the service name  (most specific)
        2. Any non-framework frame
        3. First frame of any kind (absolute fallback)
        """
        # Normalize service name for package matching: "b2b-aggregation" → "b2baggregation"
        svc_norm = re.sub(r"[-_.]", "", (service_name or "").lower())

        best_svc_match = None   # frame from service's own package
        best_user_match = None  # any non-framework frame
        best_fallback = None    # absolute first frame

        java_pat = STACK_TRACE_PATTERNS[0]  # at com.example.Class.method(File.java:123)
        for match in java_pat.finditer(text):
            groups = match.groups()
            if len(groups) < 4:
                continue
            fqn = groups[0]
            m_name = groups[1]
            f_path = groups[2]
            try:
                ln = int(groups[3])
            except ValueError:
                continue

            candidate = (f_path, ln, fqn, m_name)

            if best_fallback is None:
                best_fallback = candidate

            if self._is_framework_class(fqn):
                continue

            if best_user_match is None:
                best_user_match = candidate

            # Check if FQN contains service name (e.g. b2baggregation in com.casa.b2baggregation.*)
            fqn_norm = re.sub(r"[-_.]", "", fqn.lower())
            if svc_norm and svc_norm in fqn_norm and best_svc_match is None:
                best_svc_match = candidate

        result = best_svc_match or best_user_match or best_fallback
        if result:
            return result

        # Non-Java patterns (Python, Node.js, Go) — take first match
        for pattern in STACK_TRACE_PATTERNS[1:]:
            match = pattern.search(text)
            if match:
                groups = match.groups()
                if len(groups) >= 2:
                    file_path = groups[0] if not groups[0].isdigit() else ""
                    try:
                        line_number = int(groups[1]) if groups[1].isdigit() else 0
                    except (ValueError, IndexError):
                        line_number = 0
                    return file_path, line_number, "", ""

        return "", 0, "", ""

    def _normalize_message(self, message: str) -> str:
        """Normalize message by replacing variable content with tokens.

        This prevents false duplicates from things like:
        - Different request IDs/UUIDs in same error
        - Different timestamps
        - Different user IDs in same exception
        """
        normalized = message
        for pattern, replacement in NORMALIZATION_PATTERNS:
            normalized = pattern.sub(replacement, normalized)
        return normalized

    def _filter_framework_noise(self, stack_trace: str) -> str:
        """Filter out framework/library frames from stack trace.

        Keeps only user-relevant frames for better fingerprinting.
        """
        if not stack_trace:
            return ""

        lines = stack_trace.split("\n")
        filtered_lines = []
        user_frames = []

        for line in lines:
            # Check if this is a framework/library line
            is_noise = False
            for pattern in FRAMEWORK_NOISE_PATTERNS:
                if re.search(pattern, line, re.IGNORECASE):
                    is_noise = True
                    break

            if not is_noise:
                filtered_lines.append(line)
                # Track user frames for extraction
                if re.search(r"at\s+[\w.$]+\.", line) or re.search(r'File\s+"', line):
                    user_frames.append(line)

        # Return filtered stack trace, keeping top 10 user frames
        if user_frames:
            return "\n".join(user_frames[:10])
        return "\n".join(filtered_lines[:20])

    def _generate_fingerprint(self, service_name: str, namespace: str,
                               exception_type: str, message: str, stack_trace: str) -> str:
        """Generate a stable fingerprint for deduplication.

        Uses normalized message and filtered stack trace for better grouping.
        """
        # Normalize the message to remove variable content
        normalized_msg = self._normalize_message(message[:200])

        # Filter framework noise from stack trace
        filtered_stack = self._filter_framework_noise(stack_trace)

        # Extract first user frame for fingerprint
        first_frame = ""
        if filtered_stack:
            lines = filtered_stack.split("\n")
            if lines:
                first_frame = lines[0][:100]

        # Build fingerprint input
        sig_parts = [
            service_name,
            namespace,
            exception_type,
            normalized_msg,
            first_frame
        ]
        sig_input = "|".join(sig_parts)

        return hashlib.sha256(sig_input.encode()).hexdigest()[:32]

    async def _fetch_services(self, window_minutes: int = 30) -> Dict[str, Any]:
        """Fetch services from AI Monitoring Agent API."""
        # Correct endpoint is /api/service-status (not /api/services/status)
        url = f"{self.config.monitoring_agent.url}{self.config.monitoring_agent.api_prefix}/api/service-status"

        try:
            # Request all services from all namespaces and align with selected time scope.
            params = {
                "page_size": 500,
                "namespace": "all",
                "window_minutes": max(5, int(window_minutes or 30)),
                "incident_window_minutes": max(5, int(window_minutes or 30)),
            }
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Fetched service-status response with {len(data.get('services', {}))} services")
                        return data.get("services", {}) or {}
                    logger.warning(f"Failed to fetch services: HTTP {response.status}")
                    return {}
        except Exception as e:
            logger.error(f"Error fetching services: {e}")
            return {}

    def _analyze_service(self, svc_key: str, svc_data: Dict[str, Any]) -> List[CodeIssue]:
        """Analyze service data for code issues."""
        issues = []

        if not isinstance(svc_data, dict):
            return issues

        # Extract service info
        namespace = str(svc_data.get("namespace", "") or "")
        service_name = str(svc_data.get("name", "") or "")
        if not service_name and "/" in svc_key:
            parts = svc_key.split("/", 1)
            namespace = namespace or parts[0]
            service_name = parts[1]

        if not service_name:
            return issues

        repo_name = str(svc_data.get("repo_name", "") or "").strip()

        # Check if service should be ignored
        for pattern in IGNORED_SERVICES:
            if re.search(pattern, service_name, re.IGNORECASE):
                logger.debug(f"Ignoring service: {service_name}")
                return issues

        # Check recent errors
        recent_errors = svc_data.get("recent_errors", []) or []
        for error in recent_errors:
            if not isinstance(error, dict):
                continue

            issue = self._parse_error(error, service_name, namespace)
            if issue:
                if repo_name:
                    issue.repo_name = repo_name
                issues.append(issue)

        # Check deep inspection
        deep = svc_data.get("deep_inspection", {}) or {}
        if deep:
            issue = self._parse_deep_inspection(deep, service_name, namespace)
            if issue:
                if repo_name:
                    issue.repo_name = repo_name
                issues.append(issue)

        # Check issue/reason/root_cause fields directly on service
        issue_text = str(svc_data.get("issue", "") or "")
        reason_text = str(svc_data.get("reason", "") or "")
        root_cause_text = str(svc_data.get("root_cause", "") or "")

        if issue_text or reason_text or root_cause_text:
            combined = f"{issue_text} {reason_text} {root_cause_text}"
            if self._contains_code_exception(combined):
                error_dict = {
                    "issue": issue_text,
                    "message": reason_text,
                    "reason": reason_text,
                    "root_cause": root_cause_text
                }
                issue = self._parse_error(error_dict, service_name, namespace)
                if issue:
                    if repo_name:
                        issue.repo_name = repo_name
                    issues.append(issue)

        # Check signal field for exceptions
        signal = str(svc_data.get("signal", "") or "")
        if signal and signal.lower() not in ["ok", "none", ""]:
            signal_data = svc_data.get("signal_data", {}) or {}
            if self._contains_code_exception(signal):
                error_dict = {
                    "issue": signal,
                    "message": str(signal_data.get("message", "") or signal),
                    "reason": str(signal_data.get("reason", "") or ""),
                    "root_cause": str(signal_data.get("root_cause", "") or "")
                }
                issue = self._parse_error(error_dict, service_name, namespace)
                if issue:
                    if repo_name:
                        issue.repo_name = repo_name
                    issues.append(issue)

        # Check latest_error_message field (from ES overview)
        latest_error = str(svc_data.get("latest_error_message", "") or "")
        if latest_error and self._contains_code_exception(latest_error):
            error_dict = {
                "issue": latest_error,
                "message": latest_error,
                "reason": "",
                "root_cause": ""
            }
            issue = self._parse_error(error_dict, service_name, namespace)
            if issue:
                if repo_name:
                    issue.repo_name = repo_name
                issues.append(issue)

        # Check error field directly
        error_field = str(svc_data.get("error", "") or "")
        if error_field and self._contains_code_exception(error_field):
            error_dict = {
                "issue": error_field,
                "message": error_field,
                "reason": "",
                "root_cause": ""
            }
            issue = self._parse_error(error_dict, service_name, namespace)
            if issue:
                if repo_name:
                    issue.repo_name = repo_name
                issues.append(issue)

        # Check status_reason field (may contain exception info)
        status_reason = str(svc_data.get("status_reason", "") or "")
        if status_reason and self._contains_code_exception(status_reason):
            error_dict = {
                "issue": status_reason,
                "message": status_reason,
                "reason": "",
                "root_cause": ""
            }
            issue = self._parse_error(error_dict, service_name, namespace)
            if issue:
                if repo_name:
                    issue.repo_name = repo_name
                issues.append(issue)

        return issues

    def _contains_code_exception(self, text: str) -> bool:
        """Check if text contains any code-side exception pattern."""
        if not text:
            return False
        for pattern, _ in CODE_ISSUE_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    def _parse_error(
        self, error: Dict[str, Any], service_name: str, namespace: str
    ) -> Optional[CodeIssue]:
        """Parse error dict into CodeIssue."""
        if not self._is_allowed_service(service_name):
            return None
        issue_text = str(error.get("issue", "") or "")
        message = str(error.get("message", "") or "")
        reason = str(error.get("reason", "") or "")
        root_cause = str(error.get("root_cause", "") or "")

        combined = f"{issue_text} {message} {reason} {root_cause}"
        if self._is_non_error_info_log(combined):
            return None

        # Hard infra exclusions (always skip)
        for pattern in EXCLUDE_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return None
        # Conditional: ConnectException/refused only infra if NO user-code frame
        if self._is_infra_connectivity_only(combined):
            return None

        # Check if it's a code issue
        issue_type = IssueType.OTHER
        exception_type = ""

        for pattern, itype in CODE_ISSUE_PATTERNS:
            match = re.search(pattern, combined, re.IGNORECASE)
            if match:
                issue_type = itype
                exception_type = match.group(0)
                break

        # Only process code issues
        if issue_type == IssueType.OTHER and not exception_type:
            # Check for generic exception patterns
            exc_match = re.search(r"\b(\w+(?:Exception|Error))\b", combined)
            if exc_match:
                exception_type = exc_match.group(1)
            else:
                return None

        # Extract file/line, preferring service-owned frames over shared libs
        stack_trace = root_cause if root_cause else ""
        file_path, line_number, class_name, method_name = self._extract_location_info(
            combined, service_name=service_name
        )
        signature = self._generate_fingerprint(
            service_name, namespace, exception_type, combined, stack_trace
        )

        if self._is_generic_exception_label(exception_type):
            evidence = f"{combined}\n{stack_trace}"
            if not self._has_strong_code_signal(evidence) and not file_path and not line_number:
                return None

        return CodeIssue(
            service_name=service_name,
            namespace=namespace,
            error_signature=signature,
            exception_type=exception_type,
            exception_message=message[:500] if message else issue_text[:500],
            stack_trace=stack_trace[:2000],
            log_evidence=combined[:1000],
            file_path=file_path,
            line_number=line_number,
            issue_type=issue_type,
            confidence=0.8 if exception_type else 0.5,
            status=IssueStatus.PENDING,
        )

    def _parse_deep_inspection(
        self, deep: Dict[str, Any], service_name: str, namespace: str
    ) -> Optional[CodeIssue]:
        """Parse deep inspection into CodeIssue."""
        if not self._is_allowed_service(service_name):
            return None
        exact_issue = str(deep.get("exact_issue", "") or "")
        root_cause = str(deep.get("root_cause", "") or "")
        fix_command = str(deep.get("fix_command", "") or "")

        if not exact_issue and not root_cause:
            return None

        combined = f"{exact_issue} {root_cause}"

        # Hard infra exclusions (always skip)
        for pattern in EXCLUDE_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return None
        # Conditional: ConnectException/refused only infra if NO user-code frame
        if self._is_infra_connectivity_only(combined):
            return None

        # Check if it's a code issue
        issue_type = IssueType.OTHER
        exception_type = ""

        for pattern, itype in CODE_ISSUE_PATTERNS:
            match = re.search(pattern, combined, re.IGNORECASE)
            if match:
                issue_type = itype
                exception_type = match.group(0)
                break

        if issue_type == IssueType.OTHER:
            exc_match = re.search(r"\b(\w+(?:Exception|Error))\b", combined)
            if exc_match:
                exception_type = exc_match.group(1)
            else:
                return None

        # Extract file/line, preferring service-owned frames
        file_path, line_number, _, _ = self._extract_location_info(combined, service_name=service_name)

        # Generate stable fingerprint for deduplication
        signature = self._generate_fingerprint(
            service_name, namespace, exception_type, combined, root_cause
        )

        return CodeIssue(
            service_name=service_name,
            namespace=namespace,
            error_signature=signature,
            exception_type=exception_type,
            exception_message=exact_issue[:500],
            stack_trace=root_cause[:2000],
            log_evidence=fix_command[:500],
            file_path=file_path,
            line_number=line_number,
            issue_type=issue_type,
            confidence=float(deep.get("confidence_score", 0.7) or 0.7),
            status=IssueStatus.PENDING,
        )

    async def close(self):
        """Close HTTP session."""
        session = getattr(self, "_session", None)
        if session is not None and not session.closed:
            await session.close()
