"""CodeXA configuration management."""

import os
from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class LLMModel:
    """LLM model info."""
    id: str
    name: str
    description: str
    size: str
    vram_required: str
    speed: str  # fast, medium, slow
    quality: str  # excellent, good, moderate
    best_for: str


# Available LLMs - Qwen (default/local) + Gemini (cloud)
AVAILABLE_MODELS: List[LLMModel] = [
    # DEFAULT - Small model for CPU inference
    LLMModel(
        id="ollama:qwen2.5:1.5b",
        name="Qwen 2.5 1.5B (Default)",
        description="Fast CPU model - works without GPU",
        size="1.5B",
        vram_required="2GB",
        speed="fast",
        quality="good",
        best_for="Quick analysis, CPU-only environments"
    ),
    # Larger local model (needs GPU)
    LLMModel(
        id="ollama:qwen2.5:7b-instruct-q4_K_M",
        name="Qwen 2.5 7B",
        description="Larger model - needs GPU for speed",
        size="7B",
        vram_required="8GB",
        speed="slow on CPU",
        quality="excellent",
        best_for="Complex analysis with GPU"
    ),
    # Gemini Cloud Models (use -latest suffix for stable access)
    LLMModel(
        id="gemini:gemini-1.5-pro-latest",
        name="Gemini 1.5 Pro",
        description="Google's most capable model - 1M token context",
        size="Cloud",
        vram_required="API",
        speed="fast",
        quality="excellent",
        best_for="Complex multi-file analysis, large codebases"
    ),
    LLMModel(
        id="gemini:gemini-1.5-flash-latest",
        name="Gemini 1.5 Flash",
        description="Fast and efficient for quick tasks",
        size="Cloud",
        vram_required="API",
        speed="very fast",
        quality="good",
        best_for="Quick fixes, simple bugs"
    ),
    LLMModel(
        id="gemini:gemini-2.0-flash",
        name="Gemini 2.0 Flash",
        description="Latest Gemini model - fast and capable",
        size="Cloud",
        vram_required="API",
        speed="very fast",
        quality="excellent",
        best_for="General code fixes, fast iteration"
    ),
]

# Keep for backwards compatibility
CLOUD_MODELS = [m for m in AVAILABLE_MODELS if m.id.startswith("gemini:")]


def get_available_models() -> List[Dict[str, str]]:
    """Get list of available models for UI."""
    return [
        {
            "id": m.id,
            "name": m.name,
            "description": m.description,
            "size": m.size,
            "vram": m.vram_required,
            "speed": m.speed,
            "quality": m.quality,
            "best_for": m.best_for,
        }
        for m in AVAILABLE_MODELS
    ]


@dataclass
class LLMConfig:
    """LLM configuration.

    Use get_available_models() to see all supported models.
    """
    provider: str = "ollama"  # ollama, gemini, openai, anthropic
    model: str = "qwen2.5:1.5b"  # Smaller model - faster on CPU
    host: str = "http://127.0.0.1:11434"
    api_key: str = ""  # API key for cloud providers (Gemini, OpenAI, etc.)
    timeout_seconds: int = 300  # 5 min timeout for CPU inference
    max_tokens: int = 4096  # Reduced for smaller model
    temperature: float = 0.05  # Very low for deterministic code
    top_p: float = 0.95
    # Fallback model if primary fails
    fallback_model: str = "qwen2.5:1.5b"
    num_ctx: int = 16384  # context window; 8192 is too small for multi-file Java + long stack traces


@dataclass
class GitHubConfig:
    """GitHub configuration."""
    token: str = ""
    org: str = "fabhotelstech"
    default_branch: str = "azure_migration"
    pr_labels: List[str] = field(default_factory=lambda: ["ai-generated", "codexa-fix"])


@dataclass
class MonitoringAgentConfig:
    """AI Monitoring Agent configuration."""
    url: str = "http://localhost:5000"
    api_prefix: str = "/ai-agent"
    poll_interval_seconds: int = 60


@dataclass
class AnalysisConfig:
    """Code analysis configuration."""
    max_context_lines: int = 100
    max_files_per_issue: int = 2  # Reduced for faster CPU inference
    verification_timeout: int = 300
    workspace: str = "/tmp/codexa-workspace"
    # Generate a fix when the model reports can_fix=true OR confidence is at
    # least this. Small models often set can_fix=false despite high confidence,
    # so we trust the confidence score (fixes are human-reviewed in the PR).
    min_fix_confidence: float = 0.6


@dataclass
class CodeXAConfig:
    """Main CodeXA configuration."""
    llm: LLMConfig = field(default_factory=LLMConfig)
    github: GitHubConfig = field(default_factory=GitHubConfig)
    monitoring_agent: MonitoringAgentConfig = field(default_factory=MonitoringAgentConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    redis_url: str = ""
    debug: bool = False


def load_config() -> CodeXAConfig:
    """Load configuration from environment variables."""
    config = CodeXAConfig()

    # LLM config
    config.llm.provider = os.getenv("CODEXA_LLM_PROVIDER", "ollama")
    config.llm.model = os.getenv("CODEXA_LLM_MODEL", os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b"))
    config.llm.host = os.getenv("CODEXA_LLM_HOST", os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434"))
    config.llm.api_key = os.getenv("GEMINI_API_KEY", os.getenv("OPENAI_API_KEY", ""))
    config.llm.timeout_seconds = int(os.getenv("CODEXA_LLM_TIMEOUT", "300"))
    config.llm.max_tokens = int(os.getenv("CODEXA_LLM_MAX_TOKENS", "4096"))
    config.llm.temperature = float(os.getenv("CODEXA_LLM_TEMPERATURE", "0.1"))
    config.llm.num_ctx = int(os.getenv("CODEXA_LLM_NUM_CTX", "16384"))

    # GitHub config
    config.github.token = os.getenv("GITHUB_TOKEN", os.getenv("GH_TOKEN", ""))
    config.github.org = os.getenv("CODEXA_GITHUB_ORG", "fabhotelstech")
    config.github.default_branch = os.getenv("CODEXA_GITHUB_BRANCH", "azure_migration")

    # Monitoring agent config
    config.monitoring_agent.url = os.getenv("CODEXA_AGENT_URL", "http://localhost:5000")
    config.monitoring_agent.api_prefix = os.getenv("CODEXA_AGENT_PREFIX", "/ai-agent")
    config.monitoring_agent.poll_interval_seconds = int(os.getenv("CODEXA_POLL_INTERVAL", "60"))

    # Analysis config
    config.analysis.max_context_lines = int(os.getenv("CODEXA_MAX_CONTEXT_LINES", "100"))
    config.analysis.max_files_per_issue = int(os.getenv("CODEXA_MAX_FILES", "2"))
    # Build verification is off by default: in a CPU pod the build tools are
    # usually absent and `./mvnw` would try to download the world. Set
    # CODEXA_VERIFY_TIMEOUT to a positive value to re-enable it.
    config.analysis.verification_timeout = int(os.getenv("CODEXA_VERIFY_TIMEOUT", "0"))
    config.analysis.workspace = os.getenv("CODEXA_WORKSPACE", "/tmp/codexa-workspace")
    config.analysis.min_fix_confidence = float(os.getenv("CODEXA_MIN_CONFIDENCE", "0.6"))

    # Redis
    config.redis_url = os.getenv("REDIS_URL", "")

    # Debug
    config.debug = os.getenv("CODEXA_DEBUG", "").lower() in ("1", "true", "yes")

    return config


# Global config instance
_config: Optional[CodeXAConfig] = None


def get_config() -> CodeXAConfig:
    """Get global configuration instance."""
    global _config
    if _config is None:
        _config = load_config()
    return _config
