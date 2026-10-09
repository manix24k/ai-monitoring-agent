"""LLM provider - handles communication with LLM backends."""

import asyncio
import json
import logging
import re
import time
from collections import deque
from typing import Any, Dict, Optional

import aiohttp

from ..config import LLMConfig

logger = logging.getLogger("codexa.llm")


class RateLimiter:
    """Rate limiter for API calls with sliding window."""

    def __init__(self, max_requests_per_minute: int = 10, min_delay_seconds: float = 5.0):
        """
        Initialize rate limiter.

        Args:
            max_requests_per_minute: Maximum requests allowed per minute (default 10, well under 15 RPM limit)
            min_delay_seconds: Minimum delay between requests (default 5s)
        """
        self.max_rpm = max_requests_per_minute
        self.min_delay = min_delay_seconds
        self._request_times: deque = deque(maxlen=max_requests_per_minute)
        self._last_request_time: float = 0
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Wait until it's safe to make a request."""
        async with self._lock:
            now = time.time()

            # Enforce minimum delay between requests
            time_since_last = now - self._last_request_time
            if time_since_last < self.min_delay:
                wait_time = self.min_delay - time_since_last
                logger.debug(f"Rate limiter: waiting {wait_time:.1f}s (min delay)")
                await asyncio.sleep(wait_time)
                now = time.time()

            # Remove requests older than 60 seconds
            cutoff = now - 60
            while self._request_times and self._request_times[0] < cutoff:
                self._request_times.popleft()

            # If at capacity, wait until oldest request expires
            if len(self._request_times) >= self.max_rpm:
                oldest = self._request_times[0]
                wait_time = oldest + 60 - now + 1  # +1s buffer
                if wait_time > 0:
                    logger.info(f"Rate limiter: at capacity ({self.max_rpm} RPM), waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                    now = time.time()
                    # Clean up again after waiting
                    cutoff = now - 60
                    while self._request_times and self._request_times[0] < cutoff:
                        self._request_times.popleft()

            # Record this request
            self._request_times.append(now)
            self._last_request_time = now
            logger.debug(f"Rate limiter: request allowed ({len(self._request_times)}/{self.max_rpm} in window)")


# Global rate limiter for Gemini API
_gemini_rate_limiter = RateLimiter(max_requests_per_minute=10, min_delay_seconds=5.0)


class LLMProvider:
    """Provider for LLM inference."""

    def __init__(self, config: LLMConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._metrics = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "total_time": 0.0,
            "total_tokens": 0,
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        # Create a fresh session for each request to avoid event loop issues
        # when called from different contexts (Flask background tasks, etc.)
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        return aiohttp.ClientSession(timeout=timeout)

    def _get_provider_and_model(self) -> tuple:
        """Extract provider and model from config.

        Model ID format: "provider:model" (e.g., "gemini:gemini-1.5-pro", "ollama:qwen2.5:7b")
        """
        model = self.config.model
        provider = self.config.provider.lower()

        # Check if model ID contains provider prefix
        if ":" in model:
            parts = model.split(":", 1)
            if parts[0] in ("gemini", "ollama", "openai", "anthropic"):
                provider = parts[0]
                model = parts[1]

        return provider, model

    async def generate(self, prompt: str, system: Optional[str] = None) -> str:
        """Generate response from LLM."""
        provider, model = self._get_provider_and_model()

        # Temporarily set the model for this request
        original_model = self.config.model
        self.config.model = model

        try:
            if provider == "ollama":
                return await self._ollama_generate(prompt, system)
            elif provider == "gemini":
                return await self._gemini_generate(prompt, system)
            elif provider == "openai":
                return await self._openai_generate(prompt, system)
            elif provider == "anthropic":
                return await self._anthropic_generate(prompt, system)
            else:
                # Default to Ollama
                return await self._ollama_generate(prompt, system)
        finally:
            self.config.model = original_model

    async def _ollama_generate(self, prompt: str, system: Optional[str] = None) -> str:
        """Generate using Ollama API."""
        url = f"{self.config.host}/api/generate"

        # num_ctx must be large enough to hold the prompt, otherwise Ollama
        # silently truncates it (the model then "sees" only part of the code).
        num_ctx = int(getattr(self.config, "num_ctx", 0) or 16384)

        payload = {
            "model": self.config.model,
            "prompt": prompt,
            "stream": False,
            # Force valid JSON output. qwen2.5 honours this and stops as soon
            # as the JSON object is complete instead of rambling to num_predict.
            "format": "json",
            # Keep the model resident between the analyze + fix calls so a slow
            # CPU box does not reload the weights from disk every request.
            "keep_alive": "30m",
            "options": {
                "temperature": self.config.temperature,
                "top_p": self.config.top_p,
                "num_ctx": num_ctx,
                "num_predict": self.config.max_tokens,
            },
        }

        if system:
            payload["system"] = system

        start_time = time.time()
        self._metrics["total_calls"] += 1

        # Use context manager to ensure session is closed
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(url, json=payload) as response:
                    elapsed = time.time() - start_time
                    self._metrics["total_time"] += elapsed

                    if response.status == 200:
                        data = await response.json()
                        result = data.get("response", "")
                        self._metrics["successful_calls"] += 1

                        # Track tokens if available
                        if "eval_count" in data:
                            self._metrics["total_tokens"] += data["eval_count"]

                        logger.info(f"LLM response received in {elapsed:.2f}s")
                        return result
                    else:
                        error = await response.text()
                        self._metrics["failed_calls"] += 1
                        logger.error(f"Ollama error: {response.status} - {error}")

                        # Try fallback model
                        if self.config.fallback_model and self.config.model != self.config.fallback_model:
                            logger.info(f"Trying fallback model: {self.config.fallback_model}")
                            payload["model"] = self.config.fallback_model
                            async with session.post(url, json=payload) as fallback_resp:
                                if fallback_resp.status == 200:
                                    data = await fallback_resp.json()
                                    return data.get("response", "")

                        raise Exception(f"LLM request failed: {error}")

            except asyncio.TimeoutError:
                self._metrics["failed_calls"] += 1
                logger.error(f"LLM request timed out after {self.config.timeout_seconds}s")
                raise
            except Exception as e:
                self._metrics["failed_calls"] += 1
                logger.error(f"LLM error: {e}")
                raise

    def _parse_retry_delay(self, error_text: str) -> float:
        """Parse retry delay from Gemini 429 error response."""
        try:
            # Try to parse JSON error
            error_data = json.loads(error_text)
            details = error_data.get("error", {}).get("details", [])
            for detail in details:
                if detail.get("@type", "").endswith("RetryInfo"):
                    retry_delay = detail.get("retryDelay", "")
                    # Parse "32s" or "32.5s" format
                    if retry_delay:
                        match = re.match(r"([\d.]+)s?", retry_delay)
                        if match:
                            return float(match.group(1))
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

        # Also try to find "retry in X seconds" in message
        match = re.search(r"retry in ([\d.]+)", error_text, re.IGNORECASE)
        if match:
            return float(match.group(1))

        # Default retry delay
        return 60.0

    async def _gemini_generate(self, prompt: str, system: Optional[str] = None) -> str:
        """Generate using Google Gemini API with rate limiting and retry."""
        # Check API key
        if not self.config.api_key:
            raise Exception("GEMINI_API_KEY not set. Please set the environment variable.")

        # Extract model name (e.g., "gemini:gemini-1.5-pro" -> "gemini-1.5-pro")
        model = self.config.model
        if ":" in model:
            model = model.split(":", 1)[1]

        # Map old model names to current ones
        model_mapping = {
            "gemini-pro": "gemini-1.5-flash",
            "gemini-1.5-pro": "gemini-1.5-pro-latest",
            "gemini-1.5-flash": "gemini-1.5-flash-latest",
        }
        model = model_mapping.get(model, model)

        # Use v1 API (more stable than v1beta)
        url = f"https://generativelanguage.googleapis.com/v1/models/{model}:generateContent"

        # Build the prompt with system context
        full_prompt = prompt
        if system:
            full_prompt = f"{system}\n\n{prompt}"

        payload = {
            "contents": [
                {
                    "parts": [{"text": full_prompt}]
                }
            ],
            "generationConfig": {
                "temperature": self.config.temperature,
                "topP": self.config.top_p,
                "maxOutputTokens": self.config.max_tokens,
            }
        }

        headers = {
            "Content-Type": "application/json",
        }

        # Add API key as query parameter
        url_with_key = f"{url}?key={self.config.api_key}"

        # Retry configuration
        max_retries = 5
        base_delay = 10.0  # Base delay for exponential backoff

        for attempt in range(max_retries):
            # Apply rate limiting before each request
            await _gemini_rate_limiter.acquire()

            start_time = time.time()
            self._metrics["total_calls"] += 1

            # Use context manager to ensure session is closed
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url_with_key, json=payload, headers=headers) as response:
                        elapsed = time.time() - start_time
                        self._metrics["total_time"] += elapsed

                        if response.status == 200:
                            data = await response.json()
                            self._metrics["successful_calls"] += 1

                            # Extract text from Gemini response
                            candidates = data.get("candidates", [])
                            if candidates:
                                content = candidates[0].get("content", {})
                                parts = content.get("parts", [])
                                if parts:
                                    result = parts[0].get("text", "")
                                    logger.info(f"Gemini response received in {elapsed:.2f}s (attempt {attempt + 1})")
                                    return result
                            return ""

                        elif response.status == 429:
                            # Rate limited - parse retry delay and wait
                            error_text = await response.text()
                            retry_delay = self._parse_retry_delay(error_text)

                            # Add exponential backoff on top of suggested delay
                            backoff = min(base_delay * (2 ** attempt), 120)  # Cap at 2 minutes
                            total_wait = max(retry_delay, backoff) + 5  # +5s buffer

                            if attempt < max_retries - 1:
                                logger.warning(
                                    f"Gemini rate limited (429). Waiting {total_wait:.0f}s before retry "
                                    f"(attempt {attempt + 1}/{max_retries})"
                                )
                                await asyncio.sleep(total_wait)
                                continue
                            else:
                                self._metrics["failed_calls"] += 1
                                logger.error(f"Gemini rate limit exceeded after {max_retries} retries")
                                raise Exception(f"Gemini rate limit exceeded after {max_retries} retries: {error_text}")

                        elif response.status >= 500:
                            # Server error - retry with backoff
                            error_text = await response.text()
                            if attempt < max_retries - 1:
                                backoff = base_delay * (2 ** attempt)
                                logger.warning(
                                    f"Gemini server error ({response.status}). Retrying in {backoff:.0f}s "
                                    f"(attempt {attempt + 1}/{max_retries})"
                                )
                                await asyncio.sleep(backoff)
                                continue
                            else:
                                self._metrics["failed_calls"] += 1
                                raise Exception(f"Gemini server error after {max_retries} retries: {error_text}")

                        else:
                            error = await response.text()
                            self._metrics["failed_calls"] += 1
                            logger.error(f"Gemini error: {response.status} - {error}")
                            raise Exception(f"Gemini request failed: {error}")

            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    backoff = base_delay * (2 ** attempt)
                    logger.warning(
                        f"Gemini request timed out. Retrying in {backoff:.0f}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(backoff)
                    continue
                else:
                    self._metrics["failed_calls"] += 1
                    logger.error(f"Gemini request timed out after {max_retries} retries")
                    raise

            except aiohttp.ClientError as e:
                if attempt < max_retries - 1:
                    backoff = base_delay * (2 ** attempt)
                    logger.warning(
                        f"Gemini connection error: {e}. Retrying in {backoff:.0f}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(backoff)
                    continue
                else:
                    self._metrics["failed_calls"] += 1
                    logger.error(f"Gemini connection error after {max_retries} retries: {e}")
                    raise

        # Should not reach here
        self._metrics["failed_calls"] += 1
        raise Exception("Gemini request failed: max retries exceeded")

    async def _openai_generate(self, prompt: str, system: Optional[str] = None) -> str:
        """Generate using OpenAI-compatible API."""
        url = f"{self.config.host}/v1/chat/completions"

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self.config.model,
            "messages": messages,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
        }

        headers = {"Content-Type": "application/json"}
        api_key = getattr(self.config, "api_key", "") or ""
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["api-key"] = api_key

        start_time = time.time()
        self._metrics["total_calls"] += 1

        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload, headers=headers) as response:
                    elapsed = time.time() - start_time
                    self._metrics["total_time"] += elapsed

                    if response.status == 200:
                        data = await response.json()
                        self._metrics["successful_calls"] += 1
                        usage = data.get("usage", {}) if isinstance(data, dict) else {}
                        if isinstance(usage, dict):
                            tokens = int(
                                usage.get("total_tokens")
                                or usage.get("completion_tokens")
                                or usage.get("output_tokens")
                                or 0
                            )
                            if tokens > 0:
                                self._metrics["total_tokens"] += tokens
                        choices = data.get("choices", [])
                        if choices:
                            return choices[0].get("message", {}).get("content", "")
                        return ""
                    else:
                        error = await response.text()
                        # Some Azure deployments (including codex-style ones) do not
                        # support chat/completions and require the Responses API.
                        if "unsupported" in str(error).lower() and "operation" in str(error).lower():
                            return await self._openai_generate_via_responses(prompt, system, headers)
                        self._metrics["failed_calls"] += 1
                        raise Exception(f"OpenAI request failed: {error}")

        except Exception as e:
            self._metrics["failed_calls"] += 1
            logger.error(f"OpenAI error: {e}")
            raise

    async def _openai_generate_via_responses(self, prompt: str, system: Optional[str], headers: Dict[str, str]) -> str:
        """Fallback to OpenAI/Azure Responses API when chat/completions is unsupported."""
        url = f"{self.config.host}/v1/responses"

        joined_input = prompt if not system else f"{system}\n\n{prompt}"
        payload = {
            "model": self.config.model,
            "input": joined_input,
            "temperature": self.config.temperature,
            "max_output_tokens": self.config.max_tokens,
        }

        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    error = await response.text()
                    self._metrics["failed_calls"] += 1
                    raise Exception(f"OpenAI Responses request failed: {error}")

                data = await response.json()
                usage = data.get("usage", {}) if isinstance(data, dict) else {}
                if isinstance(usage, dict):
                    tokens = int(
                        usage.get("total_tokens")
                        or usage.get("output_tokens")
                        or usage.get("completion_tokens")
                        or 0
                    )
                    if tokens > 0:
                        self._metrics["total_tokens"] += tokens
                # OpenAI Responses API usually exposes output_text directly.
                text = data.get("output_text")
                if isinstance(text, str) and text.strip():
                    self._metrics["successful_calls"] += 1
                    return text

                # Fallback parser for structured output blocks.
                out = data.get("output", [])
                if isinstance(out, list):
                    for item in out:
                        if not isinstance(item, dict):
                            continue
                        content = item.get("content", [])
                        if not isinstance(content, list):
                            continue
                        for block in content:
                            if not isinstance(block, dict):
                                continue
                            val = block.get("text")
                            if isinstance(val, str) and val.strip():
                                self._metrics["successful_calls"] += 1
                                return val

                self._metrics["successful_calls"] += 1
                return ""

    async def _anthropic_generate(self, prompt: str, system: Optional[str] = None) -> str:
        """Generate using Anthropic API."""
        session = await self._get_session()
        url = f"{self.config.host}/v1/messages"

        payload = {
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }

        if system:
            payload["system"] = system

        headers = {"Content-Type": "application/json"}

        start_time = time.time()
        self._metrics["total_calls"] += 1

        try:
            async with session.post(url, json=payload, headers=headers) as response:
                elapsed = time.time() - start_time
                self._metrics["total_time"] += elapsed

                if response.status == 200:
                    data = await response.json()
                    self._metrics["successful_calls"] += 1
                    content = data.get("content", [])
                    if content:
                        return content[0].get("text", "")
                    return ""
                else:
                    error = await response.text()
                    self._metrics["failed_calls"] += 1
                    raise Exception(f"Anthropic request failed: {error}")

        except Exception as e:
            self._metrics["failed_calls"] += 1
            logger.error(f"Anthropic error: {e}")
            raise

    async def test(self) -> str:
        """Test LLM connectivity with a simple prompt."""
        return await self.generate(
            "Reply with exactly: 'CodeXA LLM connection successful'",
            system="You are a test assistant. Follow instructions exactly."
        )

    def get_metrics(self) -> Dict[str, Any]:
        """Get LLM performance metrics."""
        total = self._metrics["total_calls"]
        successful = self._metrics["successful_calls"]

        return {
            "total_calls": total,
            "successful_calls": successful,
            "failed_calls": self._metrics["failed_calls"],
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "avg_response_time": (
                self._metrics["total_time"] / total if total > 0 else 0
            ),
            "total_tokens": self._metrics["total_tokens"],
            "avg_tokens_per_call": (
                self._metrics["total_tokens"] / successful if successful > 0 else 0
            ),
        }

    async def close(self):
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
