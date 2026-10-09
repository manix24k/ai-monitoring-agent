#!/usr/bin/env python3
"""Simple LLM client with OpenAI-compatible API and optional Ollama fallback."""

import os
from typing import Optional

import requests

try:
    import ollama
except Exception:  # pragma: no cover
    ollama = None


class LLMClient:
    def __init__(self):
        self.api_url = os.getenv("AI_API_URL", "http://20.244.11.93/v1").rstrip("/")
        self.api_key = os.getenv("AI_API_KEY", "").strip()
        self.model = os.getenv("AI_MODEL_NAME", "qwen3").strip() or "qwen3"
        self.timeout = int(os.getenv("AI_API_TIMEOUT", "20") or "20")
        ollama_enabled_env = os.getenv("OLLAMA_ENABLED", "").strip().lower()
        self.ollama_model = os.getenv("OLLAMA_MODEL", "").strip()
        self.ollama_host = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434").strip().rstrip("/")
        self.ollama_enabled = (
            ollama_enabled_env in {"1", "true", "yes"} or
            (ollama_enabled_env == "" and bool(self.ollama_model))
        )
        self.last_error = ""

    def is_remote_available(self) -> bool:
        return bool(self.api_url and self.api_key and self.model)

    def generate(self, prompt: str, temperature: float = 0.4) -> str:
        """Generate response text from configured LLM backend."""
        self.last_error = ""
        remote_text = self._generate_remote(prompt, temperature)
        if remote_text:
            return remote_text

        ollama_text = self._generate_ollama(prompt, temperature)
        return ollama_text

    def _generate_remote(self, prompt: str, temperature: float) -> str:
        if not self.is_remote_available():
            self.last_error = "remote llm not configured"
            return ""
        try:
            response = requests.post(
                f"{self.api_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": "You are a concise SRE assistant."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": temperature
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            payload = response.json()
            choices = payload.get("choices") or []
            if not choices:
                return ""
            message = choices[0].get("message") if isinstance(choices[0], dict) else {}
            return str((message or {}).get("content", "") or "").strip()
        except Exception as exc:
            self.last_error = f"remote llm request failed: {exc}"
            return ""

    def _generate_ollama(self, prompt: str, temperature: float) -> str:
        if not self.ollama_enabled or not self.ollama_model:
            return ""

        # Prefer native SDK when available.
        if ollama is not None:
            try:
                response = ollama.generate(
                    model=self.ollama_model,
                    prompt=prompt,
                    stream=False,
                    options={
                        "temperature": temperature,
                        "top_p": 0.9
                    }
                )
                if isinstance(response, dict):
                    text = str(response.get("response", "") or "").strip()
                    if text:
                        return text
            except Exception as exc:
                self.last_error = f"ollama sdk failed: {exc}"

        # HTTP fallback works even without python ollama package.
        try:
            response = requests.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "top_p": 0.9
                    }
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            payload = response.json() if hasattr(response, "json") else {}
            if not isinstance(payload, dict):
                return ""
            return str(payload.get("response", "") or "").strip()
        except Exception as exc:
            self.last_error = f"ollama fallback failed: {exc}"
            return ""
