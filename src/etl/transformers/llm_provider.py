"""Abstract LLM provider and implementations (local + OpenRouter)."""

import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import requests


class LLMProvider(ABC):
    """Abstract base for LLM providers."""

    @abstractmethod
    def generate(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Dict[str, Any]] = None,
        max_tokens: int = 4096,
        temperature: float = 0.0,
    ) -> str:
        ...

    @abstractmethod
    def name(self) -> str:
        """Human-readable provider/model name for logging."""
        ...


class LocalLLMProvider(LLMProvider):
    """Provider for local LM Studio / llama-server endpoint."""

    def __init__(self, base_url: str = "http://localhost:1234/v1", model: str = ""):
        self.base_url = base_url.rstrip("/")
        self.model = model

    def name(self) -> str:
        tag = self.model if self.model else "default"
        return f"local ({tag} @ {self.base_url})"

    def generate(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Dict[str, Any]] = None,
        max_tokens: int = 4096,
        temperature: float = 0.0,
    ) -> str:
        url = f"{self.base_url}/chat/completions"
        payload: Dict[str, Any] = {
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if self.model:
            payload["model"] = self.model
        if response_format:
            payload["response_format"] = response_format

        last_exception: Optional[str] = None
        for attempt in range(3):
            try:
                resp = requests.post(url, json=payload, timeout=180)
                if resp.status_code == 200:
                    data = resp.json()
                    choices = data.get("choices", [])
                    if choices:
                        return choices[0].get("message", {}).get("content", "")
                    return json.dumps(data)
                else:
                    last_exception = f"HTTP {resp.status_code}: {resp.text[:300]}"
                    if attempt < 2:
                        time.sleep(2**attempt)
            except requests.exceptions.RequestException as e:
                last_exception = str(e)
                if attempt < 2:
                    time.sleep(2**attempt)

        if last_exception:
            return json.dumps({
                "schema": {},
                "justification": f"LLM call failed after 3 retries: {last_exception}",
            })
        return json.dumps({
            "schema": {},
            "justification": "LLM call failed after 3 retries.",
        })


class OpenRouterProvider(LLMProvider):
    """Provider for OpenRouter API."""

    BASE_URL = "https://openrouter.ai/api/v1"

    def __init__(self, api_key: str, model: str = "openai/gpt-oss-20b:free"):
        self.api_key = api_key
        self.model = model

    def name(self) -> str:
        return f"openrouter ({self.model})"

    def generate(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Dict[str, Any]] = None,
        max_tokens: int = 4096,
        temperature: float = 0.0,
    ) -> str:
        url = f"{self.BASE_URL}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if response_format:
            payload["response_format"] = response_format

        last_exception: Optional[str] = None
        for attempt in range(3):
            try:
                resp = requests.post(
                    url, headers=headers, json=payload, timeout=120
                )
                if resp.status_code == 200:
                    data = resp.json()
                    choices = data.get("choices", [])
                    if choices:
                        return choices[0].get("message", {}).get("content", "")
                    return json.dumps(data)
                else:
                    last_exception = f"HTTP {resp.status_code}: {resp.text[:300]}"
                    if attempt < 2:
                        time.sleep(2**attempt)
            except requests.exceptions.RequestException as e:
                last_exception = str(e)
                if attempt < 2:
                    time.sleep(2**attempt)

        if last_exception:
            return json.dumps({
                "schema": {},
                "justification": f"LLM call failed after 3 retries: {last_exception}",
            })
        return json.dumps({
            "schema": {},
            "justification": "LLM call failed after 3 retries.",
        })
