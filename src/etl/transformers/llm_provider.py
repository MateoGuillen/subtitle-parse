"""Abstract LLM provider and implementations (local + OpenRouter)."""

import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import requests


class LLMProvider(ABC):
    """Abstract base for LLM providers."""

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Short model identifier for provenance tracking."""
        ...

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

    def generate_batch_messages(
        self,
        all_messages: List[List[Dict[str, str]]],
        max_tokens: int = 512,
        temperature: float = 0.0,
    ) -> List[str]:
        """Default: sequential loop over generate(). Subclasses may override for batching."""
        return [
            self.generate(msgs, max_tokens=max_tokens, temperature=temperature)
            for msgs in all_messages
        ]


class LocalLLMProvider(LLMProvider):
    """Provider for local LM Studio / llama-server endpoint."""

    def __init__(self, base_url: str = "http://localhost:1234/v1", model: str = ""):
        self.base_url = base_url.rstrip("/")
        self.model = model

    @property
    def model_name(self) -> str:
        return self.model or "local"

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

    def generate_batch_messages(
        self,
        all_messages: List[List[Dict[str, str]]],
        max_tokens: int = 512,
        temperature: float = 0.0,
    ) -> List[str]:
        """Batch via thread pool (preserves input order)."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        n = len(all_messages)
        results = [None] * n
        with ThreadPoolExecutor(max_workers=min(8, n)) as ex:
            fut_map = {
                ex.submit(self.generate, msgs, max_tokens=max_tokens, temperature=temperature): i
                for i, msgs in enumerate(all_messages)
            }
            for fut in as_completed(fut_map):
                idx = fut_map[fut]
                results[idx] = fut.result()
        return results


class OpenRouterProvider(LLMProvider):
    """Provider for OpenRouter API."""

    BASE_URL = "https://openrouter.ai/api/v1"

    def __init__(self, api_key: str, model: str = "openai/gpt-oss-20b:free"):
        self.api_key = api_key
        self.model = model

    @property
    def model_name(self) -> str:
        return self.model

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


class TokengoProvider(LLMProvider):
    """Provider for Tokengo API (OpenAI-compatible at https://api.tokengo.com/v1).
    Supports both GLM-5.1 and DeepSeek V3.2 models.
    """

    BASE_URL = "https://api.tokengo.com/v1"

    def __init__(self, api_key: str, model: str = "z-ai/glm-5.1",
                 reasoning_enabled: bool = False, base_url: Optional[str] = None):
        self.api_key = api_key
        self.model = model
        self.reasoning_enabled = reasoning_enabled
        self.base_url = (base_url or self.BASE_URL).rstrip("/")

    @property
    def model_name(self) -> str:
        return self.model

    def name(self) -> str:
        tag = "reasoning" if self.reasoning_enabled else "normal"
        return f"tokengo ({self.model}, {tag})"

    def generate(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Dict[str, Any]] = None,
        max_tokens: int = 4096,
        temperature: float = 0.0,
    ) -> str:
        url = f"{self.base_url}/chat/completions"
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
        if self.reasoning_enabled:
            payload["reasoning_enabled"] = True

        last_exception: Optional[str] = None
        for attempt in range(3):
            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=180)
                if resp.status_code == 200:
                    data = resp.json()
                    choices = data.get("choices", [])
                    if choices:
                        return choices[0].get("message", {}).get("content", "")
                    return json.dumps(data)
                else:
                    last_exception = f"HTTP {resp.status_code}: {resp.text[:300]}"
                    if attempt < 2:
                        time.sleep(2 ** attempt)
            except requests.exceptions.RequestException as e:
                last_exception = str(e)
                if attempt < 2:
                    time.sleep(2 ** attempt)

        if last_exception:
            return json.dumps({
                "schema": {},
                "justification": f"LLM call failed after 3 retries: {last_exception}",
            })
        return json.dumps({
            "schema": {},
            "justification": "LLM call failed after 3 retries.",
        })


class VLLMOfflineProvider(LLMProvider):
    """Provider for vLLM offline API (local GPU, direct Python API).

    Uses ``vllm.LLM.generate()`` with pre-rendered prompts.
    Handles lazy model loading and continuous batching.
    """

    def __init__(
        self,
        model_path: str,
        tokenizer_path: Optional[str] = None,
        quantization: str = "awq",
        max_model_len: int = 8192,
        max_num_seqs: int = 32,
        gpu_memory_utilization: float = 0.85,
        enforce_eager: bool = True,
        trust_remote_code: bool = True,
        dtype: str = "auto",
        guided_regex: Optional[str] = None,
    ):
        self.model_path = model_path
        self.tokenizer_path = tokenizer_path or model_path
        self.quantization = quantization
        self.max_model_len = max_model_len
        self.max_num_seqs = max_num_seqs
        self.gpu_memory_utilization = gpu_memory_utilization
        self.enforce_eager = enforce_eager
        self.trust_remote_code = trust_remote_code
        self.dtype = dtype
        self.guided_regex = guided_regex
        self._model = None
        self._tokenizer = None

    def _sampling_params(self, max_tokens: int, temperature: float):
        from vllm import SamplingParams
        kw: Dict[str, Any] = dict(
            temperature=temperature, max_tokens=max_tokens,
            stop=["<|im_end|>", "<|end|>", "</s>"],
        )
        if self.guided_regex:
            from vllm.sampling_params import StructuredOutputsParams
            kw["structured_outputs"] = StructuredOutputsParams(
                regex=self.guided_regex)
        return SamplingParams(**kw)

    def _lazy_init(self):
        if self._model is not None:
            return
        from transformers import AutoTokenizer
        self._tokenizer = AutoTokenizer.from_pretrained(
            self.tokenizer_path, trust_remote_code=self.trust_remote_code
        )
        from vllm import LLM
        import os
        if "qwen" in self.model_path.lower():
            os.environ.setdefault("VLLM_USE_V2_MODEL_RUNNER", "0")
        self._model = LLM(
            model=self.model_path,
            tokenizer=self.tokenizer_path,
            quantization=self.quantization if self.quantization != "none" else None,
            max_model_len=self.max_model_len,
            max_num_seqs=self.max_num_seqs,
            gpu_memory_utilization=self.gpu_memory_utilization,
            enforce_eager=self.enforce_eager,
            trust_remote_code=self.trust_remote_code,
            dtype=self.dtype,
        )

    @property
    def model_name(self) -> str:
        return self.model_path

    def name(self) -> str:
        return f"vllm-offline ({self.model_path}, {self.quantization})"

    def generate(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Dict[str, Any]] = None,
        max_tokens: int = 512,
        temperature: float = 0.0,
    ) -> str:
        self._lazy_init()
        kwargs = {"tokenize": False, "add_generation_prompt": True}
        if "qwen" in self.model_path.lower():
            kwargs["enable_thinking"] = False
        rendered = self._tokenizer.apply_chat_template(messages, **kwargs)
        sampling_params = self._sampling_params(max_tokens, temperature)
        outputs = self._model.generate([rendered], sampling_params)
        return outputs[0].outputs[0].text.strip()

    def generate_batch(
        self,
        prompts: List[str],
        max_tokens: int = 512,
        temperature: float = 0.0,
    ) -> List[str]:
        """Batch generate for pre-rendered prompt strings.

        Uses vLLM's internal continuous batching. All prompts in
        one call -> processed in parallel on GPU.
        """
        self._lazy_init()
        sampling_params = self._sampling_params(max_tokens, temperature)
        outputs = self._model.generate(prompts, sampling_params)
        return [out.outputs[0].text.strip() for out in outputs]

    def generate_batch_messages(
        self,
        all_messages: List[List[Dict[str, str]]],
        max_tokens: int = 512,
        temperature: float = 0.0,
    ) -> List[str]:
        """Batch generate for message lists (handles rendering internally)."""
        self._lazy_init()
        rendered = []
        for messages in all_messages:
            kwargs = {"tokenize": False, "add_generation_prompt": True}
            if "qwen" in self.model_path.lower():
                kwargs["enable_thinking"] = False
            rendered.append(self._tokenizer.apply_chat_template(messages, **kwargs))
        return self.generate_batch(rendered, max_tokens, temperature)
