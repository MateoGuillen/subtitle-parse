"""
Client for calling LLM APIs with OpenAI-compatible endpoints.
"""

import base64
import json
from typing import Dict, Any, Optional
import time
import requests
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class LLMClient:
    """
    Client for calling LLM APIs.

    This class handles communication with LLM endpoints that follow OpenAI-compatible
    API schemas. It supports various providers like:
    - Local LM Studio
    - LLM Studio
    - OpenRouter
    - Any OpenAI-compatible endpoint
    """

    def __init__(
        self,
        endpoint_url: str,
        username: str = None,
        password: str = None,
        api_key: str = None,
        timeout_llm_response: int = 60,
    ):
        self.endpoint_url = endpoint_url.rstrip("/")
        self.username = username
        self.password = password
        self.api_key = api_key
        self.timeout_llm_response = timeout_llm_response
        self.logger = setup_logger(__name__)
        self._setup_session()

    def _setup_session(self):
        """Setup requests session with headers and configuration."""
        self.session = requests.Session()

        # Set default headers
        self.session.headers.update(
            {"Content-Type": "application/json", "User-Agent": "DNCP-LLM-Pipeline/1.0"}
        )

        # Priority order for authentication:
        # 1. HTTP Basic Auth with username/password
        # 2. Bearer token with API key
        # 3. Basic Auth with API key (legacy)

        if self.username and self.password:
            # HTTP Basic Authentication with username and password
            credentials = base64.b64encode(
                f"{self.username}:{self.password}".encode()
            ).decode()
            self.session.headers.update({"Authorization": f"Basic {credentials}"})
            self.logger.info("Using HTTP Basic Authentication with username/password")

        elif self.api_key:
            # Bearer token authentication
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
            self.logger.info("Using Bearer token authentication")

        # Set timeout
        self.session.timeout = self.timeout_llm_response

    @error_handling(default_return=None)
    def call_llm(
        self, payload: Dict[str, Any], max_retries: int = 3, retry_delay: float = 1.0
    ) -> Optional[Dict[str, Any]]:
        """
        Call LLM API with the given payload.

        Args:
            payload: Request payload dictionary
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds

        Returns:
            LLM response dictionary or None if failed
        """
        endpoint = f"{self.endpoint_url}/v1/chat/completions"

        for attempt in range(max_retries + 1):
            try:

                self.logger.debug(
                    "LLamando LLM (intento %s) : %s",
                    {attempt + 1},
                    {payload.get("model", "unknown")},
                )

                start_time = time.time()
                response = self.session.post(
                    endpoint, json=payload, timeout=self.timeout_llm_response
                )
                end_time = time.time()

                # Log request time
                request_time = end_time - start_time
                self.logger.debug(f"LLM request completado en {request_time:.2f}s")

                # Handle different response status codes
                if response.status_code == 200:
                    return self._parse_response(response.json())

                elif response.status_code == 429:  # Rate limit
                    self.logger.warning(
                        "Rate limit alcanzado, reintentando en %s", retry_delay * 2
                    )
                    if attempt < max_retries:
                        time.sleep(retry_delay * 2)
                        retry_delay *= 2  # Exponential backoff
                        continue

                elif response.status_code >= 500:  # Server errors
                    self.logger.warning(
                        "Error del servidor (%s), reintentando...", response.status_code
                    )
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue

                else:
                    # Client errors or other status codes
                    self.logger.error(
                        "Error LLM API (%s): %s",
                        response.status_code,
                        response.text,
                    )
                    return None

            except requests.exceptions.Timeout:
                self.logger.warning("Timeout en llamada LLM (intento %s)", attempt + 1)
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue

            except requests.exceptions.ConnectionError:
                self.logger.warning("Error de conexión LLM (intento %s)", attempt + 1)
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue

            except Exception as e:
                self.logger.error("Error inesperado en llamada LLM: %s", str(e))
                return None

        self.logger.error("Todos los intentos fallaron para llamada LLM")
        return None

    def _parse_response(
        self, response_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Parse LLM API response and extract content.

        Args:
            response_data: Raw response from LLM API

        Returns:
            Parsed content or None if parsing failed
        """
        try:
            # Handle OpenAI-style response format
            if "choices" in response_data:
                if response_data["choices"] and len(response_data["choices"]) > 0:
                    choice = response_data["choices"][0]

                    # Extract message content
                    if "message" in choice and "content" in choice["message"]:
                        content = choice["message"]["content"]

                        # Try to parse as JSON if it looks like structured data
                        if content.strip().startswith(("{", "[")):
                            try:
                                return json.loads(content)
                            except json.JSONDecodeError:
                                # Return as text if JSON parsing fails
                                return {"response": content}
                        else:
                            return {"response": content}

            # Handle direct response format (some local endpoints)
            elif "response" in response_data:
                content = response_data["response"]

                # Try to parse as JSON
                if isinstance(content, str) and content.strip().startswith(("{", "[")):
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError:
                        return {"response": content}
                else:
                    return response_data

            # Handle other response formats
            else:
                self.logger.warning(
                    "Formato de respuesta desconocido: %s",
                    list(response_data.keys()),
                )
                return response_data

        except Exception as e:
            self.logger.error("Error parseando respuesta LLM: %s", str(e))
            return None

    @error_handling(default_return=False)
    def test_connection(self) -> bool:
        """
        Test connection to LLM endpoint.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Simple test payload
            test_payload = {
                "model": "test-model",
                "messages": [{"role": "user", "content": "Hello"}],
                "max_tokens": 10,
            }

            endpoint = f"{self.endpoint_url}/v1/chat/completions"
            response = self.session.post(endpoint, json=test_payload, timeout=10)

            # Consider connection successful if we get any response (even error)
            # This just tests connectivity, not functionality
            self.logger.info("Conexión LLM OK (status: %s)", response.status_code)
            return True

        except requests.exceptions.ConnectionError:
            self.logger.error("No se puede conectar al endpoint LLM")
            return False
        except Exception as e:
            self.logger.error("Error probando conexión LLM: %s", str(e))
            return False

    def get_available_models(self) -> Optional[list]:
        """
        Get list of available models from the endpoint.

        Returns:
            List of model names or None if not supported
        """
        try:
            models_endpoint = f"{self.endpoint_url}/v1/models"
            response = self.session.get(models_endpoint, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if "data" in data:
                    return [model["id"] for model in data["data"]]

        except Exception as e:
            self.logger.debug("No se pueden obtener modelos disponibles: %s", str(e))

        return None

    def estimate_cost(self, payload: Dict[str, Any]) -> Dict[str, float]:
        """
        Estimate cost for the request (placeholder for future implementation).

        Args:
            payload: Request payload

        Returns:
            Dictionary with cost estimates
        """
        # This is a placeholder - actual implementation would depend on pricing
        input_tokens = self._estimate_tokens(payload)

        return {
            "estimated_input_tokens": input_tokens,
            "estimated_output_tokens": payload.get("max_tokens", 0),
            "estimated_cost_usd": 0.0,  # Placeholder
        }

    def _estimate_tokens(self, payload: Dict[str, Any]) -> int:
        """
        Estimate input tokens from payload.

        Args:
            payload: Request payload

        Returns:
            Estimated token count
        """
        total_chars = 0

        if "messages" in payload:
            for message in payload["messages"]:
                if "content" in message:
                    total_chars += len(str(message["content"]))

        # Rough estimation: ~4 characters per token
        return total_chars // 4

    def set_api_key(self, api_key: str):
        """
        Update API key for authenticated endpoints.

        Args:
            api_key: New API key
        """
        self.api_key = api_key
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        else:
            self.session.headers.pop("Authorization", None)

    def update_endpoint(self, endpoint_url: str):
        """
        Update endpoint URL.

        Args:
            endpoint_url: New endpoint URL
        """
        self.endpoint_url = endpoint_url.rstrip("/")
        self.logger.info("Endpoint actualizado: %s", self.endpoint_url)

    def get_endpoint_info(self) -> Dict[str, Any]:
        """
        Get information about the current endpoint configuration.

        Returns:
            Dictionary with endpoint information
        """
        return {
            "endpoint_url": self.endpoint_url,
            "has_api_key": bool(self.api_key),
            "timeout": self.timeout_llm_response,
            "session_headers": dict(self.session.headers),
        }
