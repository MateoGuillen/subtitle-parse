"""
Transformer for creating LLM API requests from pliego content and configuration.
"""

import json
from typing import Dict, Any, Optional
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class LLMRequestTransformer:
    """
    Transformer for creating LLM API requests.

    This class transforms pliego content and configuration data into properly
    formatted API requests for LLM endpoints that follow OpenAI-compatible schemas.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)
        self.text_cleaner = None

    def set_dependencies(self, text_cleaner):
        """Set dependency for text cleaning."""
        self.text_cleaner = text_cleaner

    @error_handling(default_return={})
    def create_request_payload(
        self,
        content: str,
        config_data: Dict,
        model: str,
        override_params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Create a complete LLM API request payload.

        Args:
            content: The pliego content to process
            config_data: Configuration dictionary from llm_config
            model: Model name to use (overrides config if provided)
            override_params: Optional parameters to override config values

        Returns:
            Dictionary containing the complete API request payload
        """
        # Clean and prepare content
        cleaned_content = self._prepare_content(content)

        # Build the prompt
        full_prompt = self._build_prompt(config_data["prompt"], cleaned_content)

        # Prepare parameters
        params = self._prepare_parameters(config_data, model, override_params)

        # Build the request payload
        payload = {
            "model": params["model"],
            "messages": [{"role": "user", "content": full_prompt}],
            "temperature": params["temperature"],
            "max_tokens": params["max_tokens"],
        }

        # Add response format if json_schema is provided
        if config_data.get("json_schema"):
            response_format = self._prepare_response_format(config_data["json_schema"])
            if response_format:  # Solo agregar si es válido
                payload["response_format"] = response_format

        self.logger.debug(f"Payload creado para modelo: {model}")
        return payload

    def _prepare_content(self, content: str) -> str:
        """
        Clean and prepare content for LLM processing.

        Args:
            content: Raw content from pliego

        Returns:
            Cleaned content string
        """
        if not content:
            return ""

        # Use text cleaner if available
        if self.text_cleaner:
            cleaned = self.text_cleaner.clean(content)
        else:
            cleaned = content

        # Additional cleaning specific to LLM requests
        cleaned = self._basic_text_cleaning(cleaned)

        # Truncate if too long (keeping last part which usually contains requirements)
        max_length = 8000  # Adjust based on model context window
        if len(cleaned) > max_length:
            # Take last portion which typically contains the requirements
            cleaned = "..." + cleaned[-max_length:]
            self.logger.debug(f"Content truncated to {max_length} characters")

        return cleaned

    def _basic_text_cleaning(self, text: str) -> str:
        """
        Basic text cleaning for LLM processing.

        Args:
            text: Input text

        Returns:
            Cleaned text
        """
        # Remove excessive whitespace
        text = " ".join(text.split())

        # Remove common OCR artifacts
        text = text.replace("_", " ")
        text = text.replace("|", " ")

        # Fix common spacing issues
        text = text.replace(" .", ".")
        text = text.replace(" ,", ",")
        text = text.replace("( ", "(")
        text = text.replace(" )", ")")

        return text.strip()

    def _build_prompt(self, base_prompt: str, content: str) -> str:
        """
        Build the complete prompt by combining base prompt with content.

        Args:
            base_prompt: The base prompt from configuration
            content: The pliego content

        Returns:
            Complete prompt string
        """
        # Format the prompt with content
        if "{content}" in base_prompt:
            return base_prompt.format(content=content)
        else:
            # Default format if no placeholder
            return f'{base_prompt}\n\n"""\n{content}\n"""'

    def _prepare_parameters(
        self, config_data: Dict, model: str, override_params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Prepare API parameters from configuration.

        Args:
            config_data: Configuration dictionary
            model: Model name to use
            override_params: Optional parameter overrides

        Returns:
            Dictionary with API parameters
        """
        params = {
            "model": model
            or config_data.get("modelo_recomendado", "qwen2.5-7b-instruct"),
            "temperature": config_data.get("temperatura", 0.1),
            "max_tokens": config_data.get("max_tokens", 800),
        }

        # Apply overrides if provided
        if override_params:
            params.update(override_params)

        # Ensure proper types
        params["temperature"] = float(params["temperature"])
        params["max_tokens"] = int(params["max_tokens"])

        return params

    def _prepare_response_format(self, json_schema: Any) -> Optional[Dict[str, Any]]:
        """
        Prepare response format for structured output.

        Args:
            json_schema: JSON schema definition from database (JSONB)

        Returns:
            Response format dictionary or None if invalid
        """
        try:
            # Si json_schema es una cadena, parsearlo
            if isinstance(json_schema, str):
                schema_dict = json.loads(json_schema)
            elif isinstance(json_schema, dict):
                schema_dict = json_schema
            else:
                self.logger.error(f"Tipo de schema no soportado: {type(json_schema)}")
                return None

            # Validar que el schema tenga la estructura básica requerida
            if not isinstance(schema_dict, dict):
                self.logger.error("El schema debe ser un diccionario")
                return None

            # El formato correcto para OpenAI-compatible APIs
            response_format = {
                "type": "json_schema",
                "json_schema": {
                    "name": "extraction_result",
                    "schema": schema_dict,
                    "strict": True,
                },
            }

            self.logger.debug("Response format preparado correctamente")
            return response_format

        except json.JSONDecodeError as e:
            self.logger.error(f"Error al parsear JSON schema: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error preparando response format: {e}")
            return None

    @error_handling(default_return={})
    def create_batch_payload(
        self, content_list: list, config_data: Dict, model: str
    ) -> Dict[str, Any]:
        """
        Create a batch request payload for multiple contents.

        Args:
            content_list: List of content strings
            config_data: Configuration dictionary
            model: Model name

        Returns:
            Batch request payload
        """
        # For APIs that support batch processing
        # This is a placeholder for future batch implementation
        batch_requests = []

        for i, content in enumerate(content_list):
            request = self.create_request_payload(content, config_data, model)
            request["custom_id"] = f"request_{i}"
            batch_requests.append(request)

        return {"requests": batch_requests}

    def estimate_tokens(self, text: str) -> int:
        """
        Estimate token count for text (rough approximation).

        Args:
            text: Input text

        Returns:
            Estimated token count
        """
        # Rough estimation: ~4 characters per token
        return len(text) // 4

    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        """
        Validate that the payload has required fields.

        Args:
            payload: Request payload to validate

        Returns:
            True if valid, False otherwise
        """
        required_fields = ["model", "messages"]

        for field in required_fields:
            if field not in payload:
                self.logger.error(f"Missing required field: {field}")
                return False

        # Validate messages structure
        if not isinstance(payload["messages"], list) or not payload["messages"]:
            self.logger.error("Messages must be a non-empty list")
            return False

        for msg in payload["messages"]:
            if not isinstance(msg, dict) or "role" not in msg or "content" not in msg:
                self.logger.error("Invalid message structure")
                return False

        # Validate response_format if present
        if "response_format" in payload:
            if not self._validate_response_format(payload["response_format"]):
                return False

        return True

    def _validate_response_format(self, response_format: Dict) -> bool:
        """
        Validate response_format structure.

        Args:
            response_format: Response format dictionary to validate

        Returns:
            True if valid, False otherwise
        """
        if not isinstance(response_format, dict):
            self.logger.error("response_format debe ser un diccionario")
            return False

        if response_format.get("type") != "json_schema":
            self.logger.error("response_format.type debe ser 'json_schema'")
            return False

        json_schema = response_format.get("json_schema")
        if not isinstance(json_schema, dict):
            self.logger.error("response_format.json_schema debe ser un diccionario")
            return False

        schema = json_schema.get("schema")
        if not isinstance(schema, dict):
            self.logger.error("response_format.json_schema.schema debe ser un objeto")
            return False

        return True
