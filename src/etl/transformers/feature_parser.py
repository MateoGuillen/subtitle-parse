"""
Parser for converting LLM JSON responses into structured features.
"""

from typing import Dict, List
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


class FeatureParser:
    """
    Parser for converting LLM JSON responses into structured features.

    This class takes the JSON responses from LLM models and converts them
    into normalized features that can be stored in the llm_features table.
    Different title_slugs may require different parsing strategies.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)
        self._initialize_parsers()

    def _initialize_parsers(self):
        """Initialize parser functions for different title_slugs."""
        self.parsers = {
            "capacidad_financiera": self._parse_capacidad_financiera,
            "experiencia_tecnica": self._parse_experiencia_tecnica,
            "documentos_legales": self._parse_documentos_legales,
            "criterios_evaluacion": self._parse_criterios_evaluacion,
            "garantias": self._parse_garantias,
        }

    @error_handling(default_return=[])
    def parse_features(
        self, llm_response: Dict, title_slug: str, config_data: Dict
    ) -> List[Dict[str, str]]:
        """
        Parse LLM response into features.

        Args:
            llm_response: JSON response from LLM
            title_slug: The type of content being parsed
            config_data: Configuration data for this title_slug

        Returns:
            List of feature dictionaries with keys: feature_name, feature_value
        """
        if not llm_response:
            self.logger.warning(f"Empty response for {title_slug}")
            return []

        # Use specific parser if available, otherwise use generic parser
        parser_func = self.parsers.get(title_slug, self._parse_generic)

        try:
            features = parser_func(llm_response, config_data)
            self.logger.debug(f"Parsed {len(features)} features for {title_slug}")
            return features

        except Exception as e:
            self.logger.error(f"Error parsing features for {title_slug}: {str(e)}")
            return []

    def _parse_generic(self, response: Dict, config_data: Dict) -> List[Dict[str, str]]:
        """
        Generic parser for unknown title_slugs.

        Args:
            response: LLM response
            config_data: Configuration data

        Returns:
            List of features
        """
        features = []

        def flatten_dict(d: Dict, prefix: str = "") -> None:
            """Recursively flatten dictionary into features."""
            for key, value in d.items():
                feature_name = f"{prefix}{key}" if prefix else key

                if isinstance(value, dict):
                    flatten_dict(value, f"{feature_name}_")
                elif isinstance(value, list):
                    # Handle lists by creating indexed features
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            flatten_dict(item, f"{feature_name}_{i}_")
                        else:
                            features.append(
                                {
                                    "feature_name": f"{feature_name}_{i}",
                                    "feature_value": str(item),
                                }
                            )
                else:
                    features.append(
                        {
                            "feature_name": feature_name,
                            "feature_value": str(value) if value is not None else "",
                        }
                    )

        flatten_dict(response)
        return features

    def _parse_capacidad_financiera(
        self, response: Dict, config_data: Dict
    ) -> List[Dict[str, str]]:
        """
        Parse capacidad financiera specific features.

        Expected response structure:
        {
            "requiere_capacidad_financiera": true/false,
            "monto_minimo": number,
            "ratios_requeridos": {...},
            "documentos_requeridos": [...]
        }
        """
        features = []

        # Basic requirement
        features.append(
            {
                "feature_name": "requiere_capacidad_financiera",
                "feature_value": str(
                    response.get("requiere_capacidad_financiera", False)
                ),
            }
        )

        # Minimum amount
        if "monto_minimo" in response:
            features.append(
                {
                    "feature_name": "monto_minimo",
                    "feature_value": str(response["monto_minimo"]),
                }
            )

        # Financial ratios
        ratios = response.get("ratios_requeridos", {})
        for ratio_name, ratio_value in ratios.items():
            features.append(
                {
                    "feature_name": f"ratio_{ratio_name}",
                    "feature_value": str(ratio_value),
                }
            )

        # Required documents count
        docs = response.get("documentos_requeridos", [])
        features.append(
            {"feature_name": "documentos_count", "feature_value": str(len(docs))}
        )

        # Specific document requirements
        doc_types = ["balance", "estado_resultados", "flujo_efectivo", "auditoria"]
        for doc_type in doc_types:
            required = any(doc_type in str(doc).lower() for doc in docs)
            features.append(
                {"feature_name": f"requiere_{doc_type}", "feature_value": str(required)}
            )

        return features

    def _parse_experiencia_tecnica(
        self, response: Dict, config_data: Dict
    ) -> List[Dict[str, str]]:
        """
        Parse experiencia técnica specific features.

        Expected response structure:
        {
            "anos_experiencia_minima": number,
            "proyectos_similares_requeridos": number,
            "certificaciones_requeridas": [...],
            "personal_especializado": {...}
        }
        """
        features = []

        # Minimum years of experience
        features.append(
            {
                "feature_name": "anos_experiencia_minima",
                "feature_value": str(response.get("anos_experiencia_minima", 0)),
            }
        )

        # Required similar projects
        features.append(
            {
                "feature_name": "proyectos_similares_requeridos",
                "feature_value": str(response.get("proyectos_similares_requeridos", 0)),
            }
        )

        # Certifications
        certs = response.get("certificaciones_requeridas", [])
        features.append(
            {"feature_name": "certificaciones_count", "feature_value": str(len(certs))}
        )

        # Specialized personnel
        personal = response.get("personal_especializado", {})
        for role, count in personal.items():
            features.append(
                {"feature_name": f"personal_{role}", "feature_value": str(count)}
            )

        return features

    def _parse_documentos_legales(
        self, response: Dict, config_data: Dict
    ) -> List[Dict[str, str]]:
        """
        Parse documentos legales specific features.
        """
        features = []

        # Document requirements
        docs_required = response.get("documentos_requeridos", [])
        features.append(
            {
                "feature_name": "total_documentos_requeridos",
                "feature_value": str(len(docs_required)),
            }
        )

        # Common legal documents
        legal_docs = [
            "ruc",
            "patente",
            "certificado_cumplimiento_tributario",
            "declaracion_jurada",
            "poder_representacion",
        ]

        for doc in legal_docs:
            required = any(
                doc in str(item).lower().replace(" ", "_") for item in docs_required
            )
            features.append(
                {"feature_name": f"requiere_{doc}", "feature_value": str(required)}
            )

        # Legal compliance requirements
        features.append(
            {
                "feature_name": "requiere_cumplimiento_legal",
                "feature_value": str(response.get("requiere_cumplimiento_legal", True)),
            }
        )

        return features

    def _parse_criterios_evaluacion(
        self, response: Dict, config_data: Dict
    ) -> List[Dict[str, str]]:
        """
        Parse criterios de evaluación specific features.
        """
        features = []

        # Evaluation criteria weights
        criterios = response.get("criterios", {})
        for criterio, peso in criterios.items():
            features.append(
                {"feature_name": f"peso_{criterio}", "feature_value": str(peso)}
            )

        # Scoring method
        features.append(
            {
                "feature_name": "metodo_puntuacion",
                "feature_value": str(response.get("metodo_puntuacion", "")),
            }
        )

        # Minimum score required
        features.append(
            {
                "feature_name": "puntaje_minimo",
                "feature_value": str(response.get("puntaje_minimo", 0)),
            }
        )

        return features

    def _parse_garantias(
        self, response: Dict, config_data: Dict
    ) -> List[Dict[str, str]]:
        """
        Parse garantías specific features.
        """
        features = []

        # Guarantee types
        garantias = response.get("garantias_requeridas", [])
        features.append(
            {"feature_name": "total_garantias", "feature_value": str(len(garantias))}
        )

        # Common guarantee types
        tipos_garantia = [
            "seriedad_oferta",
            "cumplimiento_contrato",
            "anticipo",
            "vicios_ocultos",
        ]

        for tipo in tipos_garantia:
            required = any(tipo in str(g).lower().replace(" ", "_") for g in garantias)
            features.append(
                {"feature_name": f"requiere_{tipo}", "feature_value": str(required)}
            )

        # Guarantee amounts (as percentage of contract value)
        for garantia in garantias:
            if isinstance(garantia, dict):
                tipo = garantia.get("tipo", "").lower().replace(" ", "_")
                porcentaje = garantia.get("porcentaje", 0)
                if tipo and porcentaje:
                    features.append(
                        {
                            "feature_name": f"porcentaje_{tipo}",
                            "feature_value": str(porcentaje),
                        }
                    )

        return features

    @error_handling(default_return=True)
    def validate_features(self, features: List[Dict[str, str]]) -> bool:
        """
        Validate that features have the required structure.

        Args:
            features: List of feature dictionaries

        Returns:
            True if valid, False otherwise
        """
        for feature in features:
            if not isinstance(feature, dict):
                self.logger.error("Feature must be a dictionary")
                return False

            if "feature_name" not in feature or "feature_value" not in feature:
                self.logger.error(
                    "Feature must have 'feature_name' and 'feature_value'"
                )
                return False

            if not isinstance(feature["feature_name"], str):
                self.logger.error("feature_name must be a string")
                return False

        return True

    def get_feature_stats(self, features: List[Dict[str, str]]) -> Dict:
        """
        Get statistics about parsed features.

        Args:
            features: List of features

        Returns:
            Statistics dictionary
        """
        if not features:
            return {"total_features": 0}

        feature_names = [f["feature_name"] for f in features]

        return {
            "total_features": len(features),
            "unique_names": len(set(feature_names)),
            "empty_values": sum(1 for f in features if not f["feature_value"]),
            "boolean_features": sum(
                1 for f in features if f["feature_value"].lower() in ["true", "false"]
            ),
            "numeric_features": sum(
                1
                for f in features
                if f["feature_value"].replace(".", "").replace("-", "").isdigit()
            ),
        }
