"""Mapping between v2 schema slugs and DB title_normalized values."""

SLUG_TO_TITLE_NORM: dict[str, str] = {
    "analisis_de_precios_ofertados": "analisis de precios ofertados",
    "porcentaje_de_multas": "porcentaje de multas",
    "fuerza_mayor": "fuerza mayor",
    "porcentaje_de_garantia_de_fiel_cumplimiento_de_contrato": "porcentaje de garantia de fiel cumplimiento de contrato",
    "experiencia_requerida": "experiencia requerida",
    "plazo_de_reposicion_de_bienes": "plazo de reposicion de bienes",
    "documentacion_requerida_para_la_firma_del_contrato": "documentacion requerida para la firma del contrato",
    "periodo_de_validez_de_la_garantia_de_los_bienes": "periodo de validez de la garantia de los bienes",
    "capacidad_tecnica": "capacidad tecnica",
    "formas_y_condiciones_de_pago": "formas y condiciones de pago",
    "capacidad_financiera": "capacidad financiera",
    "reajuste": "reajuste",
    "forma_de_instrumentacion_de_garantia_de_anticipo": "forma de instrumentacion de garantia de anticipo",
    "requisitos_de_calificacion": "requisitos de calificacion",
    "solicitud_de_pago_de_anticipo": "solicitud de pago de anticipo",
    "tasa_de_interes_por_mora": "tasa de interes por mora",
}


def get_hybrid_title_slugs() -> list[str]:
    return list(SLUG_TO_TITLE_NORM.keys())
