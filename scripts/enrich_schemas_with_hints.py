"""
Merge v1 extraction_hints into v2 schema format, generating robust prompts for LLM extraction.
Includes negative disambiguation (what NOT to extract) and range validation.

Usage::
    python scripts/enrich_schemas_with_hints.py
"""

import json, re
from pathlib import Path
from typing import Dict, Optional

V1_PATH = Path("data/processed/section_clustering/master_schemas.json")
V2_PATH = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
OUT_PATH = Path("data/processed/section_clustering_v2/master_schemas_v2.json")

V1_FIELD_HINTS: Dict[str, Dict[str, str]] = {}

PREFIX_RULES = {
    "dias": {
        "units": "dias",
        "keywords": "dias, dias calendario, dias corridos, plazo de",
        "positive": "Buscar patrones como 'N dias', 'N dias calendario', 'plazo de N dias', 'dentro de los N dias'.",
        "negative": "NO extraer porcentajes (ej: '10%', '10,00%') aunque esten cerca del numero. NO extraer montos en guarani (ej: 'Gs. 10.000.000'). NO extraer numeros que sean parte de una fecha (ej: '15 de enero').",
        "range": "Rango valido: 1-365. Si el numero esta fuera de este rango o es 0, responder NULL.",
    },
    "pct": {
        "units": "porcentaje",
        "keywords": "porcentaje, %, por ciento",
        "positive": "Buscar patrones como 'N%', 'N por ciento', 'N% del monto', 'el N%'.",
        "negative": "NO extraer numeros que no tengan '%' o 'por ciento' cerca. NO extraer montos absolutos (ej: 'Gs. 10.000.000').",
        "range": "Rango valido: 0.01-100. Si el numero esta fuera de este rango, responder NULL.",
    },
    "ratio": {
        "units": "ratio",
        "keywords": "ratio, indice, liquidez, endeudamiento",
        "positive": "Buscar patrones como 'N,N', 'N.N', 'ratio de N', 'indice de N'.",
        "negative": "NO extraer porcentajes. NO extraer numeros que sean parte de listas numeradas (ej: '1.', '2.'). NO extraer montos.",
        "range": "Rango valido: 0.01-10. Si el numero esta fuera, responder NULL.",
    },
    "num": {
        "units": "cantidad",
        "keywords": "cantidad, numero de, referencias, documentos, pasos, componentes",
        "positive": "CONTAR los elementos listados en el texto. Si hay una lista (1., 2., 3.), contar cuantos items tiene. Buscar frases como 'N documentos', 'N referencias', 'los siguientes N'.",
        "negative": "NO extraer porcentajes (ej: '10%'), montos (ej: 'Gs. 10.000.000'), ni valores unitarios de la lista (ej: '1.' como item individual, no como total). Extraer el TOTAL de elementos.",
        "range": "Rango valido: 1-50. Si el numero esta fuera, responder NULL.",
    },
    "anos": {
        "units": "anos",
        "keywords": "anos, experiencia, anios",
        "positive": "Buscar patrones como 'N anos', 'N anios', 'experiencia de N anos', 'minimo de N anos'.",
        "negative": "NO extraer porcentajes, montos, ni numeros que sean parte de fechas (ej: '2020', '2021').",
        "range": "Rango valido: 1-50. Si el numero esta fuera, responder NULL.",
    },
}


def load_v1():
    v1 = json.loads(V1_PATH.read_text(encoding="utf-8"))
    for v1_title, v1_data in v1.items():
        normalized = v1_title.strip().lower().replace(" ", "_").replace("\u00f1", "n")
        schema = v1_data.get("json_schema", {})
        if not schema:
            continue
        hints = {}
        for fname, finfo in schema.items():
            hint = finfo.get("extraction_hint") or finfo.get("description", "")
            hints[fname] = hint
        V1_FIELD_HINTS[normalized] = hints


def _get_prefix(name: str) -> Optional[str]:
    """Get the field prefix (dias_, pct_, ratio_, num_, anos_)."""
    for prefix in ["dias", "pct", "ratio", "num", "anos"]:
        if name.startswith(prefix):
            return prefix
    return None


def _make_hint(name: str, ftype: str, desc: str) -> str:
    prefix = _get_prefix(name)
    rules = PREFIX_RULES.get(prefix) if prefix else None
    kw = _extract_keywords(desc)

    if ftype == "binary":
        if kw:
            return f"Buscar si el texto menciona o hace referencia a {kw}. Retornar 1 si se menciona, 0 si no."
        return "Retornar 1 si la condicion descrita se cumple, 0 si no."

    if rules:
        parts = [
            f"Buscar el {rules['units']} asociado a {kw}.",
            rules["positive"],
            rules["negative"],
            rules["range"],
            f"Extraer SOLO el numero. Si no se encuentra o no es valido, responder NULL.",
        ]
        return " ".join(parts)

    unit = _infer_unit(name, desc)
    if ftype == "int":
        parts = [f"Buscar el valor numerico asociado a {kw}."]
        if unit:
            parts.append(f"Buscar patrones como 'N {unit}' o '{unit} N' donde N es el numero.")
        parts.append("Extraer SOLO el numero entero, sin unidades ni texto adicional.")
        parts.append("Si hay varios valores, tomar el principal. Si no se encuentra, responder NULL.")
        return " ".join(parts)
    elif ftype == "float":
        parts = [f"Buscar el valor numerico (porcentaje o decimal) asociado a {kw}."]
        parts.append("Buscar patrones como 'N%', 'N% del monto', 'N por ciento'.")
        parts.append("Extraer SOLO el numero decimal (ej: 10 para 10%, 0.1 para 0.1%).")
        parts.append("Si no se encuentra, responder NULL.")
        return " ".join(parts)
    elif ftype == "categorical":
        return f"Buscar la categoria o tipo mencionado para {kw}. Extraer SOLO el texto de la categoria."
    return desc


def _extract_keywords(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\(t[i\u00ed]picamente[^)]*\)", "", text)
    text = re.sub(r"\(ej:?[^)]*\)", "", text)
    text = re.sub(r"\(valor num[e\u00e9]rico[^)]*\)", "", text)
    stopwords = {"si", "el", "la", "los", "las", "un", "una", "de", "del", "por", "para", "que", "con", "en", "se", "no", "es", "su", "como"}
    words = [w for w in text.lower().split() if w not in stopwords and len(w) > 3]
    if not words:
        return text[:80]
    return ", ".join(words[:5])


def _infer_unit(name: str, desc: str) -> Optional[str]:
    if any(w in name for w in ["dias", "dia"]):
        return "dias"
    if any(w in name for w in ["anos", "anio"]):
        return "anos"
    return None


def _find_v1_hint(v2_title: str, v2_field_name: str) -> Optional[str]:
    if v2_title not in V1_FIELD_HINTS:
        return None
    hints = V1_FIELD_HINTS[v2_title]
    candidates = []
    v2_keywords = set(re.split(r"[_\s]+", v2_field_name.lower()))
    for v1_fname, v1_hint in hints.items():
        v1_keywords = set(re.split(r"[_\s]+", v1_fname.lower()))
        overlap = len(v2_keywords & v1_keywords)
        if overlap > 0:
            candidates.append((overlap, v1_fname, v1_hint))
    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][2]
    return None


def enrich():
    load_v1()
    v2 = json.loads(V2_PATH.read_text(encoding="utf-8"))

    for slug, title_data in v2.items():
        if slug == "meta":
            continue
        fields = title_data.get("fields", [])
        for field in fields:
            name = field["name"]
            ftype = field["type"]
            desc = field.get("description", "")

            v1_hint = _find_v1_hint(slug, name)
            if v1_hint:
                field["extraction_hint"] = v1_hint
            else:
                field["extraction_hint"] = _make_hint(name, ftype, desc)

    OUT_PATH.write_text(
        json.dumps(v2, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    v1_total = sum(1 for s, t in V1_FIELD_HINTS.items() for f in t)
    total = sum(len(td.get("fields", [])) for slug, td in v2.items() if slug != "meta")
    matched = sum(1 for slug, td in v2.items() if slug != "meta" for f in td["fields"] if _find_v1_hint(slug, f["name"]))
    print(f"v1 fields: {v1_total}")
    print(f"v2 fields total: {total}")
    print(f"v1 hints matched: {matched}")
    print(f"auto-generated: {total - matched}")
    print(f"Saved to {OUT_PATH}")


if __name__ == "__main__":
    enrich()
