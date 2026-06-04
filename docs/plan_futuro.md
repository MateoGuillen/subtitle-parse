# Plan Futuro — Extracción Masiva y Detección de Anomalías

---

## Prioridades

| # | Tarea | Archivos | Esfuerzo | Impacto |
|:-:|-------|----------|:--------:|:-------:|
| **1** | Diseñar schemas con web LLM | `llm_chat_prompts.json` | 2-3h manual | 🔴 Alto |
| **2** | Import de resultados de chat | `scripts/import_chat_results.py` (nuevo) | 1 día | 🟡 Medio |
| **3** | Pipeline de extracción masiva | 5 archivos nuevos | 3-5 días | 🔴 Alto |
| **4** | Validación de clusters | `section_clustering_transformer.py` | 1-2 días | 🟡 Medio |
| **5** | Pipeline de anomalías | 3+ archivos nuevos | 3-5 días | 🔴 Alto |

---

## 1. Diseñar Schemas con Web LLM

### Estado Actual

`llm_chat_prompts.json` contiene 10 prompts optimizados (2 muestras/cluster, truncado dinámico, clusters fusionados). Cada prompt cabe en ChatGPT (32K) y DeepSeek (128K).

### Acción

Abrir `data/processed/section_clustering/llm_chat_prompts.json` y para cada título:

1. Copiar `prompt.system` + `prompt.user` al LLM web
2. El LLM responde con JSON schema + justification
3. Guardar la respuesta

### Orden Sugerido

| Prioridad | Título | Clusters | Longitud prompt | Tokens estimados |
|:---------:|--------|:-------:|:---------------:|:----------------:|
| 🔴 1 | fraude y corrupcion | 10 | ~25,724 chars | ~6,431 tok |
| 🔴 2 | retiro sustitucion y modificacion | 11 | ~19,349 chars | ~4,837 tok |
| 🟡 3 | audiencia informativa | 10 | ~6,340 chars | ~1,585 tok |
| 🟡 4 | aclaracion de las ofertas | — | — | — |
| 🟡 5 | formato y firma | 6 | ~5,220 chars | ~1,305 tok |
| 🟢 6 | limitacion responsabilidad | 5 | ~3,785 chars | ~946 tok |
| 🟢 7 | idioma de la oferta | 4 | ~2,064 chars | ~516 tok |
| 🟢 8 | porcentaje garantia | — | — | — |
| 🟢 9 | copias de la oferta | 3 | ~1,767 chars | ~442 tok |
| 🟢 10 | planos y disenos | 3 | ~1,336 chars | ~334 tok |

### Formato de Respuesta Esperado

```json
{
  "schema": {
    "nombre_campo": {
      "type": "binary",
      "description": "Descripción clara del campo"
    }
  },
  "justification": "Explicación breve de cada campo elegido y cómo diferencia clusters"
}
```

### LLMs Recomendados

- **DeepSeek** (128K context, $0.14/M tok input, $0.28/M tok output) — recomendado para títulos largos
- **ChatGPT** (32K context, GPT-4o) — para títulos medianos/pequeños
- **Claude** (200K context) — puede procesar varios títulos en una sola sesión

---

## 2. Import de Resultados de Chat

### Archivo a Crear

`scripts/import_chat_results.py` (~200 lines)

### Comportamiento

```bash
python scripts/import_chat_results.py \
    --input chat_results.json \
    --master data/processed/section_clustering/master_schemas.json \
    --output data/processed/section_clustering/master_schemas_updated.json
# o in-place:
python scripts/import_chat_results.py \
    --input chat_results.json \
    --master data/processed/section_clustering/master_schemas.json \
    --in-place
```

### Formato de `chat_results.json`

```json
{
  "fraude y corrupcion": {
    "schema": {
      "menciona_soborno": {
        "type": "binary",
        "description": "Indica si se menciona soborno"
      }
    },
    "justification": "Cluster 5 menciona soborno explícitamente..."
  },
  "formato y firma de la oferta": {
    "schema": { ... },
    "justification": "..."
  }
}
```

### Validaciones a Implementar

```python
def validate_schema_result(result: dict) -> Tuple[bool, str]:
    """Misma validación que SectionClusteringTransformer._validate_schema"""
    # - result tiene 'schema' y 'justification' (strings no vacíos)
    # - schema es dict con 1-12 campos
    # - cada campo: type ∈ {binary, numeric}
    # - cada campo: description string no vacío
    # - sin claves extra (solo type, description)
```

### Lógica Principal

```python
def main():
    args = parse_args()
    with open(args.master) as f:
        master = json.load(f)
    with open(args.input) as f:
        new_schemas = json.load(f)

    for title, schema_info in new_schemas.items():
        if title not in master:
            print(f"WARN: '{title}' not in master, skipping")
            continue
        is_valid, reason = validate_schema_result(schema_info)
        if not is_valid:
            print(f"ERROR: '{title}' invalid: {reason}")
            continue
        # Update
        master[title]["json_schema"] = schema_info["schema"]
        master[title]["schema_justification"] = schema_info["justification"]
        # Rebuild extraction prompt
        master[title]["extraction_prompt"] = build_extraction_prompt(
            title, schema_info["schema"]
        )
        print(f"OK: updated '{title}' ({len(schema_info['schema'])} fields)")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(master, f, indent=2, ensure_ascii=False)
```

### Función `build_extraction_prompt` (tomar de `section_clustering_transformer.py` o `update_schemas.py`)

```python
EXTRACTION_PROMPT_TEMPLATE = """Eres un extractor de datos estructurados para pliegos de licitaciones públicas paraguayas.

CONTEXTO: Sección "{titulo}" de un pliego de licitación.
{titulo_descripcion}

Debes analizar el texto proporcionado y extraer UNICAMENTE los campos definidos en el siguiente esquema JSON.

ESQUEMA:
{esquema_str}

REGLAS:
1. Campos BINARIOS: 1 si la característica está presente en el texto, 0 si no.
2. Campos NUMÉRICOS: el valor numérico encontrado. Si no hay valor, poner 0.
3. Responde EXCLUSIVAMENTE con un JSON válido, sin texto adicional.
4. No inventes información que no esté en el texto.

TEXTO A ANALIZAR:
{texto}

RESPUESTA (solo JSON):"""

TITULO_DESCRIPCION = {
    "fraude y corrupcion": "Contiene referencias a posibles actos de fraude...",
    "formato y firma de la oferta": "Describe el formato requerido...",
    "copias de la oferta cps": "Especifica la cantidad y tipo de copias...",
    "limitacion de responsabilidad": "Define las limitaciones de responsabilidad...",
    "planos y disenos": "Describe requisitos de planos, diseños...",
    "porcentaje de garantia de fiel cumplimiento de con": "Establece el porcentaje...",
    "idioma de la oferta": "Especifica el idioma o idiomas...",
    "aclaracion de las ofertas": "Describe el proceso para solicitar aclaraciones...",
    "retiro sustitucion y modificacion de las ofertas": "Regula el retiro...",
    "audiencia informativa": "Describe la realización de audiencias...",
}
```

**REFACTOR SUGERIDO**: Mover `EXTRACTION_PROMPT_TEMPLATE`, `TITULO_DESCRIPCION` y `build_extraction_prompt` a un módulo compartido (`src/etl/transformers/prompt_utils.py`) para no duplicar entre `section_clustering_transformer.py` y `update_schemas.py` (y el futuro `import_chat_results.py` y `section_extraction_transformer.py`).

---

## 3. Pipeline de Extracción Masiva

### Especificación Completa

### 3.1 Tabla en Base de Datos

```sql
-- Ejecutar antes del pipeline
CREATE TABLE IF NOT EXISTS dncp.section_extracted_features (
    nro_licitacion VARCHAR NOT NULL,
    title_normalized VARCHAR NOT NULL,
    features JSONB NOT NULL,
    schema_version VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    llm_model VARCHAR,
    PRIMARY KEY (nro_licitacion, title_normalized)
);
```

**`schema_version`**: usar hash del schema JSON o timestamp ISO. Ej: `v20260604` o hash MD5 del schema.

### 3.2 Arquitectura de Archivos

| Archivo | Clase | Método principal |
|---------|-------|------------------|
| `src/etl/extractors/section_extraction_extractor.py` | `SectionExtractionExtractor` | `extract_by_title(title) -> pd.DataFrame` |
| `src/etl/transformers/section_extraction_transformer.py` | `SectionExtractionTransformer` | `extract_features(text, schema, extraction_prompt) -> dict` |
| `src/etl/loaders/section_extraction_loader.py` | `SectionExtractionLoader` | `save_checkpoint()`, `load_checkpoint()`, `upsert_batch()` |
| `src/pipelines/section_extraction_pipeline.py` | `SectionExtractionPipeline` | `run()` |
| `scripts/run_section_extraction_pipeline.py` | `main()` | CLI entry point |

### 3.3 Extractor (`section_extraction_extractor.py`)

```python
import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.logging_utils import setup_logger

class SectionExtractionExtractor:
    QUERY = """
        SELECT nro_licitacion, content_text, year, category_id
        FROM dncp.pliegos_secciones
        WHERE LOWER(title_normalized) = :title
          AND content_text IS NOT NULL
        ORDER BY nro_licitacion
    """

    def __init__(self, db_params: dict):
        conn_str = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        self._engine = create_engine(conn_str)
        self.logger = setup_logger(__name__)

    def extract_by_title(self, title: str) -> pd.DataFrame:
        """Return all sections for a title. Columns: nro_licitacion, content_text, year, category_id"""
        with self._engine.connect() as conn:
            df = pd.read_sql(text(self.QUERY), conn, params={"title": title})
        return df.dropna(subset=["content_text"])
```

**NOTA**: Reutilizar `TITLE_MAPPING` de `section_clustering_extractor.py` para mapear nombres display → DB.

### 3.4 Transformer (`section_extraction_transformer.py`)

```python
import json
from typing import Any, Dict, Optional
from src.etl.transformers.llm_provider import LLMProvider
from src.utils.logging_utils import setup_logger

class SectionExtractionTransformer:
    def __init__(
        self,
        llm_provider: LLMProvider,
        truncation_chars: int = 1500,
        max_retries: int = 2,
    ):
        self.llm_provider = llm_provider
        self.truncation_chars = truncation_chars
        self.max_retries = max_retries
        self.logger = setup_logger(__name__)

    def extract_features(
        self,
        text: str,
        schema: Dict[str, Any],
        extraction_prompt_template: str,
        title: str,
    ) -> Dict[str, Any]:
        """
        Extract features from a single section text.
        
        Args:
            text: Raw section content
            schema: The JSON schema (for validation)
            extraction_prompt_template: Prompt template with {texto} placeholder
            title: Section title (for logging)
        
        Returns:
            dict of extracted features (all fields from schema with 0/N defaults)
        """
        truncated = text[:self.truncation_chars]
        prompt = extraction_prompt_template.replace("{texto}", truncated)

        messages = [
            {"role": "system", "content": "Eres un extractor de datos estructurados."},
            {"role": "user", "content": prompt},
        ]

        for attempt in range(self.max_retries + 1):
            try:
                response = self.llm_provider.generate(
                    messages,
                    response_format={"type": "json_object"},  # LM Studio soporta
                    max_tokens=512,
                    temperature=0.0,
                )
                result = json.loads(response)
                # Validate against schema
                if self._validate_features(result, schema):
                    return result
            except (json.JSONDecodeError, Exception) as e:
                self.logger.warning(
                    "Extraction failed (attempt %d/%d) for %s: %s",
                    attempt + 1, self.max_retries + 1, title, str(e)
                )

        # Fallback: return all zeros
        return self._fallback(schema)

    def _validate_features(
        self, result: Dict, schema: Dict[str, Any]
    ) -> bool:
        """Check result has all schema fields with correct types."""
        for field_name, field_def in schema.items():
            if field_name not in result:
                return False
            val = result[field_name]
            ftype = field_def.get("type", "binary")
            if ftype == "binary":
                if val not in (0, 1):
                    return False
            elif ftype == "numeric":
                if not isinstance(val, (int, float)):
                    return False
        return True

    def _fallback(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Return all zeros/defaults for a schema."""
        result = {}
        for field_name, field_def in schema.items():
            ftype = field_def.get("type", "binary")
            result[field_name] = 0 if ftype == "binary" else 0.0
        return result
```

**IMPORTANTE**: Usar `response_format` de LM Studio con JSON schema constraint:
```python
response_format={
    "type": "json_schema",
    "json_schema": {
        "schema": schema  # el schema mismo
    }
}
```
Esto obliga al LLM a generar JSON exacto. Verificar compatibilidad con la versión de LM Studio.

### 3.5 Loader (`section_extraction_loader.py`)

```python
import json
import os
from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, text
from src.utils.logging_utils import setup_logger

CHECKPOINT_DIR = "checkpoints"

class SectionExtractionLoader:
    def __init__(self, db_params: dict, checkpoint_dir: str = CHECKPOINT_DIR):
        conn_str = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        self._engine = create_engine(conn_str)
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)
        self.logger = setup_logger(__name__)

    # ---- Checkpoint ----

    def save_checkpoint(self, title: str, processed: int, total: int,
                        last_id: str, errors: List[str]):
        path = os.path.join(self.checkpoint_dir, f"{title.replace(' ', '_')}.json")
        checkpoint = {
            "title": title,
            "processed": processed,
            "total": total,
            "last_nro_licitacion": last_id,
            "errors": errors,
            "timestamp": __import__('datetime').datetime.now().isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2)

    def load_checkpoint(self, title: str) -> Optional[Dict[str, Any]]:
        path = os.path.join(self.checkpoint_dir, f"{title.replace(' ', '_')}.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        return None

    # ---- DB Upsert ----

    UPSERT_SQL = """
        INSERT INTO dncp.section_extracted_features
            (nro_licitacion, title_normalized, features, schema_version, llm_model)
        VALUES (:nro_licitacion, :title, :features::jsonb, :schema_version, :llm_model)
        ON CONFLICT (nro_licitacion, title_normalized)
        DO UPDATE SET
            features = EXCLUDED.features,
            schema_version = EXCLUDED.schema_version,
            extracted_at = CURRENT_TIMESTAMP,
            llm_model = EXCLUDED.llm_model
    """

    def upsert_batch(self, records: List[Dict[str, Any]]):
        """Batch UPSERT. records: list of dicts with nro_licitacion, title, features, schema_version, llm_model"""
        with self._engine.begin() as conn:
            for record in records:
                record["features"] = json.dumps(record["features"], ensure_ascii=False)
                conn.execute(text(self.UPSERT_SQL), record)
        self.logger.info("Upserted %d records", len(records))
```

### 3.6 Pipeline (`section_extraction_pipeline.py`)

```python
import json
import os
import time
from typing import Dict, Any

from src.etl.extractors.section_clustering_extractor import (
    SectionClusteringExtractor, TITLE_MAPPING
)
from src.etl.transformers.section_extraction_transformer import (
    SectionExtractionTransformer
)
from src.etl.loaders.section_extraction_loader import SectionExtractionLoader
from src.etl.transformers.llm_provider import (
    LLMProvider, LocalLLMProvider, OpenRouterProvider
)
from src.utils.logging_utils import setup_logger

BATCH_SIZE = 100          # records per DB upsert
CHECKPOINT_EVERY = 500    # sections between checkpoints

class SectionExtractionPipeline:
    def __init__(self, config: dict):
        self.config = config
        self.db_params = config["db_params"]

        # LLM provider
        llm_type = config.get("llm_provider_type", "local")
        if llm_type == "local":
            self.llm_provider: LLMProvider = LocalLLMProvider(
                base_url=config.get("llm_base_url", "http://localhost:1234/v1")
            )
        else:
            self.llm_provider = OpenRouterProvider(
                api_key=config["llm_api_key"],
                model=config.get("llm_model", "openai/gpt-4o-mini"),
            )

        self.schema_version = config.get("schema_version", "v1")
        self.truncation_chars = config.get("truncation_chars", 1500)
        self.max_workers = config.get("max_workers", 1)
        self.skip_titles = config.get("skip_titles", [])
        self.dry_run = config.get("dry_run", False)
        self.max_sections = config.get("max_sections", None)

        # Componentes
        self.extractor = SectionClusteringExtractor(self.db_params)
        self.transformer = SectionExtractionTransformer(
            llm_provider=self.llm_provider,
            truncation_chars=self.truncation_chars,
        )
        self.loader = SectionExtractionLoader(self.db_params)

        self.logger = setup_logger(__name__)

    def run(self):
        t_start = time.time()
        self.logger.info("=" * 60)
        self.logger.info("SectionExtractionPipeline iniciado")
        self.logger.info("=" * 60)

        # 1. Load master schemas
        master_path = self.config["master_schema_path"]
        with open(master_path, encoding="utf-8") as f:
            master = json.load(f)

        # 2. For each title
        for title in SectionClusteringExtractor.get_top_10_titles():
            if title in self.skip_titles:
                self.logger.info("Skipping '%s'", title)
                continue
            if title not in master:
                self.logger.warning("No schema for '%s', skipping", title)
                continue

            schema = master[title].get("json_schema", {})
            if not schema:
                self.logger.warning("Empty schema for '%s', skipping", title)
                continue

            extraction_prompt = master[title].get("extraction_prompt", "")
            if not extraction_prompt:
                self.logger.warning("No extraction prompt for '%s', skipping", title)
                continue

            # 3. Extract sections
            db_title = TITLE_MAPPING[title]
            df = self.extractor.extract_by_title(db_title)
            if self.max_sections:
                df = df.head(self.max_sections)

            self.logger.info(
                "Processing '%s': %d sections", title, len(df)
            )

            # 4. Check for existing checkpoint
            checkpoint = self.loader.load_checkpoint(title)
            start_idx = 0
            if checkpoint and config.get("resume", False):
                last_id = checkpoint["last_nro_licitacion"]
                # Find index of last_id
                matches = df.index[df["nro_licitacion"] == last_id].tolist()
                if matches:
                    start_idx = df.index.get_loc(matches[0]) + 1
                    self.logger.info(
                        "Resuming '%s' from idx %d (last: %s)",
                        title, start_idx, last_id
                    )

            # 5. Process
            total = len(df)
            processed = start_idx
            errors = checkpoint.get("errors", []) if checkpoint else []
            batch = []

            for idx in range(start_idx, total):
                row = df.iloc[idx]
                text = row["content_text"]

                try:
                    features = self.transformer.extract_features(
                        text=text,
                        schema=schema,
                        extraction_prompt_template=extraction_prompt,
                        title=title,
                    )
                    record = {
                        "nro_licitacion": row["nro_licitacion"],
                        "title": title,
                        "features": features,
                        "schema_version": self.schema_version,
                        "llm_model": self.llm_provider.name(),
                    }
                    if not self.dry_run:
                        batch.append(record)
                    processed += 1
                except Exception as e:
                    errors.append(f"{row['nro_licitacion']}: {str(e)}")
                    self.logger.error(
                        "Error on %s: %s", row["nro_licitacion"], str(e)
                    )

                # Periodic flush
                if not self.dry_run and len(batch) >= BATCH_SIZE:
                    self.loader.upsert_batch(batch)
                    batch = []

                # Checkpoint
                if processed % CHECKPOINT_EVERY == 0:
                    self.logger.info(
                        "Progress '%s': %d/%d (%.1f%%)",
                        title, processed, total, 100*processed/total
                    )
                    if not self.dry_run:
                        self.loader.save_checkpoint(
                            title, processed, total,
                            row["nro_licitacion"], errors[-10:]
                        )

            # Final flush
            if not self.dry_run and batch:
                self.loader.upsert_batch(batch)

            self.logger.info(
                "Done '%s': %d/%d processed, %d errors",
                title, processed, total, len(errors)
            )

        elapsed = time.time() - t_start
        self.logger.info(
            "Pipeline completado. Tiempo total: %.2f min",
            elapsed / 60
        )
```

### 3.7 CLI (`scripts/run_section_extraction_pipeline.py`)

```python
import argparse, os, sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))

from src.pipelines.section_extraction_pipeline import SectionExtractionPipeline
from config.settings import DB_CONFIG, BASE_OUTPUT_PROCESSED_DIR, OPENROUTER_API_KEY

def main():
    parser = argparse.ArgumentParser(description="Section Extraction Pipeline")
    parser.add_argument("--llm-provider", choices=["local", "openrouter"], default="local")
    parser.add_argument("--llm-base-url", default="http://localhost:1234/v1")
    parser.add_argument("--llm-model", default="openai/gpt-4o-mini")
    parser.add_argument("--schema-version", default="v1")
    parser.add_argument("--truncation-chars", type=int, default=1500)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--max-sections", type=int, default=None, help="Limit for testing")
    parser.add_argument("--skip-titles", nargs="*", default=[])
    parser.add_argument("--dry-run", action="store_true", help="Estimate cost without executing")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument(
        "--master",
        default=os.path.join(BASE_OUTPUT_PROCESSED_DIR, "section_clustering", "master_schemas.json")
    )
    args = parser.parse_args()

    config = {
        "db_params": DB_CONFIG,
        "llm_provider_type": args.llm_provider,
        "llm_base_url": args.llm_base_url,
        "llm_api_key": OPENROUTER_API_KEY,
        "llm_model": args.llm_model,
        "schema_version": args.schema_version,
        "truncation_chars": args.truncation_chars,
        "max_workers": args.max_workers,
        "max_sections": args.max_sections,
        "skip_titles": args.skip_titles,
        "dry_run": args.dry_run,
        "resume": args.resume,
        "master_schema_path": args.master,
    }

    pipeline = SectionExtractionPipeline(config)
    pipeline.run()

if __name__ == "__main__":
    main()
```

### 3.8 Dry Run Mode

Cuando `--dry-run`, el pipeline NO escribe a DB ni checkpoints. En su lugar:
- Cuenta secciones por título
- Mide tokens promedio (sampleando 100 secciones)
- Estima costo según modelo OpenRouter
- Muestra tiempo estimado según workers configurados

### 3.9 Costos y Tiempos Estimados

| Opción | Modelo | Workers | Tiempo | Costo |
|--------|--------|:-------:|:------:|:-----:|
| **A** | Qwen 2.5 7B Q4_K_M (local) | 6 | ~22-30h | $0 |
| **B** | Gemini 2.0 Flash (OpenRouter) | N/A | ~30 min | $5-8 |
| **C** | GPT-4o mini (OpenRouter) | N/A | ~30 min | $15-20 |
| **D** | Híbrido: largos→OR, cortos→local | — | ~12h+5min | $3-5 |

**Opción D detallada**:
- Textos largos a OpenRouter: "fraude" (1984 chars), "retiro" (1139 chars), "aclaracion" (760 chars) → ~96K secciones × $0.15/1K = ~$3-5
- Textos cortos a local: resto (planos 300, idioma 281, etc.) → ~218K secciones × 6 workers = ~12h
- Tiempo total: ~12h local + 5 min OpenRouter

### 3.10 Tabla por Título para Extracción

| Título | Secciones | Longitud | Tokens/seg | Opción recomendada |
|--------|:---------:|:--------:|:----------:|:------------------:|
| fraude y corrupcion | 32,180 | 1,984 | ~500 | OpenRouter |
| formato y firma | 32,180 | 574 | ~144 | Local 7B |
| copias de la oferta | 31,298 | 357 | ~89 | Local 7B |
| limitacion responsabilidad | 30,320 | 467 | ~117 | Local 7B |
| planos y disenos | 28,924 | 300 | ~75 | Local 7B |
| porcentaje garantia | 32,180 | 304 | ~76 | Local 7B |
| idioma de la oferta | 31,653 | 281 | ~70 | Local 7B |
| aclaracion ofertas | 32,180 | 760 | ~190 | OpenRouter |
| retiro sustitucion | 31,900 | 1,139 | ~285 | OpenRouter |
| audiencia informativa | 32,180 | 535 | ~134 | Local 7B |
| **TOTAL** | **314,995** | — | — | — |

### 3.11 Concurrencia con LM Studio

LM Studio con `llama-server` soporta múltiples requests concurrentes. Configuración:

```
# En LM Studio: Settings → Server → Max concurrent requests: 6
# O al iniciar llama-server:
llama-server -m model.gguf -c 8192 --parallel 6
```

Usar `asyncio` + `semaphore` para control:

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

async def process_section_async(semaphore, transformer, text, schema, prompt, title):
    async with semaphore:
        return transformer.extract_features(text, schema, prompt, title)

# En pipeline:
semaphore = asyncio.Semaphore(6)
tasks = [process_section_async(semaphore, ...) for ...]
results = await asyncio.gather(*tasks)
```

### 3.12 `response_format` para LM Studio

LM Studio (llama-server) soporta `response_format` desde versiones recientes:
- `{"type": "json_object"}` — cualquier JSON válido
- `{"type": "json_schema", "json_schema": {"schema": {...}}}` — JSON con estructura exacta

**Verificar compatibilidad**:
```bash
curl http://localhost:1234/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{"role": "user", "content": "say hello"}],
    "response_format": {"type": "json_object"}
  }'
```

Si no soporta `json_schema`, usar `json_object` + validación post-hoc.

---

## 4. Validación de Clusters

### Problema Conocido

K-Means opera sobre embeddings reducidos con UMAP (10-20 dims). Las distancias euclideanas en espacio UMAP no corresponden a distancias semánticas en el espacio original (384-768 dims).

### Validaciones a Implementar en `SectionClusteringTransformer`

#### 4.1 Silhouette en Espacio Original

```python
def _validate_clusters_original_space(
    self, embeddings_original: np.ndarray, cluster_labels: np.ndarray
) -> Dict[str, float]:
    """Calculate silhouette in original embedding space with cosine distance."""
    from sklearn.metrics import silhouette_score
    from sklearn.metrics.pairwise import cosine_distances

    # Use cosine distance on original embeddings
    sil = silhouette_score(
        embeddings_original, cluster_labels,
        metric="cosine"
    )
    return {"silhouette_cosine_original": float(sil)}
```

#### 4.2 Intra vs Inter-cluster Similarity

```python
def _cluster_separation_quality(
    self, embeddings: np.ndarray, labels: np.ndarray
) -> Dict[str, float]:
    """Ratio of intra-cluster similarity to inter-cluster similarity."""
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np

    unique = np.unique(labels[labels != -1])
    intra = []
    inter = []

    for cid in unique:
        mask = labels == cid
        cluster_emb = embeddings[mask]
        if len(cluster_emb) < 2:
            continue
        # Intra: mean cosine sim within cluster
        sim_matrix = cosine_similarity(cluster_emb)
        intra.append(np.mean(sim_matrix[np.triu_indices_from(sim_matrix, k=1)]))

        # Inter: mean cosine sim with other clusters
        other_mask = (labels != cid) & (labels != -1)
        if other_mask.sum() > 0:
            other_emb = embeddings[other_mask]
            cross_sim = cosine_similarity(cluster_emb, other_emb)
            inter.append(np.mean(cross_sim))

    if intra and inter:
        ratio = np.mean(intra) / max(np.mean(inter), 1e-10)
        return {"intra_sim": float(np.mean(intra)),
                "inter_sim": float(np.mean(inter)),
                "separation_ratio": float(ratio)}
    return {}
```

#### 4.3 Stability (Adjusted Rand Index)

```python
def _cluster_stability(
    self, X: np.ndarray, k: int, n_runs: int = 10
) -> float:
    """Cluster multiple times with different seeds, measure stability."""
    from sklearn.cluster import KMeans
    from sklearn.metrics import adjusted_rand_score
    import numpy as np

    all_labels = []
    for seed in range(n_runs):
        km = KMeans(n_clusters=k, random_state=seed, n_init=5)
        labels = km.fit_predict(X)
        all_labels.append(labels)

    ari_scores = []
    for i in range(n_runs):
        for j in range(i + 1, n_runs):
            ari = adjusted_rand_score(all_labels[i], all_labels[j])
            ari_scores.append(ari)

    return float(np.mean(ari_scores))
```

### Criterios de Aceptación

| Métrica | Bueno | Regular | Malo |
|---------|:-----:|:-------:|:----:|
| Silhouette (cosine, 384-d) | > 0.30 | 0.15-0.30 | < 0.15 |
| Separation ratio | > 2.0 | 1.5-2.0 | < 1.5 |
| Adjusted Rand Index | > 0.80 | 0.60-0.80 | < 0.60 |

### Posibles Cambios si la Validación Falla

1. Clusterizar en espacio original con cosine distance
2. Spectral clustering en vez de K-Means
3. Gaussian Mixture Models con covariance `tied`
4. Incrementar `max_samples` de 5,000 a 10,000+
5. Cambiar embedding model a `intfloat/multilingual-e5-large`

---

## 5. Pipeline de Anomalías

### Arquitectura

```
Features extraídas (section_extracted_features)
        +
Features estructurales (has_*, len_*, tok_* de step4/step6)
        +
Features económicas (dncp.licitaciones: monto, plazo, categoria)
        +
Features temporales (año, mes, día de semana)
        ↓
  Preprocesamiento (log1p, RobustScaler, PCA opcional)
        ↓
  Modelos:
    - Isolation Forest (primary)
    - LOF (local context)
    - Autoencoder (deep anomalies)
        ↓
  Score de anomalía por licitación
        ↓
  Reporte: top-N anomalías por título, por año, por categoría
```

### Archivos a Crear

| Archivo | Propósito |
|---------|-----------|
| `src/etl/extractors/anomaly_extractor.py` | Query features desde DB + features estructurales |
| `src/etl/transformers/anomaly_transformer.py` | Preprocesamiento, detección, scoring |
| `src/etl/loaders/anomaly_loader.py` | Reportes, export CSV |
| `src/pipelines/anomaly_detection_pipeline.py` | Orquestación |
| `scripts/run_anomaly_detection_pipeline.py` | CLI |

### Métricas y Scoring

```python
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor

# Isolation Forest
iso_forest = IsolationForest(
    n_estimators=200,
    contamination=0.03,
    random_state=42,
    n_jobs=-1,
)
anomaly_scores = iso_forest.fit_predict(X)
anomaly_score = iso_forest.score_samples(X)  # lower = more anomalous

# LOF (complementario)
lof = LocalOutlierFactor(
    n_neighbors=20,
    contamination=0.03,
    novelty=False,
)
lof_scores = lof.fit_predict(X)
```

### Estrategia de Reporte

```python
# Top-N por título
for title in titles:
    mask = df["title_normalized"] == title
    title_df = df[mask].nsmallest(20, "anomaly_score")
    # Mostrar: nro_licitacion, features anómalas, score

# Evolución temporal
yearly = df.groupby("year")["anomaly_score"].mean()

# Por categoría
cat_anomalies = df.groupby("category_id").agg(
    {"anomaly_score": "mean", "nro_licitacion": "count"}
)
```

---

## 6. Mejoras Técnicas Pendientes

| # | Mejora | Archivo | Líneas | Prioridad |
|:-:|--------|---------|:------:|:---------:|
| 1 | Refactor: extraer `EXTRACTION_PROMPT_TEMPLATE` a `prompt_utils.py` | Nuevo archivo | ~50 | 🔴 |
| 2 | Refactor: extraer `TITULO_DESCRIPCION` a `prompt_utils.py` | Nuevo archivo | ~20 | 🔴 |
| 3 | Refactor: extraer `build_extraction_prompt` a `prompt_utils.py` | Nuevo archivo | ~20 | 🔴 |
| 4 | Usar `response_format: json_schema` en extraction prompt | `section_extraction_transformer.py` | ~5 | 🔴 |
| 5 | Caché de extracciones por `(nro_licitacion, title, schema_version)` | `section_extraction_loader.py` | ~30 | 🟡 |
| 6 | Asignación por similaridad vectorial (evitar LLM si el cluster ya se procesó) | `section_extraction_transformer.py` | ~80 | 🟡 |
| 7 | Reglas + LLM híbrido (regex para campos binarios simples) | `section_extraction_transformer.py` | ~50 | 🟡 |
| 8 | Incrementar `max_samples` default de 5,000 a 10,000 | `run_section_clustering_pipeline.py` | 1 | 🟢 |
| 9 | Probar otros embedding models (E5 multilingual, bge-m3) | `section_clustering_transformer.py` | — | 🟢 |
| 10 | Eliminar `hdbscan` de `requirements.txt` (ya no se usa) | `requirements.txt` | 1 | 🟢 |

---

## 7. Refactor Sugerido: `prompt_utils.py`

Crear `src/etl/transformers/prompt_utils.py`:

```python
"""Shared prompt templates and utilities for extraction prompts."""

EXTRACTION_PROMPT_TEMPLATE = """Eres un extractor de datos estructurados...
CONTEXTO: Sección "{titulo}"...
...
RESPUESTA (solo JSON):"""

TITULO_DESCRIPCION = {
    "fraude y corrupcion": "Contiene referencias...",
    # ... todos los 10 títulos
}

def build_extraction_prompt(title: str, schema: dict) -> str:
    """Build extraction prompt from title and schema dict."""
    esquema_lines = []
    for field_name, field_def in schema.items():
        ftype = field_def.get("type", "binary")
        fdesc = field_def.get("description", "")
        esquema_lines.append(f'  "{field_name}": ({ftype}) {fdesc}')
    esquema_str = "\n".join(esquema_lines)
    desc = TITULO_DESCRIPCION.get(title, "")
    return EXTRACTION_PROMPT_TEMPLATE.format(
        titulo=title,
        titulo_descripcion=desc,
        esquema_str=esquema_str,
        texto="{texto}",
    )
```

Luego actualizar:
- `section_clustering_transformer.py`: importar en vez de definir localmente
- `update_schemas.py`: importar, eliminar duplicados
- `section_extraction_transformer.py`: importar
- `import_chat_results.py`: importar

---

## 8. Línea de Tiempo

```
Semana 1:
  Día 1-2: Enviar prompts a web LLMs (fraude, retiro, audiencia)
  Día 3:   Implementar import_chat_results.py + refactor prompt_utils.py
  Día 4-5: Implementar extraction pipeline (extractor + transformer)
  
Semana 2:
  Día 1-2: Implementar pipeline (loader + pipeline + CLI)
  Día 3:   Probar con 1 título pequeño (idioma: 31K secciones, max 100)
  Día 4:   Ejecutar extracción masiva (local u OpenRouter)
  Día 5:   Validar resultados, corregir errores

Semana 3:
  Día 1-2: Validación de clusters (silhouette original, stability)
  Día 3-4: Pipeline de anomalías (primer prototipo)
  Día 5:   Reporte inicial de anomalías

Semana 4+:
  Iterar: refinar schemas → re-extraer → re-detectar anomalías
  Agregar features económicas (join con dncp.licitaciones)
  Dashboard o reporte automatizado
```

---

## Referencias Rápidas

| Para | Ver archivo |
|------|-------------|
| Código del clustering transformer | `src/etl/transformers/section_clustering_transformer.py` (1117 lines) |
| Código del LLM provider | `src/etl/transformers/llm_provider.py` (147 lines) |
| Código del extractor | `src/etl/extractors/section_clustering_extractor.py` (84 lines) |
| Código del loader | `src/etl/loaders/section_clustering_loader.py` (261 lines) |
| CLI del clustering | `scripts/run_section_clustering_pipeline.py` (132 lines) |
| CLI de update schemas | `scripts/update_schemas.py` (450 lines) |
| Plan del extraction runner | `docs/readme_pipeline_extraction_runner.md` (218 lines) |
| Plan actual (este doc) | `docs/plan_actual.md` |
| Feature selection v2 | `informe_features_v2.md` (161 lines) |
| Estrategias de features | `readme_estrategias_analise_feature_db.md` (369 lines) |
| Reporte clustering inicial | `REPORTE_CLUSTERING_Y_ESQUEMAS.md` (461 lines) |
| Decisiones de clustering | `reporte_clustering_decisiones.md` (270 lines) |
| Config de DB | `config/settings.py` (42 lines) |
| Variables de entorno | `.env` (no subir a git) |
| Dependencias | `requirements.txt` |
| Logging | `src/utils/logging_utils.py` |
| Error handler | `src/utils/error_handler.py` |
