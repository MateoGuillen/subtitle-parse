# Agentes de opencode — Proyecto `subtitle-parse`

Directorio: `.opencode/agents/`

## 1. `documentador-tutores` — Material de comunicación académica

**Propósito**: Genera todo el material para presentar la tesis a los tutores (correo ejecutivo, resumen PPT por diapositiva, flujo de datos con gráficas, respuestas a preguntas típicas de reunión, y detalle del setup de IA/benchmark).

**Modo**: `all` — se usa directamente o via comando `/tutores`.

**Archivos que produce**: `docs/presentacion_tutores/` (correo_tutores.md, ppt_tutores.md + pptx, flujo_datos.md + gráficas, preguntas_reunion.md, setup_ia_benchmark.md).

**Cómo invocarlo**: `@documentador-tutores <instrucciones>` o via comando `/tutores`.

**Dependencias**: matplotlib 3.9.4, pandas 2.3.3 (gráficas); python-pptx 1.0.2 (PPT binario opcional).

---

## 2. `muestreador-gt` — Muestreo de variables de pliego desde la BD

**Propósito**: PRIMER paso del pipeline de gold standard. Dado un conjunto de variables de pliego (campo, title_slug, tipo, extraction_hint, fuentes académicas), muestrea N filas por variable de la BD `dncp.pliegos_secciones` y produce un CSV listo para anotar.

**Modo**: `all`.

**Entrada**: Lista de variables (definiciones en Python/JSON) o referencias a variables nuevas.

**Salida**: `data/processed/alolab/gold_standard_pliego_nuevo_{N}variables.csv` (con texto + prompt + columnas vacías para ground_truth, confianza, es_revisar).

**Reproducibilidad**: `random.seed(42)`, `DISTINCT ON (nro_licitacion)`, preserva filas ya anotadas si el CSV objetivo existe.

**Script base**: `scripts/preparar_gt_nuevo_7variables.py`.

---

## 3. `anotador-pseudo-gt` — Anotación pseudo-GT con modelos de frontera

**Propósito**: SEGUNDO paso del pipeline de gold standard. Toma el CSV de `muestreador-gt` y produce un CSV completo con ground_truth, confianza, es_revisar y nota. Usa extracción determinista (regex) para casos claros, y el propio modelo de frontera (DeepSeek v4 flash) para casos REVISAR, con fallback opcional a APIs de frontera (B.AI, OpenRouter, Tokengo).

**Modo**: `all`.

**Entrada**: CSV de `muestreador-gt` (con columnas text, prompt, campo, etc.).

**Salida**: `data/processed/alolab/gold_standard_pliego_nuevo_{N}variables_{modelo}.csv` (con ground_truth, confianza, es_revisar, nota).

**Script base**: `scripts/anotar_pseudo_gt.py` (regex), `scripts/preparar_gt_nuevo_7variables.py` (referencia).

**APIs disponibles**: B.AI (deepseek-v4-flash), DeepSeek, OpenRouter, Tokengo (todas con keys en `.env`).

---

## Pipeline: muestreador → anotador

```
variables (definición)
  → [@muestreador-gt] → CSV base
  → [@anotador-pseudo-gt] → CSV anotado (pseudo-GT)
  → [@documentador-tutores] → material de presentación
```

Los agentes se encadenan produciendo y consumiendo CSVs en `data/processed/alolab/`. El contrato entre muestreador y anotador son las columnas: `nro_licitacion, campo, variable, title_slug, title_original, text, word_count, page, prompt, db_value, llm_raw, llm_value, ground_truth, nota, prioridad, fuente_academica, confianza, es_revisar`.