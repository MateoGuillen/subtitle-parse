# Capa 2 — Tablas de features pre-computadas: origen, cálculo, decisión y glosario

> 1 fila = 1 licitación (`nro_licitacion`). Las 3 tablas evitan releer jamás los 2.7M de
> secciones ni los joins OCDS: todo lo que ranking y anomalías necesitan ya está pivoteado.
> Scripts: `scripts/run_document_features_pipeline.py`, `run_document_economic_features_pipeline.py`,
> `run_document_features_v2_pipeline.py` (+ `run_document_features_v2_llm_pipeline.py` para el fallback).

## 1. `document_features` — estructurales (31.321 × 587)

**De dónde viene**: solo de `pliegos_secciones` (título + largo del texto por sección). Ningún dato económico ni semántico.
**Cómo se calcula**: pivot por título — por cada uno de los ~80 títulos se agregan 7 métricas + 12 agregados globales del documento. Todo CPU (pandas), ~3 min.
**Por qué así**: el ranking de títulos y las anomalías necesitan "forma del pliego" (¿qué trae? ¿cuánto?) sin parsear texto. El pivot ancho es la forma más barata de servirlo.
**Decisión registrada**: en el loader de ranking se cargan solo ~245 columnas relevantes (prefijos `has_/len_/tok_` + ids) por memoria.

| Prefijo | Cant | Significado | Ejemplo |
|---|---|---|---|
| `has_*` | 81 | ¿el pliego trae ese título? (0/1) | `has_condicion_de_participacion` |
| `len_*` | 81 | caracteres totales del título | `len_condicion_de_participacion` |
| `tok_*` | 81 | tokens totales del título | `tok_condicion_de_participacion` |
| `count_*` | 80 | nº de secciones del título | `count_oferta_tecnica` |
| `word_*` | 81 | palabras (post-stemming) | `word_forma_de_pago` |
| `span_*` | 80 | dispersión (primera↔última aparición) | `span_garantia` |
| `page_range_*` | 80 | rango de páginas que ocupa | `page_range_anexos` |
| agregados | ~20 | `total_sections`, `unique_titles`, `avg/max/sum/std_content_length`, `gini_content_length` (desigualdad de largos), `title_entropy` (diversidad temática) | `gini_content_length` |
| ids | 3 | `nro_licitacion`, `year`, `category_id` | — |

## 2. `document_economic_features` — económicas (29.811 × 46)

**De dónde viene**: JOINs SQL sobre OCDS (`licitaciones`, `adjudicaciones`, `oferentes`, `contratos`, `pagos_contrato`, `items_licitacion`, `enmiendas_contrato`, `consultas_llamado`, `protestas`).
**Cómo se calcula**: extractor SQL con CTEs y window functions (frecuencia del ganador, `is_repeat_winner`, `bidder_diversity`) + transformer con 12 campos derivados (ratios, flags) + loader con `NaT→None`. ~segundos.
**Por qué así**: cada columna es una variable con nombre y literatura (no un pivot): son directamente las red flags de FMI/Benford/Porter-Zona/Fazekas del top de variables.

| Grupo | Cant | Significado | Ejemplo |
|---|---|---|---|
| núcleo subasta | ~8 | oferentes, montos, método, duración | `es_unico_oferente`, `monto_estimado`, `overbudget_ratio`, `duracion_oferta_dias` |
| colusión (ventana) | 4 | historial del ganador | `winner_total_contracts`, `winner_category_frequency`, `is_repeat_winner`, `bidder_diversity` |
| ejecución | ~8 | enmiendas, pagos, consultas, protestas | `n_enmiendas`, `pago_vs_contrato_ratio`, `n_consultas`, `n_protestas` |
| descriptivos | resto | convocante, fechas, estado, criterios | `metodo_detalle`, `fecha_publicacion`, `contrato_estado` |
| ids | 1 | `nro_licitacion` | — |

**Limitaciones conocidas**: `bidder_diversity`=0 en todo (bug `cantidad_items` en origen); sin montos por oferta (solo ganador) → screens de distribución en versión degradada.

## 3. `document_features_v2` — contenido del pliego (31.321 × 108)

**De dónde viene**: `pliegos_secciones` pasada por los 81 extractores del schema (`master_schemas_v2.json`).
**Cómo se calcula**: 69 campos por regex directo + 12 hybrid (regex, y si falla → LLM). Rebuild completo DROP+CREATE en ~66s. Columnas por campo: valor + `{campo}_conf` (1.0 seguro / 0.3 absurdo / 0.0 no-encontrado) + `{campo}_source` (regex/llm/absent, solo hybrid).
**Por qué así**: cada columna es un dato concreto del pliego ("¿cuántos días para pagar el anticipo?") con su semáforo de confianza; `conf<0.8` alimenta el fallback LLM sin re-leer nada.
**Decisión registrada (Sesión 21)**: precisión > recall — 4 regex reescritos con anclaje por cláusula; lo dudoso devuelve `None` (conf 0.0) y lo resuelve Llama-8B-FP8. Gate v2 sostiene los 4 (pago 100%, solicitud 91.4%, validez 72.7%, exp 93.3%).

| Prefijo | Cant | Significado | Ejemplo |
|---|---|---|---|
| `binary_*` | 54+1 | flags 0/1 (¿exige? ¿permite? ¿incluye?) | `binary_solicita_explicacion_precios` |
| `dias_*` | 14 | plazos en días | `dias_pago_anticipo` |
| `pct_*` | 8 | porcentajes | `pct_garantia_cumplimiento` |
| `num_*` | 9 | conteos | `num_referencias_minimo` |
| `anos_*` | 3 | años de experiencia | `anos_experiencia_minima` |
| `ratio_*` | 4 | ratios financieros | `ratio_liquidez_minimo` |
| `categorical_*` | 1 | categoría (tipo de garantía) | `categorical_tipo_garantia` |
| `*_conf` | 12 | confianza por campo hybrid | `dias_pago_anticipo_conf` |
| `*_source` | 12 | procedencia (regex/llm/absent) | `dias_pago_anticipo_source` |
| ids | 3 | `nro_licitacion`, `year`, `category_id` | — |

Total: 81 valores + 12 conf + 12 source + 3 ids = **108**. (81 lógicos: 69 regex + 12 hybrid.)

> ⚠ **Auditoría sesión 23 (H4.1)**: en v2 **ningún campo numérico tiene NULL**: la ausencia se
> codifica como `0` en los 26 campos int/float (no solo en los 12 hybrid), mientras que en
> 5.266 etiquetas humanas de esos campos no hay ni un 0. Además `{campo}_source='absent'`
> mezcla dos cosas: con conf 1.0 = el doc no tiene la sección; con conf 0.0 = regex no-match.
> **No usar v2 para análisis**: usar `document_features_v3`.

## 4. `document_features_v3` — contenido con NULL ≠ 0 (31.321 × 108 + 26 `_status` + `llm_model`)

**De dónde viene**: copia de v2 generada por `scripts/build_features_v3.py` (SQL en
`sql/s23_features_v3_null_semantics.sql`); v2 queda intacta. Correcciones por campo con
`scripts/recompute_regex_field.py` (con historia) y promociones del fallback con
`scripts/promote_fallback.py` (con historia y rollback en `document_features_v2_history`).
**Semántica (26 campos numéricos)**:

| Columna | Valores | Significado |
|---|---|---|
| `{campo}` | número o **NULL** | NULL = sin valor (nunca 0) |
| `{campo}_status` | `extraido` | hay valor |
| | `sin_seccion` | el pliego no tiene sección del título del campo |
| | `no_extraido` | la sección existe pero no se obtuvo valor (= población del fallback) |
| | `ausente` | regla o LLM dictaminaron que el pliego no fija el valor (lo escribe el promote) |
| `{campo}_source` (12 hybrid) | `regex` / `rule` / `llm` / `absent` | quién produjo el valor; `rule` = regla determinista de `extraction_postprocess.RULES` (antes se etiquetaba como `llm`) |
| `{campo}_conf` (12 hybrid) | regex 1.0 · rule 0.9 · llm 0.7 · NULL | **convencional por fuente, no es una probabilidad** (H4.4) |
| `llm_model` | `{source}@{run_id}` | trazabilidad de la última promoción (una columna por fila, compartida entre campos) |

**Semántica de `dias_pago_maximo` (H4.6)**: la regla toma el **primer** "N días" del texto
(criterio del GT humano), no el máximo; se conserva el nombre de columna para no romper
consumidores. **`num_documentos_pago` (H4.3)**: corregido en v3 a conteo por nombre del
catálogo de 7 documentos genéricos DNCP (antes devolvía el índice del primer ítem).

## Mapa en una línea

`pliegos_secciones` → **forma** (`document_features`, 587) + OCDS → **plata** (`document_economic_features`, 46) + pliegos × schema → **contenido** (`document_features_v2`, 108) → top-20 variables → anomalías.
