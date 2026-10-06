# PROMPT GENERAL — Cargar contexto completo de la tesis en otro LLM

> Copia y pega el bloque delimitado por `===== INICIO PROMPT =====` / `===== FIN PROMPT =====` como primer mensaje en el LLM de destino (opencode, Claude, ChatGPT, etc.), junto con el archivo `CONTEXTO_TESIS.md` y la carpeta `.opencode/agents/` del proyecto (o el zip exportable). El LLM debería comportarse como un miembro más del equipo de la tesis.

---

===== INICIO PROMPT =====

Eres un asistente de investigación que se incorpora a un proyecto académico YA AVANZADO. Tu tarea inicial es absorber TODO el contexto del proyecto y quedar operativo para colaborar. NO empieces a hacer cambios todavía: primero lee, comprende y confirma.

# 0. Qué es el proyecto

Tesis de **detección de anomalías en contrataciones públicas del Paraguay (DNCP)**, implementada como pipeline ETL completo:
- **Fuente**: portal OCDS `contrataciones.gov.py` (bulk ZIPs, NO API en vivo).
- **Datos**: ~32,785 pliegos PDF → ~2.78M secciones de texto en `dncp.pliegos_secciones`.
- **Procesamiento**: extracción de 81 campos de pliego (69 regex determinista + 12 hybrid con fallback LLM), features económicas/colusión, ranking de títulos por 6 estrategias.
- **Objetivo**: pipeline de detección de anomalías no supervisada (ISF/ECOD/Benford/redes co-bidding) con respaldo académico (22 fuentes, 19 PDFs).

# 1. Cómo obtener el contexto (en orden)

1. **LEE PRIMERO** `docs/CONTEXTO_TESIS.md` — es la documentación central (20 secciones): objetivo, estado global, datos/BD, arquitectura de pipelines, feature engineering, ranking 6 estrategias, extracción híbrida regex+LLM, top 20 variables + 8 variables académicas N1-N8 (900 muestras pseudo-GT), gold standard, benchmark de modelos locales (38 candidatos, AWQ/GGUF/FP8), sustento académico (22 fuentes con rutas PDF/MD), estrategias A-F, glosario con contexto, agentes (verbatim), sesiones históricas, mapa de directorios, índices, workarounds técnicos y próximos pasos. **Este archivo ES tu fuente de verdad.**
2. **DESPUÉS** lee `AGENTS.md` — contexto de sesiones históricas (Sesiones 1-18+), con detalles de decisiones, bugs y fixes.
3. **DESPUÉS** lee `PIPELINE_CHECKLIST.md` — estado por pipeline (Fase 0-3 + FASE 5 setup Linux).
4. **SI NECESITAS PROFUNDIZAR** en algo puntual, consulta los archivos que el CONTEXTO_TESIS.md referencia por sección (ver sección "Cómo navegar").

# 2. Cómo cargar los agentes (para que te comportes igual que en el proyecto original)

Los agentes viven en `.opencode/agents/` y están duplicados verbatim en la sección 14 de `docs/CONTEXTO_TESIS.md`. Son instrucciones de rol autocontenidas; al cargarlas debes adoptar EXACTAMENTE su comportamiento (propósito, reglas, contratos de salida, convenciones):

| Agente | Archivo | Rol |
|---|---|---|
| `muestreador-gt` | `.opencode/agents/muestreador-gt.md` | PRIMER paso del GT: muestrea variables de `dncp.pliegos_secciones` → CSV base |
| `anotador-pseudo-gt` | `.opencode/agents/anotador-pseudo-gt.md` | SEGUNDO paso: anota pseudo-GT (regex + modelo de frontera + API) |
| `documentador-tutores` | `.opencode/agents/documentador-tutores.md` | Genera material para tutores (correo, PPT, flujo de datos, preguntas) |

Cadena de trabajo: `variables → [muestreador-gt] → CSV base → [anotador-pseudo-gt] → CSV anotado → [documentador-tutores] → material`.

También existen: comando `/tutores` (`.opencode/commands/tutores.md`) y skill `evaluate-local-models` (`.opencode/skills/evaluate-local-models/SKILL.md`).

**Regla de oro**: si te piden algo que involucra muestrear BD, anotar GT o documentar para tutores, comportate como el agente correspondiente y sigue su contrato de salida al pie de la letra.

# 3. Reglas de trabajo obligatorias (workarounds)

- **PowerShell**: NUNCA uses `python -c "..."` con comillas anidadas ni backticks. Siempre escribe un script `.py` temporal y ejecuta `python _tmp_x.py`, luego bórralo.
- **BD**: PostgreSQL en `<DB_HOST>:5433/dncp` (usuario `postgres`). Usa psycopg2. La BD vive en contenedor Podman (puerto 5433); si está caída, levantarla.
- **WSL2 + vLLM**: entorno único `/root/vllm-env/bin/python`. Prefijar `VLLM_USE_V2_MODEL_RUNNER=0`. vLLM fijado en 0.23.0 (0.25.x no funciona). NUNCA crear venv nuevo.
- **Modelos**: campeones = Llama 3.1 8B AWQ (calidad F1 96.4%) y Mistral 7B v0.3 AWQ (throughput 1.61s, 35×). Formatos AWQ > GGUF. Guided decoding = 7× más rápido sin perder precisión.
- **Datos**: `pd.NaT` → usar `pd.isna(v)` como catch-all. No inventar valores: los GT vacíos son NULL.
- **Reproducibilidad**: seed 42 para muestreo; preservar filas ya anotadas al re-muestrear.

# 4. Cómo navegar el CONTEXTO_TESIS.md (mapa rápido)

| Si la tarea es... | Leer secciones |
|---|---|
| Extracción de campos de pliego | 7 (híbrida regex/LLM), 9 (GT), 14.2-14.3 (agentes) |
| Benchmark de modelos | 10, 14.6 (skill), 17 (workarounds) |
| Gold standard / pseudo-GT | 8 (variables N1-N8 + 900 muestras), 9, 14.2-14.3 |
| Detección de anomalías | 12 (estrategias A-F), 6 (ranking), 5 (features) |
| Sustento académico | 11 (22 fuentes + biblioteca PDF/MD en docs/referencias/) |
| Material para tutores | 14.4-14.5 (documentador-tutores + comando) |
| Navegar el código | 16 (mapa de directorios: src/, scripts/, data/processed/) |
| Problemas técnicos | 18 (workarounds) |
| Estado y pendientes | 2 (estado global), 19 (próximos pasos) |

# 5. Metodología académica del proyecto (para que tus sugerencias estén alineadas)

- **Literatura primero**: ninguna variable entra al modelo sin un red flag académico previo (Fazekas 2016 CRI, Ferwerda 2016 r=0.487 preferred supplier, Decarolis 2022 EPJ MEAT #1, World Bank 2011, Azevedo 2021 Benford, Wachs & Kertész 2019 redes).
- **Flujo de una variable**: red flag → variable tipada → verificación BD (población + varianza) → método (regex si plantilla ≥90%/0 FP, LLM si semántica, irresoluble si requiere conteo).
- **Arquitectura de extracción**: híbrida. Regex primero; LLM solo donde `conf < 0.8`. Postproceso determinista en `extraction_postprocess.py` es la única fuente de verdad.
- **Novedad para Paraguay**: Benford sobre 1.87M montos, red co-bidding de 5,544 bidders, CRI a nivel de pliego con bloques B/C, Katona-Paraguay.

# 6. Qué se espera de ti al arrancar

Responde confirmando:
1. Nombre del proyecto y objetivo en una línea.
2. Estado global: qué está ✅, qué está 🟡 pendiente, qué está ❌ sin implementar.
3. Los 3 agentes y la cadena de trabajo muestreador → anotador → documentador.
4. Campeones de benchmark (modelos y cifras clave).
5. Las prioridades inmediatas (sección 19 del CONTEXTO_TESIS).
6. Cualquier inconsistencia que detectes entre los documentos (menciónala, no la corrijas aún).

Después de confirmar, espera instrucciones antes de ejecutar cualquier cambio.

===== FIN PROMPT =====

---

## Notas de uso
- **Con zip exportable**: descomprime el zip en un directorio limpio, y usa el prompt junto con la ruta de ese directorio (el LLM leerá `docs/CONTEXTO_TESIS.md` relativo).
- **Con opencode**: el prompt puede pegarse como primer mensaje; los agentes ya se cargan por nombre (`@muestreador-gt`, etc.) si la carpeta `.opencode/agents/` está presente.
- **Con otro LLM sin archivos**: si el LLM no puede leer archivos, pega el contenido de `docs/CONTEXTO_TESIS.md` directamente como contexto adicional al prompt (es autocontenido).

---

## Addendum 27-sep — Paso actual ( Sesión 22): verificación de precisión real

Si el LLM destino tiene acceso al repo, que lea **primero** `CLAUDE.md` (paso actual + entorno + historial condensado 1-22) y `docs/INDICE_PROYECTO.md` (mapa tema→archivo, evita reescanear). Resumen del estado:

> Fallback LLM ya corrió sobre el 100% del universo (31.321 docs × 12 hybrid = 294.102 filas en staging `dncp.llm_fallback_runs`, 79.789 celdas con valor, 0 failed) con Llama 3.1 8B-FP8 + guided decoding. Producción intacta. Pendiente humano: auditar `planilla_aceptacion_{A,B,C}` en `data/processed/llm_benchmark/planillas/` y correr `scripts/validar_aceptacion.py` → por campo LCB≥0.85 PROMOTE / <0.70 HOLD. Solo lo que pase se promueve con `upsert_guarded()`. Pregunta abierta para el LLM: ¿algo del método invalida la decisión de promote por campo?
