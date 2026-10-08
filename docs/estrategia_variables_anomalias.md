# Estrategia: qué variables atacar, con qué método, y dónde invertir horas humanas (08-oct-2026)

> Insumos: informatividad medida en BD para las 710 features (`scripts/varall` → `informatividad_features.csv`,
> `docs/varianza_campos.md`), sustento académico ya mapeado en `docs/07_top_variables_enriquecido.md`
> (Ferwerda 2016, Fazekas 2016/2020, EPJ 2022, OECD, WB, Azevedo/Benford) y resultados del benchmark
> (`docs/ablacion_gt.md`, `docs/pseudo_gt_varianza.md`). Las fichas de `docs/especificacion_campos.md` se mantienen.

## 0. Idea central

El objetivo es detectar anomalías, no extraer campos. Una variable solo sirve si **(a) varía entre pliegos**,
**(b) tiene sustento** como red flag y **(c) se puede obtener con precisión conocida**. Separar los campos por
**método de obtención** cambia dónde hace falta benchmark y GT humano:

| Método | Variables | Benchmark LLM | GT humano | Estado |
|---|---|---|---|---|
| SQL sobre OCDS (económicas) | ~20 | no | no (datos estructurados) | **listas** |
| Agregados estructurales del PDF | ~20 de 584 | no | no | **listas** (reducir redundancia) |
| Regex puro con variación | 6–7 | no | validación fuera de muestra chica | listas con regex 69 campos |
| Reglas deterministas (ex-hybrid) | 5 | no | **sí, fuera de muestra** (reglas se ajustaron in-sample) | staging listo |
| **LLM** (Gemma-4-12B QAT) | **≤6** | **sí, solo aquí** | **sí, foco de las horas** | pendiente prompt corregido |
| Binarios de plantilla | 3 + 27 flags | no | no | reclasificar |

Consecuencia: el **Pipeline 3 puede empezar hoy** con las capas 1-3 (≈45 variables, ninguna depende del LLM).
El trabajo de LLM/GT queda en el camino crítico solo para ≤6 variables de peso académico medio.

## 1. Clasificación de las 710 features por informatividad (BD, 31.321 docs)

| Capa | Cols | Constantes (modal ≥99%) | Informativas (entropía ≥1, cobertura ≥50%) | Lectura |
|---|---|---|---|---|
| `document_economic_features` | 45 | 11 | 21 | Núcleo del detector. Las 11 constantes (`garantia_porcentaje`, `costo_pliego`, `bidder_diversity`, `oferentes_por_item`, `metodo_contratacion`=open…) se descartan; `metodo_detalle` se toma de `licitaciones` |
| `document_features` (estructural) | 584 | 183 | 269 | 160 columnas `page_*`/`span_*` son constantes; `len_/tok_/word_` por título son 240 columnas casi colineales. Quedarse con ~20 agregados (`sum/avg/std_tokens`, `gini_content_length`, `title_entropy`, `total_sections`, `max_page`) + 27 `has_*` informativos |
| `document_features_v3` (pliego) | 81 | 21 | 5 (+ ~10 con variación pero baja cobertura) | 27 de 54 binarios varían (modal <90%); 16 son constantes. Numéricos: ver §2 |

## 2. Los 27 campos numéricos del pliego, uno por uno

Informatividad = proyección post-promote (`varianza_campos.md`) donde aplica. Sustento = `07_top_variables` §1-3.

### Tier P1 — Regex puro, varía, con sustento → **entran a Pipeline 3 ahora**
| Campo | Cobertura | Distintos | Entropía | Sustento | Nota |
|---|---|---|---|---|---|
| `pct_multa_diaria` | 83% | 34 | 2,79 | TI penalidades asimétricas (ALTA) | la más informativa del pliego |
| `pct_interes_mora_diario` | 86% | 34 | 2,32 | OCP punitivos (ALTA) | |
| `dias_reposicion` | 38% | 20 | 2,51 | EPJ WV share (MEDIA) | |
| `anos_experiencia_minima` | 9% | 12 | 1,70 | **Ferwerda 0,487 #1** (ALTA) | cobertura baja = la mayoría no fija años; eso también es dato |
| `pct_monto_referencia_minimo` | 69% | 18 | 1,55 | Ferwerda preferred supplier (ALTA) | |
| `num_referencias_minimo` | 1,5% | 8 | 2,27 | idem | escasa; usar como flag "exige referencias" |
| `pct_garantia_cumplimiento` | 100% | 4 | 0,75 | Ferwerda large tender 0,33 | 79% = 5%; sirve como ordinal |

### Tier P2 — Reglas deterministas (ex-hybrid), varían → **Pipeline 3 tras validar reglas fuera de muestra**
| Campo | Cobertura proy. | Distintos | Modal | Sustento | Qué falta |
|---|---|---|---|---|---|
| `dias_pago_maximo` (plazo específico) | 99% | 24 | 60 (69%) | Ferwerda 0,431 (ALTA) | **prompt/guía corregida** (ficha A1); regla 189/200 in-sample, 21/30 pseudo con criterio viejo |
| `dias_validez_garantia` | 23% | 54 | 365 (57%) | OCP garantías (MEDIA) | regla 200/200 in-sample, 22/28 fuera |
| `dias_solicitud_anticipo` | 24% | 15 | 10 (49%) | EPJ (MEDIA) | regla 25/30 fuera |
| `dias_pago_anticipo` | 18% | 16 | 10 (58%) | EPJ (MEDIA) | regla 27/30 fuera |
| `anos_experiencia_minima_tecnica` | 4% | 13 | 3 (37%) | **Ferwerda 0,487** (ALTA) | regla 29/30 fuera; escasa |

### Tier P3 — Casi constantes: la señal es el **desvío del estándar** → flag binario "no estándar"
| Campo | Modal | Fuera del modal | Sustento | Acción |
|---|---|---|---|---|
| `ratio_liquidez_minimo` | 1,0 (98%) | ~350 textos distintos exigen otro valor | EPJ firm preference (ALTA) | flag `liquidez_no_estandar` + valor; precisión medida solo en ese estrato |
| `ratio_endeudamiento_maximo` | 0,8 (97%) | ~460 | idem | idem |
| `num_componentes_formula` | 3 (94%) | ~260 | — (BAJA) | único campo 100% LLM; flag `formula_no_estandar`. Candidato a salir si el LLM no supera la modal fuera de muestra |
| `pct_diferencia_precio_bajo/alto` | 95% | 2 valores | Ferwerda 0,16 | flag |
| `num_categorias_documentos` | 98% | 5 | Ferwerda 0,431 | flag |

### Tier P4 — Plantilla pura o resuelto por regex → **fuera del benchmark LLM**
`dias_notificacion_fuerza_mayor` (7, 100%), `dias_aceptacion_rechazo_factura` (15, 100%),
`dias_presentacion_garantia` (10, 100%), `pct_anticipo` (vacío) → binario presencia / descartar.
`num_documentos_pago` (6/7, regex 200/200) → binario `exige_certificado_laboral`.
`num_pasos_verificacion` (6/7/8, regex 200/200) → marcador de versión de plantilla, no de riesgo.
`pct_monto_referencia_tecnica` (147 docs, 0,5%) → mantener como flag; demasiado escaso para magnitud.

## 3. Qué pasa con el LLM

Con la separación anterior el LLM decide **solo** donde regex y regla no aplican dentro de Tier P2/P3:
`num_componentes_formula` (13.836 docs) y los residuales de los 5 campos P2 (~2–3% cada uno). En la
ablación GT el LLM decidió el 8,7% de ítems; con las reglas al frente, el modelo mueve ≈1–2 pp del total.

**Benchmark reducido** (lo único que vale re-correr):
- **Modelos**: Gemma-4-12B QAT W4A16 (vLLM, principal), Qwen3-14B-AWQ (alternativa vLLM), Gemma-4-26B-A4B
  (control GGUF). Los otros 11 ya están medidos y no vuelven.
- **Campos**: los 6 de Tier P2 + fórmula (P3). Nada de plantilla.
- **Referencia**: GT fuera de muestra con el **estrato no-modal sobremuestreado** (es donde el LLM y las
  reglas pueden fallar y donde está la señal de anomalía).
- **Prompt**: con las guías reescritas desde las fichas (cambia el fingerprint → se re-corre).
- Costo: ~40 min de GPU.

## 4. Dónde van las horas humanas (y dónde no)

| Tarea | Ítems | Horas | Sustituye a |
|---|---|---|---|
| Calibración usuario vs Opus, **solo Tier P2 + fórmula + ratios no-modal** | 60 (6 campos × 10) | ~1 | `calibracion_v1.csv` (88) |
| GT fuera de muestra para P2 + fórmula, no-modal sobremuestreado | ~240 (6 × 40) | 3–4 | `aceptacion_v2` (643) + parte del estratificado-75 |
| Revisión del estratificado-75 | 76 | 2 | se mantiene (es la única validación humana existente de reglas) |
| Anotar campos plantilla, pasos, documentos | 0 | 0 | — |

Total ≈ 6–7 h humanas en vez de las ~15 h que implicaba anotar las planillas v1/v2 completas.

## 5. Secuencia propuesta

1. **Aprobar las fichas** de `especificacion_campos.md` (A1-A6, B1-B3, C1-C3). → reescribir `extraction_hint`
   de los campos P2/P3 desde las fichas; corregir mis 9 anotaciones de pago_maximo en el pseudo-GT.
2. **Regenerar la calibración** a 60 ítems (P2 + fórmula + ratios no-modal). Anotamos los dos → κ por campo.
3. **Re-correr** los 3 modelos sobre GT-12 y pseudo-GT con el prompt nuevo (~40 min GPU) → tabla final de
   modelos para la tesis (ya con el criterio unificado).
4. **Pipeline 3, fase 1, en paralelo**: Isolation Forest sobre Tier P1 + económicas + ~20 estructurales + 27
   binarios variables (≈75 variables, sin LLM). Esto produce resultados de anomalías mientras se termina el GT.
5. **GT fuera de muestra** (240 ítems) → precisión por campo P2 en estrato no-modal → promote por campo a v3
   → Pipeline 3, fase 2, agrega los campos P2/P3 con su precisión conocida.
6. Tesis: el capítulo de extracción reporta precisión **por campo y por estrato**, y declara los campos
   plantilla como binarios; el capítulo de anomalías usa las variables de §2 con su sustento de `07_top_variables`.

## 6. Decisiones que quedan para el usuario
- [ ] Aceptar la reclasificación P4 → binarios (saca 5 campos del benchmark de precisión).
- [ ] Aceptar que `num_componentes_formula` siga solo si supera la baseline modal fuera de muestra; si no, flag.
- [ ] Aprobar fichas A1-A6/B1-B3 (sin eso no se reescriben prompts ni se anota).
- [ ] Confirmar los 3 modelos del benchmark reducido.
- [ ] Autorizar arrancar Pipeline 3 fase 1 con las ~75 variables sin LLM.
