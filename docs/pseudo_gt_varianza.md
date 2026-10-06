# Varianza del benchmark: GT humano vs pseudo-GT Opus 5.5 (fuera de muestra)

> `scripts/generar_muestra_pseudo_gt.py` (muestra pre-registrada, sha256 en el repo) + anotación ciega de Claude Opus 5.5 (`pseudo_gt_opus/anotaciones.json`, commiteada antes de evaluar) + `scripts/ablacion_gt.py --set pseudo` + `scripts/pseudo_resumen.py`.
> 344 ítems de documentos que NO están en `gt_por_campo`; 4 ambiguos excluidos → 340 evaluados. ⚠ El pseudo-GT es de un LLM (segundo anotador independiente), no humano.

## A. Ranking de modelos (LLM solo) — Spearman ρ = **0.91**, Kendall τ = **0.80** (ρ solo hybrid = 0.91)

| modelo | GT humano hybrid-12 | rank | pseudo-GT 13 campos | rank | pseudo-GT hybrid |
|---|---|---|---|---|---|
| Gemma-4-12B (GGUF) | 1997/2400 (83.2%) | 1 | 312/340 (91.8%) [0.88,0.94] | 1 | 282/310 (91.0%) |
| Gemma-4-26B-A4B Q4_K_XL (GGUF) | 1993/2400 (83.0%) | 2 | 311/340 (91.5%) [0.88,0.94] | 2.5 | 281/310 (90.6%) |
| Gemma-4-12B QAT W4A16 (vLLM) | 1991/2400 (83.0%) | 3 | 311/340 (91.5%) [0.88,0.94] | 2.5 | 281/310 (90.6%) |
| Qwen3.8-27B UD-Q3_K_XL (GGUF) | 1977/2400 (82.4%) | 4 | 304/340 (89.4%) [0.86,0.92] | 5 | 274/310 (88.4%) |
| Qwen3-14B-AWQ | 1956/2400 (81.5%) | 5 | 280/340 (82.4%) [0.78,0.86] | 7.5 | 252/310 (81.3%) |
| Qwen3.6-35B-A3B IQ3_XXS (GGUF) | 1939/2400 (80.8%) | 6 | 280/340 (82.4%) [0.78,0.86] | 7.5 | 250/310 (80.6%) |
| MiMo-V2.6-9B Q8 (GGUF) | 1913/2400 (79.7%) | 7 | 305/340 (89.7%) [0.86,0.93] | 4 | 278/310 (89.7%) |
| Qwen3.5-9B Q8 (GGUF) | 1842/2400 (76.8%) | 8 | 274/340 (80.6%) [0.76,0.84] | 9 | 244/310 (78.7%) |
| Qwen3-8B-FP8 | 1800/2400 (75.0%) | 9 | 257/340 (75.6%) [0.71,0.80] | 11 | 228/310 (73.5%) |
| Ministral-3-14B (GGUF) | 1794/2400 (74.8%) | 10 | 292/340 (85.9%) [0.82,0.89] | 6 | 262/310 (84.5%) |
| Ling-3.0-tiny Q8 (GGUF) | 1707/2400 (71.1%) | 11 | 258/340 (75.9%) [0.71,0.80] | 10 | 230/310 (74.2%) |
| Ministral-3-8B Q8 (GGUF) | 1695/2400 (70.6%) | 12 | 254/340 (74.7%) [0.70,0.79] | 12 | 224/310 (72.3%) |
| EXAONE-3.5-7.8B-AWQ | 1411/2400 (58.8%) | 13 | 194/340 (57.1%) [0.52,0.62] | 13 | 166/310 (53.5%) |
| Llama-3.1-8B-FP8 | 828/2400 (34.5%) | 14 | 126/340 (37.1%) [0.32,0.42] | 14 | 97/310 (31.3%) |

## B. Capas deterministas FUERA DE MUESTRA (respuesta a H2.2) — pipeline con Gemma-4-12B (GGUF)

| campo | n | regex prod | regla (si aplica) | pipeline regex→regla→LLM | pipeline regla→regex→LLM |
|---|---|---|---|---|---|
| anos_experiencia_minima_tecnica | 30 | 29/30 | 29/30 | 29/30 | 29/30 |
| dias_aceptacion_rechazo_factura | 29 | 0/29 | 29/29 | 29/29 | 29/29 |
| dias_notificacion_fuerza_mayor | 6 | 0/6 | 6/6 | 6/6 | 6/6 |
| dias_pago_anticipo | 30 | 13/30 | 27/30 | 27/30 | 27/30 |
| dias_pago_maximo | 30 | 1/30 | 21/30 | 21/30 | 21/30 |
| dias_solicitud_anticipo | 30 | 23/30 | 25/30 | 26/30 | 25/30 |
| dias_validez_garantia | 28 | 20/28 | 22/28 | 23/28 | 22/28 |
| num_componentes_formula | 29 | 0/29 | — | 26/29 | 26/29 |
| num_documentos_pago | 30 | 30/30 | — | 30/30 | 30/30 |
| num_pasos_verificacion | 8 | 8/8 | — | 8/8 | 8/8 |
| pct_monto_referencia_tecnica | 30 | 30/30 | — | 30/30 | 30/30 |
| ratio_endeudamiento_maximo | 30 | 1/30 | 30/30 | 30/30 | 30/30 |
| ratio_liquidez_minimo | 30 | 0/30 | 30/30 | 30/30 | 30/30 |
| **TOTAL** | 340 | 155 (45.6%) | 219/243 | **315** (92.6%) | **313** (92.1%) |

## C. LLM solo por campo (pseudo-GT)

| campo | n | Gemma-4-12B (GGUF) | Gemma-4-12B QAT W4A16 (vLLM) | Gemma-4-26B-A4B Q4_K_XL (GGUF) | MiMo-V2.6-9B Q8 (GGUF) | Qwen3.8-27B UD-Q3_K_XL (GGUF) | Ministral-3-14B (GGUF) | Qwen3-14B-AWQ | Qwen3.6-35B-A3B IQ3_XXS (GGUF) | Qwen3.5-9B Q8 (GGUF) | Ling-3.0-tiny Q8 (GGUF) | Qwen3-8B-FP8 | Ministral-3-8B Q8 (GGUF) | EXAONE-3.5-7.8B-AWQ | Llama-3.1-8B-FP8 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| anos_experiencia_minima_tecnica | 30 | 30 | 30 | 30 | 30 | 30 | 30 | 29 | 30 | 30 | 30 | 29 | 30 | 30 | 30 |
| dias_aceptacion_rechazo_factura | 29 | 29 | 29 | 29 | 29 | 29 | 29 | 29 | 29 | 29 | 29 | 29 | 29 | 29 | 10 |
| dias_notificacion_fuerza_mayor | 6 | 6 | 6 | 6 | 6 | 6 | 6 | 6 | 6 | 6 | 6 | 6 | 6 | 6 | 4 |
| dias_pago_anticipo | 30 | 27 | 27 | 27 | 27 | 27 | 26 | 26 | 27 | 27 | 23 | 27 | 27 | 13 | 10 |
| dias_pago_maximo | 30 | 30 | 30 | 30 | 30 | 30 | 30 | 30 | 30 | 30 | 30 | 29 | 30 | 30 | 10 |
| dias_solicitud_anticipo | 30 | 22 | 22 | 22 | 22 | 22 | 22 | 21 | 22 | 20 | 20 | 22 | 21 | 18 | 13 |
| dias_validez_garantia | 28 | 22 | 22 | 23 | 25 | 22 | 20 | 21 | 22 | 19 | 21 | 19 | 15 | 19 | 11 |
| num_componentes_formula | 29 | 26 | 25 | 25 | 23 | 18 | 16 | 23 | 19 | 17 | 21 | 14 | 11 | 1 | 0 |
| num_documentos_pago | 30 | 27 | 27 | 27 | 25 | 27 | 24 | 3 | 2 | 28 | 5 | 4 | 4 | 10 | 0 |
| num_pasos_verificacion | 8 | 8 | 8 | 7 | 7 | 8 | 4 | 7 | 8 | 8 | 8 | 5 | 4 | 7 | 2 |
| pct_monto_referencia_tecnica | 30 | 30 | 30 | 30 | 27 | 30 | 30 | 28 | 30 | 30 | 28 | 29 | 30 | 28 | 29 |
| ratio_endeudamiento_maximo | 30 | 25 | 25 | 25 | 25 | 25 | 25 | 27 | 25 | 2 | 11 | 27 | 17 | 1 | 7 |
| ratio_liquidez_minimo | 30 | 30 | 30 | 30 | 29 | 30 | 30 | 30 | 30 | 28 | 26 | 17 | 30 | 2 | 0 |

## D. Interpretación

1. **El ranking de modelos es robusto al anotador y a la muestra** (ρ = 0,91; τ = 0,80): los 3 primeros
   (Gemma-4-12B GGUF, Gemma-4-12B QAT vLLM, Gemma-4-26B-A4B) y los 3 últimos (Ministral-3-8B, EXAONE,
   Llama-3.1-8B-FP8) coinciden en ambas referencias. Los cambios de posición en el medio (MiMo 7→4,
   Ministral-3-14B 10→6) están dentro de los IC del pseudo-GT (n=340).
2. **Las reglas deterministas se sostienen fuera de muestra, pero no al 100%**: 219/243 (90,1%) donde
   aplican, frente a 100% in-sample en `gt_por_campo` (H2.2 cuantificado: ~10 pp de optimismo).
   El regex de producción baja a 45,6%. El pipeline completo llega a 92,6% (93,8% en GT humano).
3. **Varianza de CRITERIO, no de modelo — `dias_pago_maximo`**: la regla acierta 21/30 contra este
   pseudo-GT pero 189/200 contra el GT humano, y los LLM al revés (30/30 aquí). Causa: el GT humano de
   agosto usó "primer N días del texto"; este pseudo-GT siguió la guía del campo ("sin exceder N días
   después de la presentación de la factura", = 60 en la plantilla). Los 9 casos de diferencia son pliegos
   con un plazo particular previo (5, 10, 15, 30 días…). Lección para la tesis: la definición operativa
   del campo cambia el ganador entre regla y LLM; hay que fijarla antes de anotar.
4. **Campos que discriminan modelos fuera de muestra**: `num_documentos_pago` (Gemma 27/30 vs Qwen3-14B
   3/30), `num_componentes_formula` (Gemma 26/29 vs Qwen3-8B 14/29), `ratio_endeudamiento_maximo`
   (Qwen3.5-9B 2/30). En campos-plantilla (aceptación, fuerza mayor, pct ausente) todos empatan.
5. **Por qué el pseudo-GT da cifras más altas que el GT humano** (≈91% vs 83% para el mejor): la muestra
   fuera de muestra tiene más textos plantilla y más ausencias (`anos_experiencia` y `pct` 100% sin valor),
   que son fáciles. Las cifras absolutas no son comparables entre referencias; los rankings sí.
