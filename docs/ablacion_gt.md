# Ablación contra GT humano — protocolo único para 6 modelos (F4.5/F6.1)

> `scripts/ablacion_gt.py` (GPU) + `scripts/ablacion_resumen.py` (CPU). Mismo prompt de producción y decodificación restringida número|NULL (vLLM: xgrammar; GGUF: llama-server con GBNF equivalente).
> ⚠ **regex** y **regla** se desarrollaron sobre estos GT: cotas **in-sample**. **LLM solo** es la única métrica limpia del modelo.

## B. Modelos — LLM solo (sin reglas)

McNemar exacto vs Qwen3-8B-FP8; α Bonferroni = 0,05/6.

| modelo | hybrid-12 (2.400) | C3 dias (GT∩regex) | C3 pct | McNemar hybrid-12 vs ref (b/c, p) |
|---|---|---|---|---|
| Qwen3-8B-FP8 | 1800/2400 (75.0%) [0.73,0.77] | 205/239 (85.8%) [0.81,0.90] | 13/49 (26.5%) [0.16,0.40] | — |
| Llama-3.1-8B-FP8 | 828/2400 (34.5%) [0.33,0.36] | 73/239 (30.5%) [0.25,0.37] | 2/49 (4.1%) [0.01,0.14] | 18/990, p=1.1e-265 * |
| EXAONE-3.5-7.8B-AWQ | 1411/2400 (58.8%) [0.57,0.61] | 128/239 (53.6%) [0.47,0.60] | 1/49 (2.0%) [0.00,0.11] | 72/461, p=1.9e-70 * |
| Qwen3-14B-AWQ | 1956/2400 (81.5%) [0.80,0.83] | 203/239 (84.9%) [0.80,0.89] | 21/49 (42.9%) [0.30,0.57] | 204/48, p=4.5e-24 * |
| Gemma-4-12B (GGUF) | 1997/2400 (83.2%) [0.82,0.85] | 190/239 (79.5%) [0.74,0.84] | 33/49 (67.3%) [0.53,0.79] | 259/62, p=9e-30 * |
| Ministral-3-14B (GGUF) | 1794/2400 (74.8%) [0.73,0.76] | 174/239 (72.8%) [0.67,0.78] | 9/49 (18.4%) [0.10,0.31] | 169/175, p=0.79 |
| Qwen3.8-27B UD-Q3_K_XL (GGUF) | 1977/2400 (82.4%) [0.81,0.84] | 199/239 (83.3%) [0.78,0.87] | 3/49 (6.1%) [0.02,0.17] | 250/73, p=8e-24 * |

`*` = significativo tras Bonferroni.

## A. Brazos por modelo (12 campos hybrid, 2.400 ítems)

| modelo | modal | regex (in-sample) | regla (in-sample) | **LLM solo** | pipeline regex→regla→LLM | pipeline regla→regex→LLM | errores |
|---|---|---|---|---|---|---|---|
| Qwen3-8B-FP8 | 1629 | 1252 | 1800 | **1800** (75.0%) | 2251 (93.8%) | 2283 (95.1%) | 0 |
| Llama-3.1-8B-FP8 | 1629 | 1252 | 1800 | **828** (34.5%) | 2160 (90.0%) | 2192 (91.3%) | 0 |
| EXAONE-3.5-7.8B-AWQ | 1629 | 1252 | 1800 | **1411** (58.8%) | 2162 (90.1%) | 2194 (91.4%) | 0 |
| Qwen3-14B-AWQ | 1629 | 1252 | 1800 | **1956** (81.5%) | 2300 (95.8%) | 2332 (97.2%) | 0 |
| Gemma-4-12B (GGUF) | 1629 | 1252 | 1800 | **1997** (83.2%) | 2323 (96.8%) | 2355 (98.1%) | 0 |
| Ministral-3-14B (GGUF) | 1629 | 1252 | 1800 | **1794** (74.8%) | 2238 (93.2%) | 2270 (94.6%) | 0 |
| Qwen3.8-27B UD-Q3_K_XL (GGUF) | 1629 | 1252 | 1800 | **1977** (82.4%) | 2272 (94.7%) | 2304 (96.0%) | 0 |

## C. LLM solo por campo

| campo | modal | Qwen3-8B-FP8 | Llama-3.1-8B-FP8 | EXAONE-3.5-7.8B-AWQ | Qwen3-14B-AWQ | Gemma-4-12B (GGUF) | Ministral-3-14B (GGUF) | Qwen3.8-27B UD-Q3_K_XL (GGUF) |
|---|---|---|---|---|---|---|---|---|
| anos_experiencia_minima_tecnica | 160 | 180 | 164 | 170 | 175 | 175 | 176 | 179 |
| dias_aceptacion_rechazo_factura | 198 | 200 | 70 | 200 | 200 | 200 | 200 | 200 |
| dias_notificacion_fuerza_mayor | 189 | 200 | 79 | 197 | 200 | 200 | 200 | 200 |
| dias_pago_anticipo | 98 | 190 | 103 | 121 | 190 | 189 | 179 | 191 |
| dias_pago_maximo | 104 | 121 | 32 | 104 | 104 | 104 | 105 | 104 |
| dias_solicitud_anticipo | 100 | 171 | 134 | 172 | 166 | 171 | 170 | 170 |
| dias_validez_garantia | 128 | 166 | 150 | 168 | 179 | 190 | 167 | 192 |
| num_componentes_formula | 141 | 96 | 5 | 7 | 145 | 168 | 83 | 117 |
| num_documentos_pago | 102 | 9 | 5 | 16 | 15 | 27 | 22 | 49 |
| num_pasos_verificacion | 100 | 159 | 3 | 192 | 200 | 200 | 125 | 200 |
| ratio_endeudamiento_maximo | 166 | 188 | 53 | 28 | 184 | 175 | 175 | 175 |
| ratio_liquidez_minimo | 143 | 120 | 30 | 36 | 198 | 198 | 192 | 200 |
| pct_monto_referencia_tecnica | 151 | 156 | 153 | 150 | 165 | 183 | 160 | 154 |
