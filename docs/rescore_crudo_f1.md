# F1 — Re-score del benchmark sobre salida cruda (plan de corrección, 06-oct-2026)

> Script: `scripts/rescore_crudo.py` (CPU, ~3 s, sin GPU) · Datos: `data/processed/llm_benchmark/F1_rescore_crudo.{csv,json}`, `F1_mcnemar.csv`
> Métricas: **crudo** = número literal emitido por el modelo (sin reglas); **post** = lo publicado (crudo + `extraction_postprocess.parse_response`); **null_bl** = un "modelo" que siempre responde `NULL`, pasado por el mismo postproceso.

## 1. Verificación de la auditoría (todas reproducidas exactas)

| Cifra | FP8 | AWQ | Qwen3-8B |
|---|---|---|---|
| C3 dias (239) — post (publicado) | 239 | 239 | 239 |
| C3 dias (239) — **null_bl** | **239** | **239** | **239** |
| C3 heldout (50) — null_bl | 50 | 50 | 50 |
| C3 dias (239) — **crudo** | 183 (76,6%) | 175 (73,2%) | **235 (98,3%)** |
| C3 pct (49) — post = crudo | 29 | 32 | 26 |
| C3 pct (49) — null_bl | 0 | 0 | 0 |
| Fase 5 v2 (867) — post | 758 | 754 | 696 |

## 2. Hallazgos nuevos de F1

1. **Fase 5 fuera de pct es 100% reglas.** En los 720 ítems no-pct, `post` = `null_bl` para los tres modelos: 640/720 en v2 y 418/720 en v1. Todo el ranking Fase 5 (v1 61,8% y v2 87,4%) se decide únicamente en pct (147 ítems), cuya referencia es el regex tuneado sobre ese set (cota optimista).
2. **El "dias robusto al prompt" de C4/C5 es un artefacto.** En los 120 ítems no-pct de los 150 fijos, `post` = 72 = `null_bl` para los 3 modelos en base, P1, P2, P3 y P4. En crudo sí hay sensibilidad fuerte:

| no-pct (120), crudo | base | P1 | P2 (C4) | P3 | P4 |
|---|---|---|---|---|---|
| FP8 | 67 | 69 | 56 | 38 | 38 |
| AWQ | 63 | 62 | 31 | 30 | 36 |
| QWEN | 75 | 75 | 77 | 65 | 74 |

   Qwen es el único modelo estable bajo reescritura de prompt también en dias, no solo en pct.
3. **Sobre crudo, los finalistas NO son intercambiables en dias** (McNemar exacto, Bonferroni α=0,0167):

| Par (dias 239, crudo) | b | c | p | diferencia [IC95] |
|---|---|---|---|---|
| FP8 − AWQ | 23 | 15 | 0,26 | +3,3 pp [−1,7; +8,4] |
| FP8 − Qwen | 0 | 52 | 4·10⁻¹⁶ | **−21,8 pp** [−27,0; −16,5] |
| AWQ − Qwen | 2 | 62 | 2·10⁻¹⁶ | **−25,1 pp** [−30,8; −19,4] |

   Sin validez (para descartar el efecto de meses→días): FP8 148 vs Qwen 186 de 189, p≈7·10⁻¹².
4. **pct: "indistinguibles" se reemplaza por una frase de potencia.** FP8−AWQ: 11 discordantes en n=49; la diferencia mínima detectable (poder 0,8, α=0,0167) es **≈22 pp**. Afirmación defendible: *"no se detectan diferencias en pct mayores a ~22 pp; diferencias menores no son evaluables con n=49"*.
5. **Contrapeso para Qwen**: en pct contra pseudo (147, referencia = regex tuneado) Qwen da 56 frente a 118 de FP8. Contra humano (49) la diferencia no es significativa (26 vs 29, p=0,65). La desventaja de Qwen en pct solo se ve contra la referencia contaminada.

## 3. Decisión registrada (cierre de F1)

- **Campeón sobre salida cruda: Qwen3-8B-FP8** en los campos donde el LLM decide (dias: +22 pp sobre FP8, significativo; estable bajo cambios de prompt). En pct no hay diferencia detectable contra humano.
- El veredicto v2 ("FP8 por desempate operativo") **queda invalidado**: el empate de calidad que habilitaba el desempate no existe sobre crudo. FP8 sigue siendo 1,56× más rápido (7,0 vs 4,5 req/s), pero no hay empate que desempatar.
- Implicación para F4: el run LLM nuevo usa **Qwen3-8B-FP8** (13,2 GB, entra en 16 GB), salvo que la ablación de F4.5 muestre que en los campos con regla el LLM no aporta nada; en ese caso esos campos van solo con reglas.

## 4. Extensión a TODOS los modelos probados (`scripts/rescore_todos_modelos.py`, `F1b_todos_modelos.csv`)

GT-100 humano (4 campos dias × 20 = 80 + pct 20). Crudo = número literal emitido (sin reglas).

| Modelo | dias crudo (80) | pct crudo (20) | dias publicado (post) |
|---|---|---|---|
| EXAONE-3.5-7.8B-AWQ | 79 | 20 | 78 |
| Gemma-4-12B (GGUF) | 79 | 19 | 78 |
| Qwen3-14B-AWQ | 79 | 20 | 78 |
| Qwen3.8-27B (GGUF) | 79 | 19 | 78 |
| **Qwen3-8B-FP8** | 78 | 20 | 78 |
| Ministral-3-14B (GGUF) | 78 | 19 | 78 |
| Qwen2.5-14B-AWQ | 75 | 19 | 75 |
| Qwen3-8B-AWQ | 74 | 20 | 74 |
| Mistral-7B-AWQ | 61 | 17 | 78 |
| Llama-3.1-8B-AWQ | 59 | 3 | 78 |
| **Llama-3.1-8B-FP8** | **18** | **1** | 78 |
| Granite-4.2-8B | sin crudo registrado (raw vacío en el log) | — | 78 |
| ALIA-legal-7B | run roto (score 0) | — | 0 |

Baseline siempre-NULL con el mismo postproceso: **78/80** en dias. Lecturas:
1. El "GT-100 98/100" que llevó a FP8 a la final era casi todo postproceso: FP8 sobre crudo acierta 18/80.
2. **La selección de finalistas (top-3) también estaba sesgada**: se hizo con Fase 5 (decidida solo en pct contra
   pseudo-GT de regex). Sobre crudo humano, EXAONE-3.5-7.8B-AWQ, Qwen3-14B-AWQ, Gemma-4-12B y Qwen3.8-27B
   rinden igual o mejor que Qwen3-8B-FP8 en GT-100; Llama-FP8/AWQ y Mistral-7B quedan últimos.
3. Con n=80 las diferencias entre los 6 primeros (78-79) no son significativas. **Resuelto con el retest de 7
   modelos (protocolo único, 2.600 GT): ver `endurecimiento_benchmark_plan.md` §9.8 → Gemma-4-12B recomendado.**
