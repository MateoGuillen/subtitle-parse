# Tareas pendientes del usuario (no automatizables)

> Creado 06-oct-2026 (sesión 24). Marcar `[x]` al completar. Contexto: `docs/plan_correccion_auditoria.md` §9.

## 0. Decisiones de estrategia (leer `docs/estrategia_variables_anomalias.md` §6 y `docs/especificacion_campos.md`)
- [x] Aprobar fichas A1-A6, B1-B3, C1-C3 (08-oct-2026, ficha por ficha con evidencia de BD).
- [x] Aceptar reclasificación P4 → binarios y P3 → flags "no estándar" (B3 reemplazado por `tipo_indice_reajuste`).
- [ ] Aprobar las correcciones al GT de agosto que proponga el paso I4 (`especificacion_campos.md`, plan de implementación).
- [ ] Anotar la calibración v2 (60 ítems, paso I5).
- [ ] Confirmar benchmark reducido: Gemma-4-12B QAT (vLLM) + Qwen3-14B-AWQ + Gemma-4-26B-A4B, solo campos P2 + fórmula.
- [ ] Autorizar Pipeline 3 fase 1 con las ~75 variables sin LLM.

## 1. Anotación humana (F5) — bloquea el promote (REVISADA: ver estrategia §4, ~6-7 h en total)
- [ ] **`data/processed/llm_benchmark/planillas/planilla_estratificado_75.csv`** — PRIMERO. Única validación
      fuera de muestra de las reglas/regex (se desarrollaron sobre `gt_por_campo`; sus 200/200 son optimistas).
- [ ] **`planilla_iaa_25.csv`** — idealmente con un segundo anotador (tutor/colega) → κ de Cohen.
- [ ] **`aceptacion_v2_3fe595a6.csv`** (643 ítems, ciega) — instrucciones en `planillas/LEEME_aceptacion_v2.md`.
      No abrir el archivo `_clave.csv`.
- [ ] (opcional) `planilla_audito_a47_12.csv` — sustancia de las etiquetas de agosto.
- [ ] Al terminar: `/root/vllm-env/bin/python scripts/validar_aceptacion.py aceptacion_v2_3fe595a6.csv`
      (y `--segundo <copia>.csv` si hubo segundo anotador).

## 2. Decisión de promote (F7)
- [ ] Elegir los campos que pasan a `dncp.document_features_v3` según el validador
      (LCB ≥ 0,85 → PROMOTE; < 0,70 → HOLD). `num_componentes_formula` es candidato a HOLD.
- [ ] Decidir si se cambia el orden del pipeline a regla → regex (+1,3 pp in-sample; confirmar con el estratificado-75).
- [ ] Ensayo: `/root/vllm-env/bin/python scripts/promote_fallback.py --run-id llmfb-qwen3-8b-fp8-3fe595a69e69bfd4 --fields <campos>`
      (dry-run). Aplicar: agregar `--apply`. Revertir: `--rollback --apply`.

## 2b. Modelo del fallback (decisión tuya; evidencia en `endurecimiento_benchmark_plan.md` §9.8)
- [ ] Confirmar modelo: recomendado **Gemma-4-12B QAT W4A16 (vLLM)** — empata con el mejor en GT humano y en
      el pseudo-GT, corre en el runner actual (agregar su config a `MODELS` de `run_llm_fallback.py`), 6 ítems/s
      (~1,5 h para re-correr `num_componentes_formula`). Alternativas: Gemma-4-26B-A4B GGUF, Qwen3-14B-AWQ.
- [ ] Revisar la definición de `dias_pago_maximo` (regla "primer N días" vs guía "sin exceder N días tras
      factura": cambia el ganador entre regla y LLM; ver `docs/pseudo_gt_varianza.md` §D.3).
- [ ] Re-correr `num_componentes_formula` (y residuales) con el modelo elegido antes de promoverlo.

## 3. Seguridad y publicación
- [ ] **Rotar/revocar** credenciales ya publicadas en GitHub (repo público, commits 57ad572 / 09e9f50 / f477042):
      API key de Google (`AIzaSy…`), usuario/contraseña del servidor `144.33.29.61`, contraseña de BD local.
- [ ] Configurar autenticación GitHub en WSL (`gh auth login` o token) para poder hacer push.
- [ ] Push de `feature/benchmark-models` y del tag `pre-auditoria-s23` (pedirlo explícitamente).
