# Tareas pendientes del usuario (no automatizables)

> Creado 06-oct-2026 (sesión 24). Marcar `[x]` al completar. Contexto: `docs/plan_correccion_auditoria.md` §9.

## 1. Anotación humana (F5) — bloquea el promote
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

## 3. Seguridad y publicación
- [ ] **Rotar/revocar** credenciales ya publicadas en GitHub (repo público, commits 57ad572 / 09e9f50 / f477042):
      API key de Google (`AIzaSy…`), usuario/contraseña del servidor `144.33.29.61`, contraseña de BD local.
- [ ] Configurar autenticación GitHub en WSL (`gh auth login` o token) para poder hacer push.
- [ ] Push de `feature/benchmark-models` y del tag `pre-auditoria-s23` (pedirlo explícitamente).
