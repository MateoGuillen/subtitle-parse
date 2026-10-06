# Cómo auditar `aceptacion_v2_3fe595a6.csv` (plan de corrección F5)

**Reemplaza** a `planilla_aceptacion_{A,B,C}` (v1), que NO se auditan (auditoría sesión 23:
n efectivo ≈ 1 en algunos campos, sin estratos, valor del sistema visible).

## Reglas
1. **No abras** `aceptacion_v2_3fe595a6_clave.csv` (tiene el valor del sistema). La planilla es
   **ciega**: juzgás el texto, no al sistema.
2. Por cada fila leé `descripcion` (qué pide el campo) y `texto` (la sección del pliego), y completá
   **una** de estas dos columnas:
   - `valor_auditor`: el número correcto según el pliego (días, años, ratio, cantidad), solo el número.
   - `estado`: `no_fija` si el pliego no fija ese valor (plantilla vacía, "[indicar]", no aparece),
     o `ambiguo` si hay varios valores plausibles y no se puede decidir (explicá en `notas`).
3. `notas` es libre (opcional). Si dudás entre dos valores, usá `ambiguo` y anotá ambos.
4. Unidad = **texto distinto**: cada fila representa una plantilla que puede cubrir cientos de
   pliegos idénticos. No hace falta buscar el pliego original.

## Orden sugerido (por impacto)
1. `ratio_liquidez_minimo`, `ratio_endeudamiento_maximo` (las reglas recuperaron ~25K valores c/u que v1 había descartado: es lo más importante de validar).
2. `dias_pago_maximo` (regla = "primer N días"; ¿coincide con el plazo máximo de pago?).
3. `dias_pago_anticipo`, `dias_solicitud_anticipo`, `dias_validez_garantia`, `anos_experiencia_minima_tecnica`.
4. `num_componentes_formula` (único campo decidido por el LLM; la ablación sugiere HOLD).
5. Campos plantilla (`dias_aceptacion_rechazo_factura`, `dias_notificacion_fuerza_mayor`) y censos chicos.

## Al terminar
```
/root/vllm-env/bin/python scripts/validar_aceptacion.py aceptacion_v2_3fe595a6.csv
```
Por campo: LCB95 ≥ 0,85 → PROMOTE · punto < 0,70 → HOLD · resto → JUICIO.
Si un segundo anotador completa una copia (mismo formato): `--segundo <copia>.csv` → κ de Cohen.

## Otras anotaciones pendientes (ya generadas)
- `planilla_estratificado_75.csv`: validación **fuera de muestra** de las reglas (la más importante para la tesis: las reglas se ajustaron sobre `gt_por_campo`).
- `planilla_iaa_25.csv`: acuerdo entre anotadores (κ).
- `planilla_audito_a47_12.csv`: sustancia de las etiquetas de agosto.
