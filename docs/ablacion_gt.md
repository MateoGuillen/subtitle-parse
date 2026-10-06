# Ablación de 3+2 brazos contra GT humano (F4.5/F6.1)

> `scripts/ablacion_gt.py` (GPU, crudos) + `scripts/ablacion_resumen.py` (CPU). 12 campos hybrid × 200 textos con etiqueta humana (`gt_por_campo`).
> ⚠ **regex** y **regla** fueron desarrollados sobre estos mismos GT: son cotas **in-sample** (optimistas). **llm** es la única métrica limpia del modelo. Validación fuera de muestra de reglas/regex: F5 (estratificado-75).

## Qwen3-8B-FP8

| campo | n | modal | regex | regla | llm | pipeline |
|---|---|---|---|---|---|---|
| anos_experiencia_minima_tecnica | 200 | 160 | 178 | 199 | 180 | 198 |
| dias_aceptacion_rechazo_factura | 200 | 198 | 2 | 200 | 200 | 198 |
| dias_notificacion_fuerza_mayor | 200 | 189 | 11 | 200 | 200 | 200 |
| dias_pago_anticipo | 200 | 98 | 119 | 200 | 190 | 200 |
| dias_pago_maximo | 200 | 104 | 66 | 189 | 122 | 187 |
| dias_solicitud_anticipo | 200 | 100 | 158 | 200 | 171 | 195 |
| dias_validez_garantia | 200 | 128 | 166 | 200 | 166 | 181 |
| num_componentes_formula | 200 | 141 | 5 | 5 | 95 | 95 |
| num_documentos_pago | 200 | 102 | 200 | 5 | 9 | 200 |
| num_pasos_verificacion | 200 | 100 | 200 | 3 | 159 | 200 |
| ratio_endeudamiento_maximo | 200 | 166 | 88 | 199 | 188 | 198 |
| ratio_liquidez_minimo | 200 | 143 | 59 | 200 | 120 | 198 |
| **TOTAL** | 2400 | **1629** (67.9%, IC [0.66,0.70]) | **1252** (52.2%, IC [0.50,0.54]) | **1800** (75.0%, IC [0.73,0.77]) | **1800** (75.0%, IC [0.73,0.77]) | **2250** (93.8%, IC [0.93,0.95]) |

## Llama-3.1-8B-FP8

| campo | n | modal | regex | regla | llm | pipeline |
|---|---|---|---|---|---|---|
| anos_experiencia_minima_tecnica | 200 | 160 | 178 | 199 | 164 | 198 |
| dias_aceptacion_rechazo_factura | 200 | 198 | 2 | 200 | 70 | 198 |
| dias_notificacion_fuerza_mayor | 200 | 189 | 11 | 200 | 79 | 200 |
| dias_pago_anticipo | 200 | 98 | 119 | 200 | 103 | 200 |
| dias_pago_maximo | 200 | 104 | 66 | 189 | 32 | 187 |
| dias_solicitud_anticipo | 200 | 100 | 158 | 200 | 134 | 195 |
| dias_validez_garantia | 200 | 128 | 166 | 200 | 150 | 181 |
| num_componentes_formula | 200 | 141 | 5 | 5 | 5 | 5 |
| num_documentos_pago | 200 | 102 | 200 | 5 | 5 | 200 |
| num_pasos_verificacion | 200 | 100 | 200 | 3 | 3 | 200 |
| ratio_endeudamiento_maximo | 200 | 166 | 88 | 199 | 53 | 198 |
| ratio_liquidez_minimo | 200 | 143 | 59 | 200 | 30 | 198 |
| **TOTAL** | 2400 | **1629** (67.9%, IC [0.66,0.70]) | **1252** (52.2%, IC [0.50,0.54]) | **1800** (75.0%, IC [0.73,0.77]) | **828** (34.5%, IC [0.33,0.36]) | **2160** (90.0%, IC [0.89,0.91]) |

