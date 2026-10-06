# Varianza por campo (F4.6, auditoría H4.2)

> Generado por `scripts/varianza_campos.py` sobre `dncp.document_features_v3` (31321 docs, NULL≠0) + proyección del run `llmfb-qwen3-8b-fp8-3fe595a69e69bfd4` (estado post-promote).
> `binario`: el valor modal es ≥99% de los extraídos → como magnitud no discrimina; usar como indicador de cláusula estándar presente.

| campo | método | extraídos | cobertura | distintos | modal (frac) | entropía | textos sección | fuentes | clase |
|---|---|---|---|---|---|---|---|---|---|
| pct_garantia_cumplimiento | regex | 31319 | 100.0% | 4 | 10 (78.6%) | 0.751 | 177 | {} | baja variabilidad |
| num_documentos_pago | hybrid | 30990 | 98.9% | 2 | 6 (87.1%) | 0.554 | 23218 | {"regex": 30990} | baja variabilidad |
| num_componentes_formula | hybrid | 13836 | 44.2% | 12 | 3 (94.0%) | 0.414 | 17510 | {} | baja variabilidad |
| pct_diferencia_precio_bajo | regex | 27164 | 86.7% | 2 | 25 (95.1%) | 0.283 | 418 | {} | baja variabilidad |
| pct_diferencia_precio_alto | regex | 27164 | 86.7% | 2 | 15 (95.1%) | 0.283 | 418 | {} | baja variabilidad |
| ratio_endeudamiento_maximo | hybrid | 25605 | 81.8% | 24 | 0.8 (97.4%) | 0.262 | 19351 | {"regex": 171} | baja variabilidad |
| ratio_liquidez_minimo | hybrid | 25982 | 83.0% | 24 | 1 (97.8%) | 0.218 | 19351 | {"regex": 31} | baja variabilidad |
| num_categorias_documentos | regex | 30523 | 97.5% | 5 | 2 (98.0%) | 0.155 | 670 | {} | baja variabilidad |
| dias_aceptacion_rechazo_factura | hybrid | 30992 | 99.0% | 3 | 15 (100.0%) | 0.001 | 23218 | {"regex": 2} | binario (cláusula estándar) |
| dias_notificacion_fuerza_mayor | hybrid | 29831 | 95.2% | 1 | 7 (100.0%) | -0.0 | 559 | {} | binario (cláusula estándar) |
| dias_presentacion_garantia | regex | 31319 | 100.0% | 1 | 10 (100.0%) | -0.0 | 177 | {} | binario (cláusula estándar) |
| pct_multa_diaria | regex | 25968 | 82.9% | 34 | 0.5 (32.2%) | 2.794 | 638 | {} | magnitud |
| dias_validez_garantia | hybrid | 7298 | 23.3% | 54 | 365 (57.1%) | 2.527 | 7050 | {"regex": 5724} | magnitud |
| dias_reposicion | regex | 11924 | 38.1% | 20 | 5 (30.0%) | 2.507 | 11584 | {} | magnitud |
| pct_interes_mora_diario | regex | 26933 | 86.0% | 34 | 0.01 (44.5%) | 2.319 | 1579 | {} | magnitud |
| num_referencias_minimo | regex | 459 | 1.5% | 8 | 3 (34.4%) | 2.27 | 25202 | {} | magnitud |
| pct_monto_referencia_tecnica | regex | 147 | 0.5% | 9 | 50 (37.4%) | 2.242 | 17547 | {} | magnitud |
| anos_experiencia_minima_tecnica | hybrid | 1284 | 4.1% | 13 | 3 (36.8%) | 2.175 | 17547 | {"regex": 689} | magnitud |
| dias_solicitud_anticipo | hybrid | 7403 | 23.6% | 15 | 10 (49.3%) | 1.991 | 6530 | {"regex": 5965} | magnitud |
| dias_pago_anticipo | hybrid | 5685 | 18.1% | 16 | 10 (57.6%) | 1.989 | 6530 | {"regex": 1057} | magnitud |
| anos_experiencia_minima | regex | 2681 | 8.6% | 12 | 3 (60.2%) | 1.695 | 25202 | {} | magnitud |
| dias_pago_maximo | hybrid | 30992 | 99.0% | 24 | 60 (69.1%) | 1.67 | 23218 | {"regex": 1484} | magnitud |
| num_referencias_tecnicas_minimo | regex | 70 | 0.2% | 5 | 3 (60.0%) | 1.602 | 17547 | {} | magnitud |
| pct_monto_referencia_minimo | regex | 21493 | 68.6% | 18 | 50 (66.2%) | 1.545 | 25202 | {} | magnitud |
| num_pasos_verificacion | hybrid | 30851 | 98.5% | 3 | 6 (53.1%) | 1.393 | 773 | {"regex": 30851} | magnitud |
| pct_anticipo | regex | 0 | 0.0% | 0 | nan (nan%) | 0.0 | 7 | {} | sin datos |
