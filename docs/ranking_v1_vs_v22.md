# Comparativo Ranking v1 vs v2.2

## Resumen

- v1: 80 títulos rankeados (6 estrategias, K=10 hardcoded)
- v2.2: 76 títulos rankeados (8 estrategias, K=auto-elegido)

## TOP-20 v2.2 con comparación a v1

| rank v2.2 | title | score v2.2 | rank v1 | score v1 | Δ rank | boilerplate | numeric | economic | descart |
|----------:|-------|------------:|--------:|---------:|-------:|------------:|--------:|---------:|:-------:|
| 1 | analisis_de_precios_ofertados | 0.5374 | 32 | 0.3661 | +31 | 0.691 | 1.000 | 0.796 |  |
| 2 | porcentaje_de_multas | 0.5166 | 20 | 0.3982 | +18 | 0.827 | 0.600 | 0.868 |  |
| 3 | fuerza_mayor | 0.5136 | 3 | 0.5238 | +0 | 0.494 | 0.000 | 0.540 |  |
| 4 | porcentaje_de_garantia_de_fiel_cumplimiento_de_con | 0.4895 | 30 | 0.3751 | +26 | 0.635 | 0.984 | 0.043 |  |
| 5 | aclaracion_de_las_ofertas | 0.4591 | 4 | 0.4884 | -1 | 0.477 | 0.000 | 0.043 |  |
| 6 | experiencia_requerida | 0.4555 | 46 | 0.2691 | +40 | 1.000 | 0.712 | 0.872 |  |
| 7 | plazo_de_reposicion_de_bienes | 0.4345 | 29 | 0.3754 | +22 | 0.935 | 0.104 | 0.679 |  |
| 8 | documentacion_requerida_para_la_firma_del_contrato | 0.4271 | 33 | 0.3634 | +25 | 0.772 | 0.000 | 0.324 |  |
| 9 | causales_de_terminacion_del_contrato | 0.4190 | 10 | 0.4367 | +1 | 0.517 | 0.000 | 0.531 |  |
| 10 | confidencialidad_de_la_informacion | 0.4185 | 2 | 0.5250 | -8 | 0.544 | 0.000 | 0.540 |  |
| 11 | impuestos_y_derechos | 0.4185 | 7 | 0.4539 | -4 | 0.504 | 0.000 | 0.759 |  |
| 12 | suministros_y_especificaciones_tecnicas | 0.4134 | 24 | 0.3894 | +12 | 0.745 | 0.032 | 0.736 |  |
| 13 | derechos_intelectuales | 0.4089 | 8 | 0.4467 | -5 | 0.478 | 0.000 | 0.681 |  |
| 14 | copias_de_la_oferta__cps | 0.4075 | 12 | 0.4252 | -2 | 0.499 | 0.000 | 0.405 |  |
| 15 | periodo_de_validez_de_la_garantia_de_los_bienes | 0.4004 | 19 | 0.4031 | +4 | 0.568 | 0.124 | 0.679 |  |
| 16 | capacidad_tecnica | 0.3987 | 39 | 0.3170 | +23 | 0.891 | 0.068 | 0.871 |  |
| 17 | formas_y_condiciones_de_pago | 0.3978 | 23 | 0.3900 | +6 | 0.981 | 0.276 | 0.213 |  |
| 18 | plan_de_entrega_de_los_bienes | 0.3964 | 41 | 0.2973 | +23 | 0.937 | 0.236 | 0.757 |  |
| 19 | responsabilidad_del_proveedor | 0.3961 | 28 | 0.3765 | +9 | 0.514 | 0.000 | 0.426 |  |
| 20 | otras_causales_de_terminacion_del_contrato | 0.3932 | 17 | 0.4060 | -3 | 0.144 | 0.000 | 0.531 |  |

## Ganadores (mayor subida v1 → v2.2)

| title | v1 | v2.2 | Δ |
|-------|---:|-----:|--:|
| experiencia_requerida | #46 | #6 | +40 |
| analisis_de_precios_ofertados | #32 | #1 | +31 |
| porcentaje_de_garantia_de_fiel_cumplimiento_de_con | #30 | #4 | +26 |
| documentacion_requerida_para_la_firma_del_contrato | #33 | #8 | +25 |
| plan_de_entrega_de_los_bienes | #41 | #18 | +23 |
| capacidad_tecnica | #39 | #16 | +23 |
| plazo_de_reposicion_de_bienes | #29 | #7 | +22 |
| porcentaje_de_multas | #20 | #2 | +18 |
| criterios_de_desempate_de_ofertas | #38 | #22 | +16 |
| sistema_de_presentacion_de_ofertas | #67 | #54 | +13 |
| requisitos_documentales_para_la_evaluacion_de_la_e | #50 | #37 | +13 |
| obligatoriedad_de_declarar_informacion_del_persona | #59 | #47 | +12 |
| formalizacion_de_la_contratacion | #70 | #58 | +12 |
| identificacion_de_la_unidad_solicitante_y_justific | #71 | #59 | +12 |
| resolucion_de_conflictos_a_traves_del_arbitraje | #69 | #57 | +12 |

## Perdedores (mayor bajada v1 → v2.2)

| title | v1 | v2.2 | Δ |
|-------|---:|-----:|--:|
| fraude_y_corrupcion | #1 | #23 | -22 |
| retiro_sustitucion_y_modificacion_de_las_ofertas | #9 | #31 | -22 |
| formato_y_firma_de_la_oferta | #6 | #27 | -21 |
| capacidad_financiera | #21 | #42 | -21 |
| audiencia_informativa | #13 | #34 | -21 |
| inspecciones_y_pruebas | #15 | #33 | -18 |
| tiempo_de_funcionamiento_de_los_bienes | #18 | #35 | -17 |
| interpretacion | #5 | #21 | -16 |
| requisitos_de_calificacion | #55 | #68 | -13 |
| idioma_de_la_oferta | #16 | #29 | -13 |
| documentos_de_la_oferta | #57 | #69 | -12 |
| planos_y_disenos | #14 | #26 | -12 |
| subcontratacion | #64 | #75 | -11 |
| periodo_de_validez_de_la_garantia_de_mantenimiento | #56 | #66 | -10 |
| disconformidad_errores_y_omisiones | #43 | #53 | -10 |