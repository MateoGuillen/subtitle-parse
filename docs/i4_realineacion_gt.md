# I4 — Re-alineación del GT con la especificación v1.0 (propuesta, 08-oct-2026)

> Generado por `scripts/generar_i4_realineacion.py`. **Nada se modificó**: el GT de agosto y el pseudo-GT
> Opus siguen intactos hasta que el usuario apruebe. Planilla para revisar/aprobar:
> `data/processed/llm_benchmark/planillas/i4_realineacion.csv` (columnas `aprobar` = s/n, `valor_final` si corrige, `nota_usuario`).
> Los residuales que la regla deja al LLM no se proponen (su valor GT se mantiene).

## Resumen por campo

| Campo | Ref. | Ítems | Difieren | Con decisión de ficha | A revisar |
|---|---|---|---|---|---|
| anos_experiencia_minima_tecnica | gt_agosto | 200 | 17 | 17 | 0 |
| dias_pago_anticipo | gt_agosto | 200 | 0 | 0 | 0 |
| dias_pago_maximo | gt_agosto | 200 | 15 | 15 | 0 |
| dias_solicitud_anticipo | gt_agosto | 200 | 0 | 0 | 0 |
| dias_validez_garantia | gt_agosto | 200 | 36 | 36 | 0 |
| pct_monto_referencia_tecnica | gt_agosto | 200 | 11 | 11 | 0 |
| ratio_endeudamiento_maximo | gt_agosto | 200 | 1 | 0 | 1 |
| ratio_liquidez_minimo | gt_agosto | 200 | 0 | 0 | 0 |
| anos_experiencia_minima_tecnica | pseudo_opus | 30 | 0 | 0 | 0 |
| dias_pago_anticipo | pseudo_opus | 30 | 1 | 1 | 0 |
| dias_pago_maximo | pseudo_opus | 30 | 4 | 4 | 0 |
| dias_solicitud_anticipo | pseudo_opus | 30 | 0 | 0 | 0 |
| dias_validez_garantia | pseudo_opus | 28 | 0 | 0 | 0 |
| pct_monto_referencia_tecnica | pseudo_opus | 30 | 0 | 0 | 0 |
| ratio_endeudamiento_maximo | pseudo_opus | 30 | 0 | 0 | 0 |
| ratio_liquidez_minimo | pseudo_opus | 30 | 0 | 0 | 0 |

## Por decisión (con un ejemplo cada una)

### `A5-antiguedad` — 4 ítems
A5: la antigüedad en el rubro / existencia legal cuenta como experiencia de la empresa. Por referencia: gt_agosto=4.
- Ej. `anos_experiencia_minima_tecnica` nro 423360: actual **nan** → propuesto **3.0** · …- 2022). (Dicho documento deberá estar debidamente sellado y firmado por el responsable que lo emitió). b. La Empresa oferente deberá contar con una antigüedad mínima de 3 años en el mercado, proveyendo servicio y bienes similares a lo solicitado en el presente llam

### `A5-maximo` — 1 ítems
A5: empresa > personal y, dentro del nivel, el máximo. Por referencia: gt_agosto=1.
- Ej. `anos_experiencia_minima_tecnica` nro 431683: actual **4.0** → propuesto **7.0** · …fensa y Ciberseguridad Estratégica Exigido Arquitecto de Software Graduado o con malla curricular concluida de la carrera de Licenciatura o Ingeniería en Informática, de 7 años de experiencia demostrable con el título Universitario con la fecha de expedición. Exigid

### `A5-personal` — 12 ítems
A5: experiencia del personal ('ejercicio de la profesión', 'desde el título', antigüedad en el plantel). Por referencia: gt_agosto=12.
- Ej. `anos_experiencia_minima_tecnica` nro 475962: actual **nan** → propuesto **3.0** · …su totalidad deberán estar inscriptos en el Instituto de Previsión Social, de los cuales como mínimo 5 (cinco) personas deberán ser estables con una antigüedad mínima de 3 años, para lo cual, el oferente deberá presentar copia autenticada de las constancias de entra

### `A1-mayor` — 1 ítems
A1: dos plazos de pago genuinos → el mayor. Por referencia: gt_agosto=1.
- Ej. `dias_pago_maximo` nro 471046: actual **15.0** → propuesto **30.0** · …a presentación de la factura. Otras formas y condiciones de pago al proveedor en virtud del contrato serán las siguientes: el certificado, el plazo para el pago es hasta 30 (treinta) días calendarios posterior a la presentación. Todos los pagos serán realizados a través de la

### `A1-plazo-obra` — 6 ítems
A1: plazo propio de obra ('Plazo de pago: N días desde la aprobación del certificado') > estándar 60. Por referencia: gt_agosto=6.
- Ej. `dias_pago_maximo` nro 414858: actual **60.0** → propuesto **10.0** · …os 4. Certificado de Cumplimiento Tributario 5. Constancia de Cumplimiento con la Seguridad Social 6. Formulario de Informe de Servicios Personales (FIS). Plazo de pago: 10 días, desde la aprobación del certificado de obra y la presentación de la factura. Otras forma

### `A1-trampa` — 8 ítems
A1: el GT tomó un plazo que la ficha define como trampa (no es plazo de pago). Por referencia: gt_agosto=8.
- Ej. `dias_pago_maximo` nro 449660: actual **30.0** → propuesto **60.0** · …frecuencia: …de Servicios Personales (FIS). Otras formas y condiciones de pago al proveedor en virtud del contrato serán las siguientes: A PLAZOS.- Los pagos se harán efectivos cada 30 días, dentro de los 60 días calendarios contados a partir de la presentación de las facturas r

### `A2-mayor-365` — 22 ítems
A2: plazos > 365 días se toman tal cual (el GT aplicaba un rango 1-365). Por referencia: gt_agosto=22.
- Ej. `dias_validez_garantia` nro 476210: actual **nan** → propuesto **1095.0** · …3 (tres) años contados a partir de la recepción definitiva de los bienes. Incluye: Soporte de atención de Hardware contra defectos de fábrica, Mano de Obra y Repuestos incluyendo traslado de los equipos de la oficina del cliente al proveedor y viceversa a cargo del proveedor. Si la reparación implic

### `A2-por-item` — 14 ítems
A2: plazos distintos por lote/ítem → el máximo + flag garantia_varia_por_item. Por referencia: gt_agosto=14.
- Ej. `dias_validez_garantia` nro 444955: actual **nan** → propuesto **1095.0** · …El vencimiento deberá ser igual o superior a 18 (Diez y ocho) meses como mínimo para los Insumos y una garantía mínima de 3 (tres) años para las Prótesis. Si por la naturaleza de los mismos o por necesidad de la Institución no se puede cumplir con este requisito, la recepción del producto deberá ser

### `A6-maximo` — 1 ítems
A6: varios % de referencias genuinos (no alternativos) → el máximo. Por referencia: gt_agosto=1.
- Ej. `pct_monto_referencia_tecnica` nro 459330: actual **50.0** → propuesto **60.0** · …icios de limpieza general) a instituciones públicas o privadas durante los 3 (tres) últimos años (2.021, 2.022 y 2.023), un volumen de negocio equivalente en promedio al 60% (sesenta por ciento) como mínimo del valor de su oferta (Presentar Fotocopia de contratos

### `A6-omitido` — 1 ítems
A6: el texto fija un % de referencias sobre el monto (facturas/ventas) que el GT no tomó. Por referencia: gt_agosto=1.
- Ej. `pct_monto_referencia_tecnica` nro 462134: actual **nan** → propuesto **50.0** · …rior comercialización, constancia de compra de cortes, de los últimos 3 años con copias de facturas de compra de dichos bienes, cuya sumatoria represente por lo menos el 50 % del monto máximo ofertado. · En caso de no contar con matadero propio, contrato de presta

### `A6-solvencia` — 3 ítems
A6→A8: 'solvencia/capacidad económica del N% del monto' pasa a la variable nueva pct_respaldo_financiero_monto. Por referencia: gt_agosto=3.
- Ej. `pct_monto_referencia_tecnica` nro 454048: actual **30.0** → propuesto **nan** · …n la lista de precios, en formato auditorio. Estacionamiento mínimo para 900 vehículos PARA EL LOTE N° 2 a. El oferente deberá demostrar poseer una solvencia superior al 30% del monto máximo establecido en el presente lote. Para el efecto, para demostrar poseer l

### `A6-sumatoria` — 2 ítems
A6: con alternativas, el porcentaje de la sumatoria. Por referencia: gt_agosto=2.
- Ej. `pct_monto_referencia_tecnica` nro 463795: actual **40.0** → propuesto **60.0** · …á cumplir al menos una de las siguientes 2 restricciones: La sumatoria de los montos de las referencias presentadas que cumplan estos criterios deberá ser como mínimo el 60% del monto referencial de la adquisición. Al menos una referencia de trabajo similar por e

### `A6-trampa` — 4 ítems
A6: el % no es de referencias (consorcio, stock, repuestos, personal, póliza, multa…). Por referencia: gt_agosto=4.
- Ej. `pct_monto_referencia_tecnica` nro 420144: actual **10.0** → propuesto **nan** · …DEMOSTRAR STOCK CONTABLE DE REPUESTOS PARA ATENDER EL EQUIPO OFERTADO DE POR LO MENOS 10% DEL MONTO DE LA OFERTA EL OFERENTE DEBERA POSEER AUTORIZADO DEL BIEN OFERTADO.

### `revisar` — 1 ítems
sin decisión de ficha que la explique: decide el usuario. Por referencia: gt_agosto=1.
- Ej. `ratio_endeudamiento_maximo` nro 463117: actual **nan** → propuesto **nan** · …Capacidad Financiera Con el objetivo de calificar la situación financiera del oferente, se considerarán los siguientes índices: Contribuyente de IRE GENERAL. Deberán cumplir con el siguiente parámetro: Ratio de Liquidez (activo corriente / pasivo corriente): Deberá ser igual o mayor que 1 en promedi

### `A3A4-trampa` — 1 ítems
A3/A4: el GT tomó el plazo del otro concepto (solicitud vs pago) o del texto legal fijo. Por referencia: pseudo_opus=1.
- Ej. `dias_pago_anticipo` nro 431782: actual **10.0** → propuesto **nan** · …a) Plazo máximo para la presentación de la solicitud de pago de anticipo: Para acceder al anticipo, el Contratista deberá presentar dentro de los 10 días calendarios posteriores a la firma del contrato la solicitud de pago de anticipo. b) Dire

### `A1-plazo-propio` — 4 ítems
A1: plazo propio de la entidad > estándar 60 (el GT tomó el estándar). Por referencia: pseudo_opus=4.
- Ej. `dias_pago_maximo` nro 442517: actual **60.0** → propuesto **58.0** · …alizar los pagos correspondientes, las mismas deberán estar acompañados por el último pago de IVA. El pago de la factura a crédito se realizará a ms tardar dentro de los 58 (cincuenta y ocho) días posteriores a la recepción de los servicios proveídos y de acuerdo a la disponibilidad y

