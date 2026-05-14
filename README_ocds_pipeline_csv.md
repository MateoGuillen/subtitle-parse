Estoy construyendo un sistema ETL en Python para mi tesis de detección de anomalías
en contrataciones públicas del Paraguay (DNCP). Te paso el contexto completo:

## OBJETIVO DE LA TESIS

Detectar anomalías en licitaciones públicas usando:

1. Contenido textual de Pliegos de Bases y Condiciones (PDFs) — MI APORTE NUEVO
2. Datos estructurados de la API/CSVs del DNCP
   Algoritmo final: Isolation Forest sobre feature matrix híbrida (texto + datos)

## BASE DE DATOS PostgreSQL — esquema "dncp"

Tablas existentes YA POBLADAS:

- categorias (category_id, descripcion)
- licitaciones (nro_licitacion PK, category_id FK, year, + columnas API pendientes)
- pliegos_secciones (particionada por year: document_id, nro_licitacion,
  category_id, year, title, title_normalized, page, line_start, line_end,
  depth, content_length, estimated_tokens, word_count, size_bytes,
  content_text, content_clean[], content_length_clean)

Tablas YA DEFINIDAS en DDL (pendientes de poblar):

- convocantes (convocante_id PK, nombre, region, localidad, direccion, api_enriched_at)
- items_licitacion (item_id PK, nro_licitacion FK, descripcion, cantidad,
  unidad_id, unidad_nombre, monto, moneda, clasificacion_id, clasificacion_desc,
  unspsc_id, unspsc_desc)
- proveedores (proveedor_id PK "PY-RUC-xxx", ruc, nombre_comercial, nombre_legal,
  tipo_entidad, tamanio, tipo_actividad, region, localidad, direccion, email,
  telefono, tipo_entidad_detalle, escala, url_web, nivel_institucional, api_enriched_at)
- proveedor_productos (id serial PK, proveedor_id FK, producto_id, producto_nombre)
- adjudicaciones (award_id PK, nro_licitacion FK, tender_id, proveedor_id FK,
  proveedor_nombre, monto_adjudicado, moneda, fecha_adjudicacion, estado,
  estado_detalle, descripcion, compiled_release_id, invitation_id)
- contratos (contrato_id PK "LC-XXXX-YY-ZZZZZZ", award_id FK, nro_licitacion FK,
  proveedor_id FK, monto_contrato, moneda, fecha_firma, fecha_inicio, fecha_fin,
  estado, estado_detalle, cantidad_enmiendas, monto_total_enmiendas,
  cantidad_pagos, monto_total_pagado, compiled_release_id, duracion_dias)
- enmiendas_contrato (enmienda_id PK, contrato_id FK, nro_licitacion,
  fecha, descripcion, codigo_financiero, monto_enmienda, moneda)
- pagos_contrato (pago_id PK "SIAF+...", contrato_id FK, nro_licitacion,
  proveedor_id, fecha_pago, fecha_solicitud, fecha_factura, nro_factura,
  monto_factura, monto_pagado, moneda, retencion_iva, retencion_renta,
  retencion_dncp, multa, codigo_financiero, pagador_id, pagador_nombre,
  sistema_origen)
- protestas (protesta_id PK, nro_licitacion FK, tender_id, fecha, estado,
  resultado, motivo, recurrente_id, recurrente_nombre, compiled_release_id,
  tipo_evento, descripcion_evento, estado_evento)
- oferentes (id serial PK, compiled_release_id, nro_licitacion FK,
  proveedor_id, proveedor_nombre, year)
- proveedores_notificados (id serial PK, compiled_release_id, nro_licitacion FK,
  proveedor_id, proveedor_nombre, year)
- consultas_llamado (consulta_id, compiled_release_id, nro_licitacion FK,
  fecha, titulo, descripcion, respuesta, fecha_respuesta, autor_id,
  autor_nombre, year)
- criterios_llamado (id serial PK, compiled_release_id, nro_licitacion FK,
  criterio_id, criterio_titulo, criterio_descripcion, criterio_fuente,
  grupo_id, grupo_descripcion, requisito_id, requisito_titulo,
  requisito_valor, year)
- hitos_contrato (id serial PK, compiled_release_id, contrato_id FK,
  nro_licitacion, hito_id, orden_compra_id, titulo, tipo, codigo,
  fecha_prevista, fecha_cumplida, estado, year)

licitaciones tiene columnas adicionales pendientes:
compiled_release_id, ocid, tiene_subasta, tiene_acuerdo_marco,
duracion_contrato_dias, costo_pliego, criterio_elegibilidad,
tender_id, titulo, metodo_contratacion, metodo_detalle,
categoria_principal, categoria_detalle, monto_estimado, moneda,
criterio_adjudicacion, criterio_detalle, estado, estado_detalle,
fecha_publicacion, fecha_apertura, fecha_fin_consultas,
duracion_consultas_dias, duracion_oferta_dias, cantidad_oferentes,
tiene_consultas, metodo_entrega, garantia_porcentaje,
garantia_validez_dias, cantidad_items, cantidad_lotes,
convocante_id FK, api_enriched_at

## FUENTE DE DATOS

CSVs del estándar OCDS descargados de:
https://www.contrataciones.gov.py/images/opendata-v3/final/ocds/{year}/masivo.zip
Años disponibles: 2021, 2022, 2023, 2024, 2025

CSVs más importantes dentro del ZIP:

- records.csv → licitaciones + convocantes (tiene compiled_release_id y tender/id)
- parties.csv → proveedores + convocantes
- awards.csv + awa_suppliers.csv → adjudicaciones
- ten_tenderers.csv → oferentes
- ten_notifiedSuppliers.csv → proveedores_notificados
- ten_items.csv + ten_ite_additionalClassific.csv → items_licitacion
- ten_criteria.csv + ten_cri_req_requirements.csv + ten_cri_requirementGroups.csv → criterios_llamado
- ten_enquiries.csv → consultas_llamado
- contracts.csv → contratos
- con_amendments.csv → enmiendas_contrato
- con_imp_transactions.csv + con_imp_tra_finantialObliga.csv +
  con_imp_tra_fin_retentions.csv → pagos_contrato
- con_imp_milestones.csv → hitos_contrato
- complaints.csv + events.csv → protestas

PUENTE CLAVE: compiled_release_id (en todos los CSVs) se une con
nro_licitacion via records.csv columna compiledRelease/tender/id
que equivale al nro_licitacion del sistema.

## ARQUITECTURA DEL CÓDIGO

Patrón ETL con estas clases por archivo:
src/etl/extractors/ ← leen datos crudos
src/etl/transformers/ ← transforman a DataFrames limpios
src/etl/loaders/ ← escriben parquet (checkpoint) y PostgreSQL
src/pipelines/ ← orquestan el flujo
scripts/ ← punto de entrada

Características del patrón:

- Procesamiento por chunks/batches para no saturar RAM
- Checkpoint en parquet antes de escribir a DB (idempotente)
- @error_handling decorator en todos los métodos
- Logging con setup_logger(**name**)
- Upsert con ON CONFLICT DO NOTHING o DO UPDATE
- Liberación explícita de memoria con del df

## ESTADO ACTUAL

Se están construyendo los pipelines para poblar todas las tablas
desde los CSVs OCDS. El orden de construcción es:

1. ocds_csv_extractor.py ← YA CONSTRUIDO (descarga ZIP, itera CSVs en chunks)
2. ocds_records_transformer.py ← YA CONSTRUIDO (records.csv → licitaciones + convocantes)
3. ocds_parties_transformer.py ← YA CONSTRUIDO (parties.csv → proveedores)
4. ocds_awards_transformer.py ← YA CONSTRUIDO (awards + awa_suppliers → adjudicaciones)
5. ocds_contracts_transformer.py ← YA CONSTRUIDO (contracts + amendments + transactions)
6. ocds_tender_transformer.py ← YA CONSTRUIDO (tenderers + notified + items + criteria + enquiries)
7. ocds_complaints_transformer.py ← YA CONSTRUIDO (complaints + events → protestas)
8. ocds_loader.py ← YA CONSTRUIDO (parquet checkpoint + PostgreSQL upsert)
9. ocds_pipeline.py ← FALTA CONSTRUIR (orquestador por año)
10. run_ocds_pipeline.py ← FALTA CONSTRUIR (script de entrada)

El extractor base ya maneja:

- Descarga del ZIP por año con requests + stream
- Extracción selectiva de CSVs relevantes
- Iteración en chunks de 100k filas
- Construcción del mapa compiled_release_id → nro_licitacion
- Skip de años ya procesados via checkpoint parquet

Necesito que continues construyendo en este orden: 2. ocds_records_transformer.py (records.csv → licitaciones + convocantes) 3. ocds_parties_transformer.py (parties.csv → proveedores) 4. ocds_awards_transformer.py (awards + awa_suppliers → adjudicaciones) 5. ocds_contracts_transformer.py (contracts + amendments + transactions) 6. ocds_tender_transformer.py (tenderers + notified + items + criteria + enquiries) 7. ocds_complaints_transformer.py (complaints + events → protestas) 8. ocds_loader.py (parquet checkpoint + PostgreSQL upsert) 9. ocds_pipeline.py (orquestador por año) 10. run_ocds_pipeline.py (script de entrada)
