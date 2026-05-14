# Pipeline ETL OCDS CSV — DNCP Paraguay

Pipeline ETL para poblar la base de datos PostgreSQL `dncp` con datos estructurados de contrataciones públicas del Paraguay (DNCP), extraídos de los archivos CSV del estándar [OCDS](https://standard.open-contracting.org/) disponibles en:

```
https://www.contrataciones.gov.py/images/opendata-v3/final/ocds/{year}/masivo.zip
```

Años procesados: **2021, 2022, 2023, 2024, 2025**.

---

## Arquitectura General

```
scripts/run_ocds_csv_pipeline.py   ← punto de entrada
src/pipelines/ocds_csv_pipeline.py ← orquestador (clase OcdsCsvPipeline)
src/etl/extractors/ocds_csv_extractor.py   ← descarga, extrae, itera CSVs
src/etl/transformers/ocds_records_transformer.py   → licitaciones + convocantes
src/etl/transformers/ocds_parties_transformer.py   → convocantes + proveedores
src/etl/transformers/ocds_awards_transformer.py    → adjudicaciones
src/etl/transformers/ocds_contracts_transformer.py → contratos + enmiendas + pagos
src/etl/transformers/ocds_tender_transformer.py    → oferentes + notificados + items + criterios + consultas
src/etl/transformers/ocds_complaints_transformer.py → protestas
src/etl/loaders/ocds_loader.py         ← checkpoint parquet + upsert PostgreSQL
```

---

## Orden de Carga (FK dependencies)

```
convocantes → licitaciones → proveedores → adjudicaciones →
contratos → enmiendas → pagos → oferentes → notificados →
items → criterios → consultas → protestas
```

Cada año se procesa de forma independiente. El checkpoint permite saltar años ya procesados.

---

## Paso a Paso del Pipeline

### 1. Script de entrada: `scripts/run_ocds_csv_pipeline.py`

Toma la configuración de `config/settings.py` (variables de entorno: DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, BASE_OUTPUT_RAW_DIR) y construye el DSN de PostgreSQL. Crea el directorio de trabajo en `{BASE_OUTPUT_RAW_DIR}/ocds_csv`. Instancia `OcdsCsvPipeline` con los años `[2021-2025]` y ejecuta `.run()`.

### 2. Orquestador: `src/pipelines/ocds_csv_pipeline.py` — `run()`

Conecta a PostgreSQL, itera cada año y llama a `run_year(year)`. Al finalizar desconecta.

### 3. Por año: `run_year(year)`

```
a) Verificar checkpoint → si ya procesado, salta
b) download_zip(year)   → descarga masivo.zip con streaming (8 MB chunks)
c) extract_relevant_csvs(year) → extrae solo 20 CSVs relevantes del ZIP
d) build_id_map(year)   → construye {compiledRelease/id → nro_licitacion} desde records.csv
e) _process_records     → records.csv → licitaciones + convocantes
f) _process_parties     → parties.csv → convocantes + proveedores
g) _process_awards      → awards.csv + awa_suppliers.csv → adjudicaciones
h) _process_contracts   → contracts.csv + con_amendments.csv + con_imp_transactions.csv + obligation + retentions → contratos + enmiendas + pagos
i) _process_tender      → 10 CSVs del tender → oferentes + notificados + items + criterios + consultas
j) _process_complaints  → complaints.csv + events.csv → protestas
k) _load_year           → concatena acumuladores → parquet checkpoint + upsert PostgreSQL
l) Si todos los upserts fueron exitosos → marca año procesado
```

### 4. Procesamiento específico por CSV

| CSV | Tabla(s) destino | Columnas mapeadas |
|-----|------------------|-------------------|
| `records.csv` | licitaciones, convocantes | compiledRelease/id → compiled_release_id, compiledRelease/tender/id → nro_licitacion, más 25 columnas del tender |
| `parties.csv` | convocantes, proveedores | Comparte entidades según el campo `roles` (buyer/procuringEntity → convocantes, supplier/tenderer → proveedores) |
| `awards.csv` | adjudicaciones (base) | award_id, proveedor_id, monto_adjudicado, fecha, estado |
| `awa_suppliers.csv` | adjudicaciones (merge) | award_id + proveedor_id + nombre → se mergea con awards |
| `contracts.csv` | contratos | contrato_id, award_id, monto, fechas, estado, duración |
| `con_amendments.csv` | enmiendas_contrato | enmienda_id, contrato_id, fecha, descripción, monto |
| `con_imp_transactions.csv` | pagos_contrato (base) | pago_id, contrato_id, monto, fechas, pagador |
| `con_imp_tra_finantialObliga.csv` | pagos (merge) | factura asociada a cada pago |
| `con_imp_tra_fin_retentions.csv` | pagos (pivot) | retenciones (IVA, RENTA, DNCP, MULTA) → se pivotean por tipo |
| `ten_tenderers.csv` | oferentes | proveedores que se presentaron a la licitación |
| `ten_notifiedSuppliers.csv` | proveedores_notificados | proveedores invitados directamente |
| `ten_items.csv` | items_licitacion (base) | item_id, descripción, cantidad, monto, clasificación |
| `ten_ite_additionalClassific.csv` | items (merge) | clasificación UNSPSC adicional |
| `ten_criteria.csv` | criterios_llamado (base) | criterios de evaluación |
| `ten_cri_requirementGroups.csv` | criterios (merge) | grupos de requisitos |
| `ten_cri_req_requirements.csv` | criterios (merge) | requisitos específicos con valores esperados |
| `ten_enquiries.csv` | consultas_llamado | preguntas de oferentes sobre el pliego |
| `complaints.csv` | protestas (base) | IDs de protestas |
| `events.csv` | protestas (merge) | eventos asociados a cada protesta (tipo, fecha, estado) |
| `con_imp_milestones.csv` | hitos_contrato (pendiente) | hitos de ejecución del contrato |

### 5. Carga a PostgreSQL: `_load_year()`

1. **Concatena** todos los chunks acumulados por tipo en DataFrames únicos
2. **Deduplica** por PK (drop_duplicates)
3. Para cada tabla en orden FK:
   - Escribe a **parquet** (checkpoint — siempre se guarda, incluso si DB falla)
   - Ejecuta **upsert** (`INSERT ... ON CONFLICT DO NOTHING/UPDATE`)
   - Si el upsert falla → marca error pero continúa
4. Si hubo **algún error** en cualquier upsert del año → **NO marca el año como procesado**

### 6. Checkpoint

- Por año: `{work_dir}/checkpoint/year_{year}.parquet`
- Por tabla: `{work_dir}/checkpoint/parquet/{year}/{table}.parquet`
- El checkpoint de año es un flag simple (archivo parquet con columna `year`)
- Los parquets por tabla son copias de seguridad para re-procesamiento

---

## Decisiones Técnicas

### `int4` en vez de `int2` en licitaciones

**Problema**: Los CSVs del DNCP contienen valores de duración/cantidad que exceden el rango de `int2` (-32768 a 32767), causando `smallint out of range`. Año 2025.

**Solución**: Las columnas `duracion_consultas_dias`, `duracion_oferta_dias`, `duracion_contrato_dias`, `cantidad_oferentes`, `garantia_validez_dias`, `cantidad_items`, `cantidad_lotes` se definen como `int4` en el DDL.

### FK `contrato_id` eliminadas en enmiendas_contrato y pagos_contrato

**Problema**: `con_amendments.csv` y `con_imp_transactions.csv` referencian contratos de años anteriores que no existen en ningún `contracts.csv` del pipeline (ej: contrato de 2015 referenciado en enmiendas de 2021). La FK bloquea el INSERT de todo el batch.

**Solución**: Se eliminaron las FK:
```sql
ALTER TABLE dncp.enmiendas_contrato DROP CONSTRAINT enmiendas_contrato_contrato_id_fkey;
ALTER TABLE dncp.pagos_contrato DROP CONSTRAINT pagos_contrato_contrato_id_fkey;
```
El pipeline filtra por `self._v(r, "contrato_id")` para evitar filas totalmente nulas, pero no bloquea referencias a contratos de otros años. Para la tesis, el impacto es mínimo (~0.1% de filas).

### `NaT` → `None` en valores de timestamp

**Problema**: `pd.to_datetime(x, errors='coerce')` produce `pd.NaT` cuando la conversión falla. psycopg2 no sabe adaptar `NaT` a `NULL` de PostgreSQL, causando `invalid input syntax for type timestamp: "NaT"`.

**Solución**: El helper `_v()` en `ocds_loader.py` ahora usa `pd.isna(val)` que detecta `NaN`, `NaT` y `pd.NA`, retornando `None` en todos los casos.

### `NAType` no adaptable en psycopg2

**Problema**: Columnas casteadas a `Int32` (nullable integer de pandas) usan `pd.NA` que no es adaptable por psycopg2 (`can't adapt type 'NAType'`).

**Solución**: `pd.isna(val)` en `_v()` también cubre `pd.NA`.

### ON CONFLICT DO UPDATE con duplicados

**Problema**: Acumular chunks individuales de parties.csv produce convocantes/proveedores repetidos. `execute_values` con `ON CONFLICT DO UPDATE` no tolera duplicados en el mismo comando (`ON CONFLICT DO UPDATE command cannot affect row a second time`).

**Solución**: `drop_duplicates(subset=[pk])` en `_load_year()` antes del upsert.

### Checkpoint condicional

**Problema**: `@error_handling` captura excepciones y el pipeline continúa, pero el checkpoint se marcaba igual, impidiendo re-procesar años con errores.

**Solución**: Todos los upsert methods retornan `True/False`. `_load_year()` retorna `False` si algún upsert falló. `run_year()` solo marca checkpoint si `_load_year()` retornó `True`. Los datos siempre se guardan en parquet, por lo que no hay pérdida.

---

## Cómo Correr

### Requisitos

```bash
# PostgreSQL accesible con esquema dncp creado
# Variables de entorno en .env:
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=172.31.233.136
DB_PORT=5433
DB_NAME=dncp
BASE_OUTPUT_RAW_DIR=./data/raw
```

### DDL

```bash
PGPASSWORD=postgres psql -h 172.31.233.136 -p 5433 -U postgres -d dncp -c "DROP SCHEMA IF EXISTS dncp CASCADE; CREATE SCHEMA dncp;"
PGPASSWORD=postgres psql -h 172.31.233.136 -p 5433 -U postgres -d dncp -f DDL_OCDS_PIPELINE_COMPLETO.sql
```

### Ejecución

```bash
python -m scripts.run_ocds_csv_pipeline
```

### Re-ejecutar desde cero

```bash
PGPASSWORD=postgres psql -h 172.31.233.136 -p 5433 -U postgres -d dncp <<< "
TRUNCATE TABLE dncp.hitos_contrato, dncp.pagos_contrato, dncp.enmiendas_contrato, dncp.contratos, dncp.adjudicaciones, dncp.criterios_llamado, dncp.consultas_llamado, dncp.proveedores_notificados, dncp.oferentes, dncp.proveedor_productos, dncp.items_licitacion, dncp.licitaciones, dncp.proveedores, dncp.convocantes, dncp.categorias, dncp.pliegos_secciones_2021, dncp.pliegos_secciones_2022, dncp.pliegos_secciones_2023, dncp.pliegos_secciones_2024, dncp.pliegos_secciones_2025 CASCADE;
"
rm -rf ./data/raw/ocds_csv/checkpoint
python -m scripts.run_ocds_csv_pipeline
```

---

## Estructura de Archivos

```
DDL_OCDS_PIPELINE_COMPLETO.sql           ← esquema PostgreSQL completo
src/etl/extractors/ocds_csv_extractor.py ← descarga ZIP, extrae CSVs, itera en chunks, mapa de IDs
src/etl/transformers/ocds_records_transformer.py → licitaciones + convocantes
src/etl/transformers/ocds_parties_transformer.py → convocantes + proveedores
src/etl/transformers/ocds_awards_transformer.py  → adjudicaciones
src/etl/transformers/ocds_contracts_transformer.py → contratos, enmiendas, pagos
src/etl/transformers/ocds_tender_transformer.py  → oferentes, notificados, items, criterios, consultas
src/etl/transformers/ocds_complaints_transformer.py → protestas
src/etl/loaders/ocds_loader.py           ← checkpoint parquet + upsert PostgreSQL
src/pipelines/ocds_csv_pipeline.py       ← orquestador ETL
scripts/run_ocds_csv_pipeline.py         ← entry point
```

---

## Volúmenes de Datos (estimados 2021-2025)

| Tabla | Registros totales |
|-------|-------------------|
| convocantes | ~2,000 |
| licitaciones | ~61,000 |
| proveedores | ~55,000 |
| adjudicaciones | ~89,000 |
| contratos | ~85,000 |
| enmiendas | ~19,000 |
| pagos | ~684,000 |
| oferentes | ~142,000 |
| notificados | ~195,000 |
| items | ~2,544,000 |
| criterios | ~78,000 |
| consultas | ~187,000 |
| protestas | ~4,800 |
