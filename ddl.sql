-- =============================================================================
-- DDL: DNCP Licitaciones - Secciones de Pliegos
-- Base: PostgreSQL 15+ en Podman/WSL
-- Optimizado para: millones de registros, consultas analíticas, particionado
--
-- Estrategia de rendimiento:
--   - PARTITION BY LIST (year): cada año es una tabla física separada,
--     las consultas filtradas por año solo tocan una partición.
--   - Tipos de datos correctos: INTEGER/SMALLINT en lugar de TEXT para
--     columnas numéricas — ahorra espacio y acelera comparaciones.
--   - content_text como TEXT con pg_trgm para búsqueda full-text eficiente.
--   - BRIN index en page/line_start: ideal para datos con correlación física
--     (se insertan en orden de página), ocupa 100x menos que B-tree.
--   - GIN index en title_normalized con pg_trgm: búsqueda por LIKE/ILIKE
--     en O(1) en lugar de O(n).
--   - Índice compuesto (title_normalized, nro_licitacion): cubre el caso
--     más común de "dame todas las secciones X de la licitación Y".
-- =============================================================================

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS pg_trgm;    -- búsqueda fuzzy en texto
CREATE EXTENSION IF NOT EXISTS btree_gin;  -- GIN para tipos escalares

-- Schema
CREATE SCHEMA IF NOT EXISTS dncp;

-- =============================================================================
-- TABLA: categorias
-- Sin cambios respecto al DDL original.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dncp.categorias (
    category_id TEXT NOT NULL,
    descripcion TEXT,
    CONSTRAINT categorias_pkey PRIMARY KEY (category_id)
);

-- =============================================================================
-- TABLA: licitaciones
-- year cambia de TEXT a SMALLINT — ahorra 2-4 bytes por fila y permite
-- comparaciones aritméticas (year > 2022) sin cast.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dncp.licitaciones (
    nro_licitacion  TEXT      NOT NULL,
    category_id     TEXT,
    year            SMALLINT,
    CONSTRAINT licitaciones_pkey PRIMARY KEY (nro_licitacion),
    CONSTRAINT licitaciones_category_id_fkey
        FOREIGN KEY (category_id) REFERENCES dncp.categorias (category_id)
);

CREATE INDEX IF NOT EXISTS idx_licitaciones_year
    ON dncp.licitaciones (year);

CREATE INDEX IF NOT EXISTS idx_licitaciones_category
    ON dncp.licitaciones (category_id);

-- =============================================================================
-- TABLA: pliegos_secciones (particionada por año)
--
-- Cambios respecto al DDL original:
--   - Renombrada de "pliegos" a "pliegos_secciones" para mayor claridad.
--   - Columnas numéricas usan tipos correctos (INTEGER, SMALLINT).
--   - Nuevas columnas del pipeline de limpieza:
--       content_text          TEXT    — contenido limpio listo para LLM
--       content_clean         TEXT[]  — líneas limpias como array
--       content_length_clean  INTEGER — conteo de líneas post-limpieza
--       title_normalized      TEXT    — título sin tildes/mayúsculas
--       estimated_tokens      INTEGER — estimación de tokens para LLM
--       size_bytes            INTEGER — tamaño en bytes del contenido raw
--       word_count            INTEGER — conteo de palabras
--   - FK a licitaciones incluida en partición (requiere estar en cada partición).
-- =============================================================================
CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones (
    -- Identidad
    document_id          TEXT        NOT NULL,
    nro_licitacion       TEXT        NOT NULL,
    category_id          TEXT,
    year                 SMALLINT    NOT NULL,

    -- Estructura del documento
    title                TEXT,
    title_normalized     TEXT,
    page                 SMALLINT,
    line_start           INTEGER,
    line_end             INTEGER,
    depth                SMALLINT,

    -- Métricas del contenido original
    content_length       INTEGER,
    estimated_tokens     INTEGER,
    word_count           INTEGER,
    size_bytes           INTEGER,

    -- Contenido limpio (generado por ContentCleaningPipeline)
    content_text         TEXT,           -- líneas unidas con \n — input directo al LLM
    content_clean        TEXT[],         -- líneas como array — para procesamiento por línea
    content_length_clean INTEGER         -- líneas post-limpieza

) PARTITION BY LIST (year);

-- =============================================================================
-- PARTICIONES POR AÑO
-- Agregar una partición por cada año presente en los datos.
-- Para años futuros, agregar: CREATE TABLE dncp.pliegos_secciones_NNNN
--   PARTITION OF dncp.pliegos_secciones FOR VALUES IN (NNNN);
-- =============================================================================
CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones_2019
    PARTITION OF dncp.pliegos_secciones FOR VALUES IN (2019);

CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones_2020
    PARTITION OF dncp.pliegos_secciones FOR VALUES IN (2020);

CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones_2021
    PARTITION OF dncp.pliegos_secciones FOR VALUES IN (2021);

CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones_2022
    PARTITION OF dncp.pliegos_secciones FOR VALUES IN (2022);

CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones_2023
    PARTITION OF dncp.pliegos_secciones FOR VALUES IN (2023);

CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones_2024
    PARTITION OF dncp.pliegos_secciones FOR VALUES IN (2024);

CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones_2025
    PARTITION OF dncp.pliegos_secciones FOR VALUES IN (2025);

CREATE TABLE IF NOT EXISTS dncp.pliegos_secciones_2026
    PARTITION OF dncp.pliegos_secciones FOR VALUES IN (2026);

-- =============================================================================
-- ÍNDICES — creados sobre la tabla padre, heredados por particiones
--
-- Estrategia por tipo de consulta:
--
--   B-tree (btree): mejor para igualdad y rangos en columnas de cardinalidad
--                   alta como nro_licitacion, document_id.
--
--   GIN + pg_trgm:  mejor para LIKE/ILIKE/búsqueda de texto en title_normalized.
--                   Ejemplo: WHERE title_normalized LIKE '%especificaciones%'
--
--   BRIN:           mejor para columnas con correlación física (los datos se
--                   insertan en orden, como page o line_start). Ocupa ~100x
--                   menos espacio que B-tree con rendimiento similar para
--                   scans secuenciales.
-- =============================================================================

-- Consulta más común: todas las secciones de una licitación
CREATE INDEX IF NOT EXISTS idx_ps_nro_licitacion
    ON dncp.pliegos_secciones (nro_licitacion);

-- Filtrar por título normalizado (igualdad exacta)
CREATE INDEX IF NOT EXISTS idx_ps_title_normalized
    ON dncp.pliegos_secciones (title_normalized);

-- Consulta compuesta: sección X de licitación Y (cubre ambos filtros)
CREATE INDEX IF NOT EXISTS idx_ps_title_norm_licitacion
    ON dncp.pliegos_secciones (title_normalized, nro_licitacion);

-- Búsqueda fuzzy en título: WHERE title_normalized LIKE '%objeto%'
CREATE INDEX IF NOT EXISTS idx_ps_title_normalized_trgm
    ON dncp.pliegos_secciones USING GIN (title_normalized gin_trgm_ops);

-- Búsqueda full-text en contenido limpio (opcional, pesado)
-- Descomenta si necesitás búsqueda semántica en el contenido:
-- CREATE INDEX IF NOT EXISTS idx_ps_content_text_trgm
--     ON dncp.pliegos_secciones USING GIN (content_text gin_trgm_ops);

-- Filtros por document_id (para reconstruir el documento completo)
CREATE INDEX IF NOT EXISTS idx_ps_document_id
    ON dncp.pliegos_secciones (document_id);

-- BRIN para page y line_start — datos correlacionados con el orden de inserción
CREATE INDEX IF NOT EXISTS idx_ps_page_brin
    ON dncp.pliegos_secciones USING BRIN (page) WITH (pages_per_range = 128);

-- Filtros por category_id para análisis por categoría
CREATE INDEX IF NOT EXISTS idx_ps_category_id
    ON dncp.pliegos_secciones (category_id);

-- Filtros por tokens estimados (para seleccionar secciones que caben en context window)
CREATE INDEX IF NOT EXISTS idx_ps_estimated_tokens
    ON dncp.pliegos_secciones (estimated_tokens);

-- =============================================================================
-- CONFIGURACIÓN DE RENDIMIENTO PARA PODMAN/WSL
-- Ejecutar como superusuario después de crear la base.
-- Ajustar según RAM disponible (valores para 32GB RAM).
-- =============================================================================

-- ALTER SYSTEM SET shared_buffers = '4GB';           -- 12.5% de RAM
-- ALTER SYSTEM SET effective_cache_size = '16GB';    -- 50% de RAM
-- ALTER SYSTEM SET work_mem = '256MB';               -- por operación de sort/hash
-- ALTER SYSTEM SET maintenance_work_mem = '1GB';     -- para CREATE INDEX
-- ALTER SYSTEM SET max_parallel_workers_per_gather = '4';
-- ALTER SYSTEM SET max_parallel_workers = '8';
-- ALTER SYSTEM SET wal_compression = 'on';
-- ALTER SYSTEM SET checkpoint_completion_target = '0.9';
-- SELECT pg_reload_conf();

-- =============================================================================
-- VISTAS DE USO FRECUENTE
-- =============================================================================

-- Resumen por año y categoría
CREATE OR REPLACE VIEW dncp.v_resumen_anual AS
SELECT
    year,
    category_id,
    COUNT(DISTINCT nro_licitacion)  AS total_licitaciones,
    COUNT(*)                         AS total_secciones,
    AVG(estimated_tokens)::INTEGER   AS avg_tokens,
    SUM(size_bytes) / 1024 / 1024   AS total_mb
FROM dncp.pliegos_secciones
GROUP BY year, category_id
ORDER BY year DESC, total_licitaciones DESC;

-- -- Títulos más frecuentes (para identificar boilerplate)
CREATE OR REPLACE VIEW dncp.v_titulos_frecuentes AS
SELECT
    title_normalized,
    COUNT(*)                        AS frecuencia,
    COUNT(DISTINCT nro_licitacion)  AS en_licitaciones,
    ROUND(COUNT(DISTINCT nro_licitacion)::NUMERIC /
          (SELECT COUNT(DISTINCT nro_licitacion) FROM dncp.pliegos_secciones) * 100, 2
    ) AS pct_licitaciones
FROM dncp.pliegos_secciones
GROUP BY title_normalized
ORDER BY frecuencia DESC;