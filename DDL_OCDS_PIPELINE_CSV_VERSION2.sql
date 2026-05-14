-- ═══════════════════════════════════════════════════════════════════
-- TABLAS NUEVAS DESDE CSV OCDS
-- Complementan el esquema existente sin reemplazarlo
-- ═══════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────
-- OFERENTES POR LICITACION (ten_tenderers.csv)
-- Quiénes se presentaron — no solo quién ganó
-- Feature clave: cantidad_oferentes real vs numberOfTenderers
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.oferentes (
    id                  serial PRIMARY KEY,
    compiled_release_id text NOT NULL,      -- compiledRelease/id
    nro_licitacion      text REFERENCES dncp.licitaciones(nro_licitacion),
    proveedor_id        text,               -- compiledRelease/tender/tenderers/0/id (RUC)
    proveedor_nombre    text,
    year                int2
);
CREATE INDEX idx_oferentes_nro_licit    ON dncp.oferentes(nro_licitacion);
CREATE INDEX idx_oferentes_proveedor    ON dncp.oferentes(proveedor_id);
CREATE INDEX idx_oferentes_release      ON dncp.oferentes(compiled_release_id);

-- ─────────────────────────────────────────────────────────────────
-- PROVEEDORES NOTIFICADOS (ten_notifiedSuppliers.csv)
-- A quiénes se invitó directamente — señal de direccionamiento
-- Si el notificado = ganador siempre, es anomalía
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.proveedores_notificados (
    id                  serial PRIMARY KEY,
    compiled_release_id text NOT NULL,
    nro_licitacion      text REFERENCES dncp.licitaciones(nro_licitacion),
    proveedor_id        text,
    proveedor_nombre    text,
    year                int2
);
CREATE INDEX idx_prov_not_nro_licit  ON dncp.proveedores_notificados(nro_licitacion);
CREATE INDEX idx_prov_not_proveedor  ON dncp.proveedores_notificados(proveedor_id);

-- ─────────────────────────────────────────────────────────────────
-- CONSULTAS AL LLAMADO (ten_enquiries.csv)
-- Preguntas de oferentes sobre el pliego
-- Muchas consultas = pliego ambiguo o confuso
-- Pocas consultas en pliego complejo = oferentes disuadidos
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.consultas_llamado (
    consulta_id         text,
    compiled_release_id text NOT NULL,
    nro_licitacion      text REFERENCES dncp.licitaciones(nro_licitacion),
    fecha               timestamptz,
    titulo              text,
    descripcion         text,
    respuesta           text,
    fecha_respuesta     timestamptz,
    autor_id            text,
    autor_nombre        text,
    year                int2,
    PRIMARY KEY (compiled_release_id, consulta_id)
);
CREATE INDEX idx_consultas_nro_licit ON dncp.consultas_llamado(nro_licitacion);

-- ─────────────────────────────────────────────────────────────────
-- CRITERIOS Y REQUISITOS (ten_criteria + ten_cri_req_requirements)
-- Garantías y requisitos formales del llamado
-- Detecta si el porcentaje de garantía es inusual para el rubro
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.criterios_llamado (
    id                  serial PRIMARY KEY,
    compiled_release_id text NOT NULL,
    nro_licitacion      text REFERENCES dncp.licitaciones(nro_licitacion),
    criterio_id         text,
    criterio_titulo     text,
    criterio_descripcion text,
    criterio_fuente     text,               -- "tenderer"
    grupo_id            text,
    grupo_descripcion   text,
    requisito_id        text,
    requisito_titulo    text,               -- "Porcentaje de la garantia"
    requisito_valor     text,               -- "5"
    year                int2
);
CREATE INDEX idx_criterios_nro_licit ON dncp.criterios_llamado(nro_licitacion);

-- ─────────────────────────────────────────────────────────────────
-- HITOS DE EJECUCION (con_imp_milestones.csv)
-- Entregas y pagos planificados vs reales
-- Hitos vencidos sin cumplir = anomalía en ejecución
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.hitos_contrato (
    id                  serial PRIMARY KEY,
    compiled_release_id text NOT NULL,
    contrato_id         text REFERENCES dncp.contratos(contrato_id),
    nro_licitacion      text,
    hito_id             text,
    orden_compra_id     text,
    titulo              text,
    tipo                text,               -- "delivery", "payment"
    codigo              text,
    fecha_prevista      timestamptz,
    fecha_cumplida      timestamptz,
    estado              text,               -- "met", "pending"
    year                int2
);
CREATE INDEX idx_hitos_contrato     ON dncp.hitos_contrato(contrato_id);
CREATE INDEX idx_hitos_nro_licit    ON dncp.hitos_contrato(nro_licitacion);

-- ─────────────────────────────────────────────────────────────────
-- ADAPTACIONES A TABLAS EXISTENTES
-- Columnas que vienen de los CSV pero no estaban en el DDL previo
-- ─────────────────────────────────────────────────────────────────

-- licitaciones: agregar compiled_release_id como puente con los CSV
ALTER TABLE dncp.licitaciones
    ADD COLUMN IF NOT EXISTS compiled_release_id    text,
    ADD COLUMN IF NOT EXISTS ocid                   text,
    ADD COLUMN IF NOT EXISTS tiene_subasta          boolean,
    ADD COLUMN IF NOT EXISTS tiene_acuerdo_marco    boolean,
    ADD COLUMN IF NOT EXISTS duracion_contrato_dias int2,
    ADD COLUMN IF NOT EXISTS costo_pliego           numeric,
    ADD COLUMN IF NOT EXISTS criterio_elegibilidad  text;

CREATE INDEX IF NOT EXISTS idx_licit_compiled_release
    ON dncp.licitaciones(compiled_release_id);

-- contratos: agregar duración en días y campos faltantes del CSV
ALTER TABLE dncp.contratos
    ADD COLUMN IF NOT EXISTS compiled_release_id    text,
    ADD COLUMN IF NOT EXISTS duracion_dias          int2;

-- adjudicaciones: agregar invitation_id que viene del CSV
ALTER TABLE dncp.adjudicaciones
    ADD COLUMN IF NOT EXISTS compiled_release_id    text,
    ADD COLUMN IF NOT EXISTS invitation_id          text;

-- proveedores: agregar campos que vienen de parties.csv
ALTER TABLE dncp.proveedores
    ADD COLUMN IF NOT EXISTS tipo_entidad_detalle   text,
    ADD COLUMN IF NOT EXISTS escala                 text,
    ADD COLUMN IF NOT EXISTS url_web                text,
    ADD COLUMN IF NOT EXISTS nivel_institucional    text;

-- protestas: el CSV de complaints solo tiene id, enriquecer con events
ALTER TABLE dncp.protestas
    ADD COLUMN IF NOT EXISTS compiled_release_id    text,
    ADD COLUMN IF NOT EXISTS tipo_evento            text,
    ADD COLUMN IF NOT EXISTS descripcion_evento     text,
    ADD COLUMN IF NOT EXISTS estado_evento          text;