-- ═══════════════════════════════════════════════════════════════════
-- ESQUEMA DNCP — DDL COMPLETO
-- ═══════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────
-- 1. CATEGORIAS (existente)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.categorias (
    category_id     text NOT NULL,
    descripcion     text NULL,
    CONSTRAINT categorias_pkey PRIMARY KEY (category_id)
);

-- ─────────────────────────────────────────────────────────────────
-- 2. CONVOCANTES (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.convocantes (
    convocante_id       text PRIMARY KEY,
    nombre              text,
    region              text,
    localidad           text,
    direccion           text,
    api_enriched_at     timestamptz
);

-- ─────────────────────────────────────────────────────────────────
-- 3. LICITACIONES (existente + enriquecida)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.licitaciones (
    -- columnas originales
    nro_licitacion              text NOT NULL,
    category_id                 text NULL,
    year                        int2 NULL,

    -- columnas nuevas — tender API
    tender_id                   text,
    titulo                      text,
    metodo_contratacion         text,
    metodo_detalle              text,
    categoria_principal         text,
    categoria_detalle           text,
    monto_estimado              numeric,
    moneda                      text DEFAULT 'PYG',
    criterio_adjudicacion       text,
    criterio_detalle            text,
    estado                      text,
    estado_detalle              text,
    fecha_publicacion           timestamptz,
    fecha_apertura              timestamptz,
    fecha_fin_consultas         timestamptz,
    duracion_consultas_dias     int2,
    duracion_oferta_dias        int2,
    cantidad_oferentes          int2,
    tiene_consultas             boolean,
    metodo_entrega              text,
    garantia_porcentaje         numeric,
    garantia_validez_dias       int2,
    cantidad_items              int2,
    cantidad_lotes              int2,
    convocante_id               text REFERENCES dncp.convocantes(convocante_id),
    api_enriched_at             timestamptz,

    CONSTRAINT licitaciones_pkey PRIMARY KEY (nro_licitacion),
    CONSTRAINT licitaciones_category_id_fkey
        FOREIGN KEY (category_id) REFERENCES dncp.categorias(category_id)
);

CREATE INDEX idx_licitaciones_category      ON dncp.licitaciones(category_id);
CREATE INDEX idx_licitaciones_year          ON dncp.licitaciones(year);
CREATE INDEX idx_licitaciones_convocante    ON dncp.licitaciones(convocante_id);
CREATE INDEX idx_licitaciones_tender_id     ON dncp.licitaciones(tender_id);
CREATE INDEX idx_licitaciones_categoria     ON dncp.licitaciones(categoria_principal);
CREATE INDEX idx_licitaciones_metodo        ON dncp.licitaciones(metodo_contratacion);
CREATE INDEX idx_licitaciones_fecha_pub     ON dncp.licitaciones(fecha_publicacion);
CREATE INDEX idx_licitaciones_monto         ON dncp.licitaciones(monto_estimado);

-- ─────────────────────────────────────────────────────────────────
-- 4. ITEMS DE LICITACION (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.items_licitacion (
    item_id             text PRIMARY KEY,
    nro_licitacion      text NOT NULL REFERENCES dncp.licitaciones(nro_licitacion),
    descripcion         text,
    cantidad            numeric,
    unidad_id           text,
    unidad_nombre       text,
    monto               numeric,
    moneda              text DEFAULT 'PYG',
    clasificacion_id    text,
    clasificacion_desc  text,
    unspsc_id           text,
    unspsc_desc         text
);

CREATE INDEX idx_items_nro_licitacion   ON dncp.items_licitacion(nro_licitacion);
CREATE INDEX idx_items_clasificacion    ON dncp.items_licitacion(clasificacion_id);
CREATE INDEX idx_items_unspsc           ON dncp.items_licitacion(unspsc_id);

-- ─────────────────────────────────────────────────────────────────
-- 5. PROVEEDORES (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.proveedores (
    proveedor_id        text PRIMARY KEY,
    ruc                 text,
    nombre_comercial    text,
    nombre_legal        text,
    tipo_entidad        text,
    tamanio             text,
    tipo_actividad      text,
    region              text,
    localidad           text,
    direccion           text,
    email               text,
    telefono            text,
    api_enriched_at     timestamptz
);

-- ─────────────────────────────────────────────────────────────────
-- 6. PROVEEDOR PRODUCTOS (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.proveedor_productos (
    id                  serial PRIMARY KEY,
    proveedor_id        text NOT NULL REFERENCES dncp.proveedores(proveedor_id),
    producto_id         text,
    producto_nombre     text
);

CREATE INDEX idx_prov_prod_proveedor ON dncp.proveedor_productos(proveedor_id);
CREATE INDEX idx_prov_prod_producto  ON dncp.proveedor_productos(producto_id);

-- ─────────────────────────────────────────────────────────────────
-- 7. ADJUDICACIONES (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.adjudicaciones (
    award_id            text PRIMARY KEY,
    nro_licitacion      text NOT NULL REFERENCES dncp.licitaciones(nro_licitacion),
    tender_id           text,
    proveedor_id        text REFERENCES dncp.proveedores(proveedor_id),
    proveedor_nombre    text,
    monto_adjudicado    numeric,
    moneda              text DEFAULT 'PYG',
    fecha_adjudicacion  timestamptz,
    estado              text,
    estado_detalle      text,
    descripcion         text
);

CREATE INDEX idx_adj_nro_licitacion ON dncp.adjudicaciones(nro_licitacion);
CREATE INDEX idx_adj_proveedor      ON dncp.adjudicaciones(proveedor_id);
CREATE INDEX idx_adj_tender_id      ON dncp.adjudicaciones(tender_id);

-- ─────────────────────────────────────────────────────────────────
-- 8. CONTRATOS (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.contratos (
    contrato_id             text PRIMARY KEY,
    award_id                text REFERENCES dncp.adjudicaciones(award_id),
    nro_licitacion          text NOT NULL REFERENCES dncp.licitaciones(nro_licitacion),
    proveedor_id            text REFERENCES dncp.proveedores(proveedor_id),
    monto_contrato          numeric,
    moneda                  text DEFAULT 'PYG',
    fecha_firma             timestamptz,
    fecha_inicio            timestamptz,
    fecha_fin               timestamptz,
    estado                  text,
    estado_detalle          text,
    cantidad_enmiendas      int2    DEFAULT 0,
    monto_total_enmiendas   numeric DEFAULT 0,
    cantidad_pagos          int2    DEFAULT 0,
    monto_total_pagado      numeric DEFAULT 0
);

CREATE INDEX idx_contratos_nro_licitacion   ON dncp.contratos(nro_licitacion);
CREATE INDEX idx_contratos_proveedor        ON dncp.contratos(proveedor_id);
CREATE INDEX idx_contratos_award            ON dncp.contratos(award_id);
CREATE INDEX idx_contratos_fecha_firma      ON dncp.contratos(fecha_firma);

-- ─────────────────────────────────────────────────────────────────
-- 9. ENMIENDAS DE CONTRATO (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.enmiendas_contrato (
    enmienda_id         text PRIMARY KEY,
    contrato_id         text NOT NULL REFERENCES dncp.contratos(contrato_id),
    nro_licitacion      text,
    fecha               timestamptz,
    descripcion         text,
    codigo_financiero   text,
    monto_enmienda      numeric,
    moneda              text DEFAULT 'PYG'
);

CREATE INDEX idx_enmiendas_contrato     ON dncp.enmiendas_contrato(contrato_id);
CREATE INDEX idx_enmiendas_nro_licit    ON dncp.enmiendas_contrato(nro_licitacion);

-- ─────────────────────────────────────────────────────────────────
-- 10. PAGOS DE CONTRATO (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.pagos_contrato (
    pago_id             text PRIMARY KEY,
    contrato_id         text NOT NULL REFERENCES dncp.contratos(contrato_id),
    nro_licitacion      text,
    proveedor_id        text,
    fecha_pago          timestamptz,
    fecha_solicitud     timestamptz,
    fecha_factura       timestamptz,
    nro_factura         text,
    monto_factura       numeric,
    monto_pagado        numeric,
    moneda              text DEFAULT 'PYG',
    retencion_iva       numeric DEFAULT 0,
    retencion_renta     numeric DEFAULT 0,
    retencion_dncp      numeric DEFAULT 0,
    multa               numeric DEFAULT 0,
    codigo_financiero   text,
    pagador_id          text,
    pagador_nombre      text,
    sistema_origen      text DEFAULT 'SIAF'
);

CREATE INDEX idx_pagos_contrato     ON dncp.pagos_contrato(contrato_id);
CREATE INDEX idx_pagos_proveedor    ON dncp.pagos_contrato(proveedor_id);
CREATE INDEX idx_pagos_fecha        ON dncp.pagos_contrato(fecha_pago);
CREATE INDEX idx_pagos_cod_fin      ON dncp.pagos_contrato(codigo_financiero);

-- ─────────────────────────────────────────────────────────────────
-- 11. PROTESTAS (nueva)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.protestas (
    protesta_id         text PRIMARY KEY,
    nro_licitacion      text NOT NULL REFERENCES dncp.licitaciones(nro_licitacion),
    tender_id           text,
    fecha               timestamptz,
    estado              text,
    resultado           text,
    motivo              text,
    recurrente_id       text,
    recurrente_nombre   text
);

CREATE INDEX idx_protestas_nro_licitacion   ON dncp.protestas(nro_licitacion);
CREATE INDEX idx_protestas_tender_id        ON dncp.protestas(tender_id);

-- ─────────────────────────────────────────────────────────────────
-- 12. PLIEGOS SECCIONES (existente — particionada por year)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE dncp.pliegos_secciones (
    document_id             text NOT NULL,
    nro_licitacion          text NOT NULL,
    category_id             text NULL,
    year                    int2 NOT NULL,
    title                   text NULL,
    title_normalized        text NULL,
    page                    int2 NULL,
    line_start              int4 NULL,
    line_end                int4 NULL,
    depth                   int2 NULL,
    content_length          int4 NULL,
    estimated_tokens        int4 NULL,
    word_count              int4 NULL,
    size_bytes              int4 NULL,
    content_text            text NULL,
    content_clean           text[] NULL,
    content_length_clean    int4 NULL
) PARTITION BY LIST (year);

CREATE INDEX idx_ps_document_id           ON ONLY dncp.pliegos_secciones(document_id);
CREATE INDEX idx_ps_nro_licitacion        ON ONLY dncp.pliegos_secciones(nro_licitacion);
CREATE INDEX idx_ps_category_id           ON ONLY dncp.pliegos_secciones(category_id);
CREATE INDEX idx_ps_title_normalized      ON ONLY dncp.pliegos_secciones(title_normalized);
CREATE INDEX idx_ps_estimated_tokens      ON ONLY dncp.pliegos_secciones(estimated_tokens);
CREATE INDEX idx_ps_title_norm_licit      ON ONLY dncp.pliegos_secciones(title_normalized, nro_licitacion);
CREATE INDEX idx_ps_page_brin             ON ONLY dncp.pliegos_secciones USING brin(page) WITH (pages_per_range='128');
CREATE INDEX idx_ps_title_normalized_trgm ON ONLY dncp.pliegos_secciones USING gin(title_normalized gin_trgm_ops);