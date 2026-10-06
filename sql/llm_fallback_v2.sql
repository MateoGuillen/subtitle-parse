-- Fallback v2 (plan de corrección F3; auditoría H1.3, H1.4, H1.5, H7.2, H7.3).
-- Las tablas v1 (llm_fallback_cache, llm_fallback_runs) quedan congeladas como evidencia
-- histórica del run llmfb-meta-llama-3.1-8b-instruct-fp8-2834432bb7237924.
-- Idempotente. Aplicar con:
--   podman exec -i dncp psql -U postgres -d dncp < sql/llm_fallback_v2.sql

-- Registro de corridas: el fingerprint completo queda en la BD, no solo en un JSON.
CREATE TABLE IF NOT EXISTS dncp.llm_runs (
    run_id       TEXT PRIMARY KEY,
    fingerprint  TEXT NOT NULL,
    config       JSONB NOT NULL,
    git_sha      TEXT NOT NULL,
    git_dirty    BOOLEAN NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);

-- Caché v2: guarda SOLO el crudo del modelo (incluido "NULL"). El valor se recalcula
-- al leer con extraction_postprocess.resolve(), así una regla corregida nunca queda
-- congelada en la caché (H1.4). La clave incluye el fingerprint de generación
-- (modelo+revisión+quant+prompt+schema del campo+sampling+guided).
CREATE TABLE IF NOT EXISTS dncp.llm_fallback_cache_v2 (
    text_sha256  CHAR(64) NOT NULL,
    field        TEXT NOT NULL,
    gen_fp       CHAR(16) NOT NULL,
    raw          TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (text_sha256, field, gen_fp)
);

-- Staging v2: una fila por (run, doc, campo) con origen explícito del valor.
CREATE TABLE IF NOT EXISTS dncp.llm_fallback_runs_v2 (
    run_id              TEXT NOT NULL REFERENCES dncp.llm_runs(run_id),
    nro_licitacion      TEXT NOT NULL,
    field               TEXT NOT NULL,
    section_title       TEXT,
    section_line_start  INTEGER,
    section_year        SMALLINT,
    text_sha256         CHAR(64),
    rule_applies        BOOLEAN,
    rule_value          NUMERIC,
    raw                 TEXT,          -- crudo completo del LLM (NULL si no se consultó)
    model_value         NUMERIC,       -- número del LLM validado, sin reglas
    value               NUMERIC,       -- valor final según mode
    source              TEXT CHECK (source IN ('rule', 'llm', 'none')),
    mode                TEXT NOT NULL,
    from_cache          BOOLEAN NOT NULL DEFAULT FALSE,
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'done', 'failed')),
    attempts            INT NOT NULL DEFAULT 0,
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, nro_licitacion, field)
);
CREATE INDEX IF NOT EXISTS llm_fallback_runs_v2_status_idx
    ON dncp.llm_fallback_runs_v2 (run_id, status);

-- updated_at siempre al día (H1.5: en v1 nunca se actualizaba)
CREATE OR REPLACE FUNCTION dncp.touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS llm_fallback_runs_v2_touch ON dncp.llm_fallback_runs_v2;
CREATE TRIGGER llm_fallback_runs_v2_touch BEFORE UPDATE ON dncp.llm_fallback_runs_v2
    FOR EACH ROW EXECUTE FUNCTION dncp.touch_updated_at();

-- Historia de promociones a producción: permite auditoría y rollback (H7.3).
CREATE TABLE IF NOT EXISTS dncp.document_features_v2_history (
    id              BIGSERIAL PRIMARY KEY,
    target_table    TEXT NOT NULL,
    nro_licitacion  TEXT NOT NULL,
    field           TEXT NOT NULL,
    old_value       NUMERIC,
    old_conf        REAL,
    old_source      TEXT,
    new_value       NUMERIC,
    new_conf        REAL,
    new_source      TEXT,
    old_status      TEXT,
    new_status      TEXT,
    run_id          TEXT NOT NULL,
    promoted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    rolled_back_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS dfv2_history_run_idx
    ON dncp.document_features_v2_history (run_id, field);

-- columnas agregadas tras la primera aplicación (idempotente)
ALTER TABLE dncp.document_features_v2_history ADD COLUMN IF NOT EXISTS old_status TEXT;
ALTER TABLE dncp.document_features_v2_history ADD COLUMN IF NOT EXISTS new_status TEXT;
