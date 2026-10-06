-- Caché content-addressed + staging versionado del fallback LLM (diseño: docs/piloto_llm_fallback.md)
-- Aplicar con: psql "$DATABASE_URL" -f sql/llm_fallback_cache.sql

CREATE TABLE IF NOT EXISTS dncp.llm_fallback_cache (
    text_sha256 CHAR(64) NOT NULL,
    model_id    TEXT NOT NULL,
    prompt_hash CHAR(64) NOT NULL,
    field       TEXT NOT NULL,
    value       NUMERIC,
    raw_short   TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (text_sha256, model_id, prompt_hash, field)
);

CREATE TABLE IF NOT EXISTS dncp.llm_fallback_runs (
    run_id         TEXT NOT NULL,
    nro_licitacion TEXT NOT NULL,
    field          TEXT NOT NULL,
    value          NUMERIC,
    conf_new       REAL,
    raw_short      TEXT,
    model_id       TEXT NOT NULL,
    prompt_hash    CHAR(64) NOT NULL,
    schema_version CHAR(64) NOT NULL,
    code_version   TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'done', 'failed')),
    attempts       INT NOT NULL DEFAULT 0,
    last_error     TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, nro_licitacion, field)
);

CREATE INDEX IF NOT EXISTS llm_fallback_runs_status_idx
    ON dncp.llm_fallback_runs (run_id, status);
