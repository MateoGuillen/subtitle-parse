"""Script to load cleaned sections into PostgreSQL."""

import os
from src.pipelines.sections_to_db_pipeline import SectionsToDbPipeline
from config.settings import BASE_OUTPUT_PROCESSED_DIR
from config.settings import DB_CONFIG


def main():
    """
    Configure and run the sections-to-database pipeline.

    Prerequisites
    -------------
    1. PostgreSQL running in Podman/WSL:
       podman run -d --name dncp-db \\
           -e POSTGRES_USER=postgres \\
           -e POSTGRES_PASSWORD=secret \\
           -e POSTGRES_DB=dncp \\
           -p 5432:5432 \\
           -v dncp_data:/var/lib/postgresql/data \\
           postgres:15

    2. DDL applied:
       psql -h localhost -U postgres -d dncp -f sql/ddl.sql

    3. Install psycopg2:
       pip install psycopg2-binary

    Input
    -----
    Partitioned parquet dataset produced by ContentCleaningPipeline::

        data/processed/sections_clean/
          year=2021/part-0.parquet
          year=2022/part-0.parquet
          ...

    Output
    ------
    Table populated in PostgreSQL::

        dncp.pliegos_secciones   (inserted, partitioned by year)

    Checkpoints
    -----------
    After each year completes, a marker file ``.sections_db_done_{year}`` is
    written to the sections directory.  On re-run, completed years are skipped
    automatically.  Delete the marker to reprocess a year.

    Configuration
    -------------
    Edit the variables below to match your environment.
    For production, move credentials to environment variables or a
    secrets manager — never commit passwords to version control.
    """

    # ── Paths ──────────────────────────────────────────────────────────────
    sections_dir = os.path.join(BASE_OUTPUT_PROCESSED_DIR, "sections_clean")

    # ── Database connection ────────────────────────────────────────────────
    # Read from environment variables if available, fall back to defaults.

    # Use database config if available, fall back to environment variables.
    db_host = DB_CONFIG.get("host")
    db_port = DB_CONFIG.get("port")
    db_name = DB_CONFIG.get("database")
    db_user = DB_CONFIG.get("user")
    db_password = DB_CONFIG.get("password")

    db_dsn = (
        f"host={db_host} port={db_port} dbname={db_name} "
        f"user={db_user} password={db_password}"
    )

    # ── Pipeline configuration ─────────────────────────────────────────────
    config = {
        "sections_dir": sections_dir,
        "db_dsn": db_dsn,
        # Rows committed per batch.
        # 10_000 is safe for 32GB RAM.  Increase to 50_000 for faster loads
        # if you have headroom — each batch holds ~50MB of row data in memory.
        "batch_size": 10_000,
        # Load specific years only, or None for all available years.
        # Example: "years": [2024, 2025]  to reload only recent years.
        # Note: años con partición faltante en PostgreSQL se saltan.
        "years": [2021, 2022, 2023, 2024, 2025],
        # Saltar años anteriores a este (útil para reanudar tras crash).
        # Ejemplo: "start_year": 2025  → solo procesa 2025 en adelante.
        "start_year": None,
    }

    pipeline = SectionsToDbPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
