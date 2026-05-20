"""Script de entrada para el pipeline OCDS CSV.

Uso:
    python scripts/run_ocds_csv_pipeline.py

Requiere variables de entorno (ver config/settings.py):
    DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
    BASE_OUTPUT_RAW_DIR, BASE_OUTPUT_PROCESSED_DIR, BASE_INPUT_EXTERNAL_DATA_DIR

Prerrequisito:
    Ejecutar primero: python -m scripts.run_ocds_pipeline
    (genera el CSV de licitaciones con pliego electrónico)
"""

import os
from src.pipelines.ocds_csv_pipeline import OcdsCsvPipeline
from config.settings import (
    DB_CONFIG, BASE_OUTPUT_RAW_DIR,
    BASE_OUTPUT_PROCESSED_DIR, BASE_INPUT_EXTERNAL_DATA_DIR,
)


def _build_dsn() -> str:
    return (
        f"host={DB_CONFIG['host']} "
        f"port={DB_CONFIG['port']} "
        f"dbname={DB_CONFIG['database']} "
        f"user={DB_CONFIG['user']} "
        f"password={DB_CONFIG['password']}"
    )


def main():
    work_dir = os.path.join(BASE_OUTPUT_RAW_DIR, "ocds_csv")

    # Whitelist: licitaciones con Pliego Electrónico (generado por run_ocds_pipeline)
    whitelist_path = os.path.join(
        BASE_OUTPUT_PROCESSED_DIR, "csv",
        "ten_documents_pliego_pdf_every_year_filtered.csv",
    )
    # Mapa de categorías
    categories_path = os.path.join(
        BASE_INPUT_EXTERNAL_DATA_DIR, "unique_categories_sorted.csv",
    )

    os.makedirs(work_dir, exist_ok=True)

    config = {
        "work_dir": work_dir,
        "db_dsn": _build_dsn(),
        "years": [2021, 2022, 2023, 2024, 2025],
        "whitelist_path": whitelist_path,
        "categories_path": categories_path,
    }

    pipeline = OcdsCsvPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
