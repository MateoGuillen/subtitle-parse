"""
Entry point for the document feature engineering pipeline.

Usage::

    python scripts/run_document_features_pipeline.py

Builds the ``dncp.document_features`` table by pivoting section titles
(``dncp.pliegos_secciones``) into document-level feature columns.
"""

from src.pipelines.document_features_pipeline import DocumentFeaturesPipeline
from config.settings import DB_CONFIG


def main() -> None:
    config = {
        "db_params": DB_CONFIG,
        "top_n": 80,
    }
    pipeline = DocumentFeaturesPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
