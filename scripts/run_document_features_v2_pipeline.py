"""
Entry point for the document features v2 pipeline (regex-based numeric extraction).

Usage::

    python scripts/run_document_features_v2_pipeline.py

Builds the ``dncp.document_features_v2`` table by applying per-title regex
patterns defined in ``master_schemas_v2.json`` to extract parametric features
(montos, porcentajes, plazos, binarios) from the 25 finalist titles.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipelines.document_features_v2_pipeline import DocumentFeaturesV2Pipeline
from config.settings import DB_CONFIG


def main() -> None:
    config = {
        "db_params": DB_CONFIG,
    }
    pipeline = DocumentFeaturesV2Pipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
