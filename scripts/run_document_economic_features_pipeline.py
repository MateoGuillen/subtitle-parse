"""
Entry point for the document economic features pipeline.

Usage::

    python scripts/run_document_economic_features_pipeline.py

Populates ``dncp.document_economic_features`` with economic indicators
derived from OCDS tables (licitaciones, contratos, enmiendas,
adjudicaciones, oferentes, pagos, protestas, convocantes, proveedores).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipelines.document_economic_features_pipeline import (
    DocumentEconomicFeaturesPipeline,
)
from config.settings import DB_CONFIG


def main() -> None:
    config = {
        "db_params": DB_CONFIG,
    }
    pipeline = DocumentEconomicFeaturesPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
