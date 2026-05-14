"""Extractor base para los CSV OCDS del DNCP.

Responsabilidades:
- Descargar masivo.zip por año con streaming (no carga todo en RAM).
- Extraer selectivamente solo los CSVs relevantes.
- Iterar cada CSV en chunks de 100k filas para no saturar RAM.
- Construir y cachear el mapa compiled_release_id → nro_licitacion
  usando records.csv, que es el puente entre todos los demás CSVs.
- Detectar años ya procesados via checkpoint parquet (idempotente).
"""

import os
import zipfile
import requests
import pandas as pd
from typing import Iterator, Optional
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger


# CSVs que necesitamos del ZIP — ignoramos el resto para ahorrar disco y RAM
RELEVANT_CSVS = {
    "records.csv",
    "parties.csv",
    "awards.csv",
    "awa_suppliers.csv",
    "ten_tenderers.csv",
    "ten_notifiedSuppliers.csv",
    "ten_items.csv",
    "ten_ite_additionalClassific.csv",
    "ten_criteria.csv",
    "ten_cri_requirementGroups.csv",
    "ten_cri_req_requirements.csv",
    "ten_enquiries.csv",
    "contracts.csv",
    "con_amendments.csv",
    "con_imp_transactions.csv",
    "con_imp_tra_finantialObliga.csv",
    "con_imp_tra_fin_retentions.csv",
    "con_imp_milestones.csv",
    "complaints.csv",
    "events.csv",
}

# URL base del DNCP para los ZIPs masivos
BASE_URL = "https://www.contrataciones.gov.py/images/opendata-v3/final/ocds/{year}/masivo.zip"

# Columnas de records.csv que necesitamos para el mapa de IDs
RECORDS_ID_COLS = [
    "compiledRelease/id",           # compiled_release_id
    "compiledRelease/tender/id",    # nro_licitacion
]

CHUNK_SIZE = 100_000


class OcdsCsvExtractor:
    """
    Extractor base para los archivos CSV OCDS del DNCP.

    Descarga el ZIP de cada año, extrae solo los CSVs relevantes,
    y provee iteradores en chunks para procesamiento eficiente en RAM.
    """

    def __init__(self, work_dir: str):
        """
        Args:
            work_dir: Directorio donde se guardan ZIPs y CSVs extraídos.
                      Estructura: work_dir/{year}/masivo.zip
                                  work_dir/{year}/csv/{archivo}.csv
        """
        self.work_dir = work_dir
        self.logger = setup_logger(__name__)
        self._id_maps: dict[str, dict] = {}  # cache por año

    # ─────────────────────────────────────────────────────────────
    # Descarga y extracción
    # ─────────────────────────────────────────────────────────────

    @error_handling(default_return=False)
    def download_zip(self, year: int, force: bool = False) -> bool:
        """
        Descarga masivo.zip para el año dado con streaming.
        Si ya existe en disco lo saltea (a menos que force=True).

        Args:
            year: Año a descargar (2021-2025).
            force: Si True, re-descarga aunque exista.

        Returns:
            True si el ZIP está disponible en disco.
        """
        year_dir = os.path.join(self.work_dir, str(year))
        os.makedirs(year_dir, exist_ok=True)
        zip_path = os.path.join(year_dir, "masivo.zip")

        if os.path.exists(zip_path) and not force:
            self.logger.info("ZIP %d ya existe en %s, saltando descarga.", year, zip_path)
            return True

        url = BASE_URL.format(year=year)
        self.logger.info("Descargando %s ...", url)

        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            downloaded = 0
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):  # 8MB chunks
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        self.logger.info("  %.1f%% (%d MB / %d MB)",
                                         pct, downloaded // 1_000_000, total // 1_000_000)

        self.logger.info("Descarga completa: %s", zip_path)
        return True

    @error_handling(default_return=False)
    def extract_relevant_csvs(self, year: int, force: bool = False) -> bool:
        """
        Extrae solo los CSVs relevantes del ZIP al directorio csv/{year}/.
        Si ya están extraídos los saltea.

        Args:
            year: Año del ZIP.
            force: Si True, re-extrae aunque existan.

        Returns:
            True si los CSVs están disponibles.
        """
        year_dir = os.path.join(self.work_dir, str(year))
        zip_path = os.path.join(year_dir, "masivo.zip")
        csv_dir  = os.path.join(year_dir, "csv")
        os.makedirs(csv_dir, exist_ok=True)

        if not os.path.exists(zip_path):
            self.logger.error("ZIP no encontrado: %s", zip_path)
            return False

        with zipfile.ZipFile(zip_path, "r") as zf:
            all_names = zf.namelist()
            to_extract = [
                n for n in all_names
                if os.path.basename(n) in RELEVANT_CSVS
            ]

            for name in to_extract:
                basename = os.path.basename(name)
                dest = os.path.join(csv_dir, basename)
                if os.path.exists(dest) and not force:
                    self.logger.info("  Ya existe: %s", basename)
                    continue
                self.logger.info("  Extrayendo: %s", basename)
                with zf.open(name) as src, open(dest, "wb") as dst:
                    dst.write(src.read())

        extracted = os.listdir(csv_dir)
        self.logger.info("CSVs disponibles para %d: %s", year, extracted)
        return True

    # ─────────────────────────────────────────────────────────────
    # Mapa de IDs — puente entre CSVs y nro_licitacion
    # ─────────────────────────────────────────────────────────────

    @error_handling(default_return={})
    def build_id_map(self, year: int) -> dict:
        """
        Construye el mapa compiled_release_id → nro_licitacion
        leyendo records.csv en chunks.

        El compiled_release_id es la clave común en todos los CSVs.
        El nro_licitacion es la PK en nuestra base de datos.

        Args:
            year: Año del dataset.

        Returns:
            Dict {compiled_release_id: nro_licitacion}
        """
        if year in self._id_maps:
            return self._id_maps[year]

        csv_path = self._csv_path(year, "records.csv")
        if not os.path.exists(csv_path):
            self.logger.error("records.csv no encontrado para año %d", year)
            return {}

        id_map = {}
        for chunk in pd.read_csv(
            csv_path,
            usecols=RECORDS_ID_COLS,
            chunksize=CHUNK_SIZE,
            dtype=str,
            low_memory=False,
        ):
            release_col = "compiledRelease/id"
            tender_col  = "compiledRelease/tender/id"

            chunk = chunk.dropna(subset=[release_col, tender_col])
            batch = dict(zip(chunk[release_col], chunk[tender_col]))
            id_map.update(batch)

        self.logger.info(
            "Mapa de IDs para %d: %d entradas", year, len(id_map)
        )
        self._id_maps[year] = id_map
        return id_map

    # ─────────────────────────────────────────────────────────────
    # Iteradores de CSV en chunks
    # ─────────────────────────────────────────────────────────────

    def iter_csv(
        self,
        year: int,
        filename: str,
        usecols: Optional[list] = None,
        dtype: Optional[dict] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Itera un CSV del año dado en chunks de CHUNK_SIZE filas.
        Agrega la columna 'nro_licitacion' usando el mapa de IDs.
        Agrega la columna 'year' con el valor del año.

        Args:
            year: Año del dataset.
            filename: Nombre del CSV (ej: "awards.csv").
            usecols: Lista de columnas a leer (None = todas).
            dtype: Tipos de columnas (None = inferir como str).

        Yields:
            DataFrame con chunk de filas + nro_licitacion + year.
        """
        csv_path = self._csv_path(year, filename)
        if not os.path.exists(csv_path):
            self.logger.warning("CSV no encontrado: %s (año %d)", filename, year)
            return
        if os.path.getsize(csv_path) == 0:
            self.logger.warning("CSV vacío: %s (año %d), saltando", filename, year)
            return

        id_map = self.build_id_map(year)
        release_col = "compiledRelease/id"

        # Asegurar que la columna de release siempre se lea
        cols = usecols
        if cols and release_col not in cols:
            cols = [release_col] + cols

        effective_dtype = dtype or {}
        if release_col not in effective_dtype:
            effective_dtype[release_col] = str

        total_rows = 0
        for chunk in pd.read_csv(
            csv_path,
            usecols=cols,
            chunksize=CHUNK_SIZE,
            dtype=effective_dtype,
            low_memory=False,
        ):
            # Agregar nro_licitacion desde el mapa
            chunk["nro_licitacion"] = chunk[release_col].map(id_map)
            chunk["year"] = year

            total_rows += len(chunk)
            self.logger.debug(
                "  %s año %d: chunk %d filas (total %d)",
                filename, year, len(chunk), total_rows
            )
            yield chunk

        self.logger.info(
            "Iteración completa: %s año %d — %d filas totales",
            filename, year, total_rows
        )

    def iter_records(self, year: int) -> Iterator[pd.DataFrame]:
        """Itera records.csv directamente (sin mapa, es la fuente del mapa)."""
        csv_path = self._csv_path(year, "records.csv")
        if not os.path.exists(csv_path):
            self.logger.warning("records.csv no encontrado para año %d", year)
            return

        for chunk in pd.read_csv(
            csv_path,
            chunksize=CHUNK_SIZE,
            dtype=str,
            low_memory=False,
        ):
            chunk["year"] = year
            yield chunk

    # ─────────────────────────────────────────────────────────────
    # Checkpoint — detectar años ya procesados
    # ─────────────────────────────────────────────────────────────

    def is_year_processed(self, year: int, checkpoint_path: str) -> bool:
        """
        Verifica si un año ya fue procesado leyendo el parquet checkpoint.

        Args:
            year: Año a verificar.
            checkpoint_path: Path al parquet de checkpoint.

        Returns:
            True si el año ya está en el checkpoint.
        """
        if not os.path.exists(checkpoint_path):
            return False
        try:
            df = pd.read_parquet(checkpoint_path, columns=["year"])
            return year in df["year"].values
        except Exception:
            return False

    def mark_year_processed(self, year: int, checkpoint_path: str) -> None:
        """Marca un año como procesado en el checkpoint parquet."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
        new_row = pd.DataFrame([{"year": year}])

        if os.path.exists(checkpoint_path):
            existing = pd.read_parquet(checkpoint_path)
            combined = pd.concat([existing, new_row], ignore_index=True)
        else:
            combined = new_row

        pq.write_table(
            pa.Table.from_pandas(combined, preserve_index=False),
            checkpoint_path,
            compression="snappy",
        )
        self.logger.info("Año %d marcado como procesado en %s", year, checkpoint_path)

    # ─────────────────────────────────────────────────────────────
    # Helper interno
    # ─────────────────────────────────────────────────────────────

    def _csv_path(self, year: int, filename: str) -> str:
        return os.path.join(self.work_dir, str(year), "csv", filename)