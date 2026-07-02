"""Transformer for PDF content data."""

import re
import unicodedata
import logging
from typing import Tuple, List, Dict, Optional
from collections import defaultdict
import pandas as pd
import pyarrow as pa
from src.utils.error_handler import error_handling
from src.core.entities.pdf_models import ContentSection


class PdfContentTransformer:
    """
    Transformer for PDF content data.

    This class is responsible for transforming PDF lines and outlines data,
    matching outlines with lines, and extracting content sections.

    Attributes:
        logger (logging.Logger): Logger for this class.
        input_external_dir (str): Directory for external input data.
    """

    def __init__(self):
        """Initialize the PdfContentTransformer."""
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def clean_text(texto: str) -> str:
        """
        Clean duplicate text and replace hyphens.

        Args:
            texto (str): Text to clean.

        Returns:
            str: Cleaned text.
        """
        if pd.isna(texto):
            return texto
        return re.sub(r"\s+", " ", texto).strip()  # Clean extra spaces

    @staticmethod
    def _normalize_exact(text: str) -> str:
        """
        Normalize text for exact matching: lowercase, strip accents and punctuation.

        Args:
            text (str): Text to normalize.

        Returns:
            str: Normalized text.
        """
        if pd.isna(text):
            return ""
        text = unicodedata.normalize("NFKD", str(text))
        text = text.encode("ascii", "ignore").decode("ascii")
        text = re.sub(r"[^\w\s]", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text.lower()

    @error_handling(default_return=None)
    def match_titles_with_lines(
        self, outlines_df: pd.DataFrame, pdf_lines_df: pd.DataFrame
    ) -> pd.DataFrame:
        self.logger.info("Cleaning text fields...")
        outlines_df = outlines_df.copy()
        outlines_df["clean_title"] = outlines_df["title"].apply(self.clean_text)
        pdf_lines_df["clean_line_text"] = pdf_lines_df["line_text"].apply(
            self.clean_text
        )

        self.logger.info("Merging datasets...")
        merged_df = pd.merge(
            outlines_df,
            pdf_lines_df[
                [
                    "document_id",
                    "page_number",
                    "line_text",
                    "clean_line_text",
                    "line_number",
                ]
            ],
            left_on=["document_id", "page", "clean_title"],
            right_on=["document_id", "page_number", "clean_line_text"],
            how="left",
        )

        merged_df = merged_df.drop(
            ["page_number", "line_text", "clean_title", "clean_line_text"], axis=1
        )

        # ✅ FIX: eliminar duplicados conservando el primer match por outline
        original_len = len(merged_df)
        merge_keys = [col for col in outlines_df.columns if col != "clean_title"]
        merged_df = merged_df.sort_values(
            "line_number", na_position="last"
        ).drop_duplicates(subset=merge_keys, keep="first")
        duplicates_removed = original_len - len(merged_df)
        if duplicates_removed > 0:
            self.logger.warning(
                "Removed %d duplicate outline matches", duplicates_removed
            )

        total_matches = merged_df["line_number"].notna().sum()
        match_rate = (total_matches / len(merged_df)) * 100 if len(merged_df) > 0 else 0
        self.logger.info("Total outline entries: %s", len(merged_df))
        self.logger.info("Successfully matched: %s", total_matches)
        self.logger.info("Match rate: %.2f%%", match_rate)

        # Fallback: buscar títulos no matcheados mediante normalización
        unmatched = merged_df[merged_df["line_number"].isna()]
        if len(unmatched) > 0:
            self.logger.info(
                "Fallback searching for %d unmatched outlines via normalized matching...",
                len(unmatched),
            )

            pdf_copy = pdf_lines_df.copy()
            pdf_copy["norm_text"] = pdf_copy["line_text"].apply(
                lambda x: self._normalize_exact(x) if pd.notna(x) else ""
            )

            page_lookup = {}
            stripped_lookup = {}
            for _, row in (
                pdf_copy.sort_values(
                    ["document_id", "page_number", "line_number"]
                ).iterrows()
            ):
                doc = row["document_id"]
                pg = row["page_number"]
                norm = row["norm_text"]
                if norm:
                    key = (doc, pg)
                    if key not in page_lookup:
                        page_lookup[key] = {}
                        stripped_lookup[key] = {}
                    if norm not in page_lookup[key]:
                        page_lookup[key][norm] = row["line_number"]
                    stripped = re.sub(r"^[\d\s]+", "", norm).strip()
                    if stripped and stripped != norm and stripped not in stripped_lookup[key]:
                        stripped_lookup[key][stripped] = row["line_number"]

            fallback_matched = 0
            for idx in unmatched.index:
                doc = merged_df.loc[idx, "document_id"]
                pg = int(merged_df.loc[idx, "page"])
                title = merged_df.loc[idx, "title"]
                norm_title = self._normalize_exact(title)

                found = False
                for offset in range(0, 6):
                    for sign in [1, -1] if offset > 0 else [0]:
                        search_page = pg + sign * offset
                        if search_page < 1:
                            continue
                        key = (doc, search_page)
                        if key in page_lookup:
                            if norm_title in page_lookup[key]:
                                merged_df.loc[idx, "page"] = search_page
                                merged_df.loc[idx, "line_number"] = page_lookup[key][
                                    norm_title
                                ]
                                fallback_matched += 1
                                found = True
                                break
                            if norm_title in stripped_lookup.get(key, {}):
                                merged_df.loc[idx, "page"] = search_page
                                merged_df.loc[idx, "line_number"] = stripped_lookup[
                                    key
                                ][norm_title]
                                fallback_matched += 1
                                found = True
                                break
                    if found:
                        break

            self.logger.info(
                "Fallback matched %d / %d outlines",
                fallback_matched,
                len(unmatched),
            )

        return merged_df

    @error_handling(default_return=(None, None))
    def preprocess_dataframes(self, pdf_lines_df, outlines_df):
        # ✅ Reemplazar iterrows por groupby vectorizado
        pdf_lines_dict = defaultdict(lambda: defaultdict(list))

        grouped = pdf_lines_df.sort_values("line_number").groupby(
            ["document_id", "page_number"], sort=False
        )
        for (doc_id, page_num), group in grouped:
            pdf_lines_dict[doc_id][int(page_num)] = group[
                ["line_number", "line_text"]
            ].to_dict("records")

        outlines_df = outlines_df.sort_values(["document_id", "page", "line_number"])
        return pdf_lines_dict, outlines_df

    @error_handling(default_return=[])
    def get_section_content(
        self,
        pdf_lines_dict: Dict,
        doc_id: str,
        start_page: int,
        end_page: int,
        start_line: int,
        end_line: Optional[int],
    ) -> List[str]:
        """
        Extract content between two points in the PDF using preprocessed dictionary.

        Args:
            pdf_lines_dict (Dict): Dictionary with PDF lines.
            doc_id (str): Document ID.
            start_page (int): Start page.
            end_page (int): End page.
            start_line (int): Start line.
            end_line (Optional[int]): End line.

        Returns:
            List[str]: Extracted content.
        """
        content = []

        for page in range(start_page, end_page + 1):
            if page not in pdf_lines_dict[doc_id]:
                continue

            page_lines = pdf_lines_dict[doc_id][page]

            for line in page_lines:
                line_num = line["line_number"]

                if page == start_page and line_num < start_line:
                    continue
                if page == end_page and end_line is not None and line_num >= end_line:
                    break

                content.append(line["line_text"])

        return content

    @error_handling(default_return=[])
    def extract_content_sections(
        self, pdf_lines_dict: Dict, outlines_df: pd.DataFrame
    ) -> List[ContentSection]:
        """
        Extract content sections using preprocessed data.

        Args:
            pdf_lines_dict (Dict): Dictionary with PDF lines.
            outlines_df (pd.DataFrame): DataFrame with outlines.

        Returns:
            List[ContentSection]: Extracted content sections.
        """
        sections = []

        def process_document_outlines(doc_id, doc_outlines):
            doc_rows = doc_outlines.to_dict("records")
            if not doc_rows:
                return

            # Depth 1 items son boundaries virtuales (títulos generales).
            # No generan sección propia — forzar line_number=None
            # para que el loop los salte, pero sirvan como límite en
            # determine_section_end (Caso 3: diferente página, sin match)
            for r in doc_rows:
                if r.get("depth") == 1:
                    r["line_number"] = None

            # Ordenar por (page, line_number virtual).
            # Depth-1 (line_number=None → 0) queda antes que depth-2
            # en la misma página, correcto como header
            doc_rows.sort(key=lambda r: (r.get("page", 0), r.get("line_number") or 0))

            # Synthetic end-of-document marker para capturar TODO el contenido
            # del último título real del documento
            if doc_id in pdf_lines_dict:
                pages = pdf_lines_dict[doc_id]
                last_page = max(pages.keys())
                last_line = max(l["line_number"] for l in pages[last_page])
                last_outline = doc_rows[-1]
                doc_rows.append({
                    "document_id": doc_id,
                    "page": last_page,
                    "line_number": last_line + 1,
                    "title": "__END_OF_DOCUMENT__",
                    "depth": 0,
                    "nro_licitacion": last_outline.get("nro_licitacion"),
                    "category_id": last_outline.get("category_id"),
                    "year": last_outline.get("year"),
                })

            for i, current in enumerate(doc_rows):
                if pd.isna(current["line_number"]):
                    continue
                if current.get("title") == "__END_OF_DOCUMENT__":
                    continue

                end_page, end_line = determine_section_end(i, doc_rows, current)

                content = self.get_section_content(
                    pdf_lines_dict,
                    doc_id,
                    current["page"],
                    end_page,
                    int(current["line_number"]),
                    end_line,
                )

                section = create_content_section(current, content, end_line)
                sections.append(section)

        def determine_section_end(i, doc_rows, current):
            if i >= len(doc_rows) - 1:
                return current["page"] + 1, None

            next_outline = doc_rows[i + 1]

            # Caso 1+2 (unificado): el siguiente outline tiene line_number
            # resuelto → cortar exactamente en su (page, line_number), sea
            # la misma u otra página.
            # ANTES el Caso 2 cortaba en (next.page - 1, None), perdiendo el
            # contenido desde el título actual hasta la línea del título
            # siguiente (~115K secciones con content_length=1, line_end=-1).
            # get_section_content() aplica end_line solo en end_page, por lo
            # que el contenido intermedio se preserva completo hasta la línea
            # del título siguiente.
            if not pd.isna(next_outline["line_number"]):
                if next_outline["page"] != current["page"]:
                    self.logger.info(
                        "Caso 2: doc=%s title='%s' cutting at page %d line %d "
                        "(next '%s' on page %d)",
                        doc_id, current["title"],
                        next_outline["page"], int(next_outline["line_number"]),
                        next_outline["title"], next_outline["page"],
                    )
                return next_outline["page"], int(next_outline["line_number"])

            # Caso 3+4 (unificado): otra/s página/s SIN line_number →
            # buscar el PRÓXIMO outline con line_number como límite.
            # Esto evita perder páginas intermedias cuando hay outlines
            # no matcheados (depth-1 o fallback fallido) entre medio.
            if next_outline["page"] != current["page"]:
                self.logger.info(
                    "Caso 3: doc=%s title='%s' next '%s' at page %d unmatched, "
                    "skipping forward for next matched outline",
                    doc_id,
                    current["title"],
                    next_outline["title"],
                    next_outline["page"],
                )
            else:
                self.logger.warning(
                    "Caso 4: doc=%s title='%s' skipping '%s' on same page",
                    doc_id,
                    current["title"],
                    next_outline["title"],
                )
            for j in range(i + 2, len(doc_rows)):
                future = doc_rows[j]
                if not pd.isna(future["line_number"]):
                    return future["page"], int(future["line_number"])

            # Caso 5: no hay más outlines con line_number
            return current["page"] + 1, None

        def create_content_section(current, content, end_line):
            return ContentSection(
                title=current["title"],
                content=content,
                page=current["page"],
                line_start=current["line_number"],
                line_end=end_line,
                depth=current["depth"],
                document_id=doc_id,
                nro_licitacion=current["nro_licitacion"],
                category_id=current["category_id"],
                year=current["year"],
            )

        for doc_id, doc_outlines in outlines_df.groupby("document_id"):
            process_document_outlines(doc_id, doc_outlines)

        return sections

    @error_handling(default_return=None)
    def prepare_sections_dataframe(
        self, sections: List[ContentSection]
    ) -> pd.DataFrame:
        """
        Prepare DataFrame from content sections using the ContentSection schema.

        Args:
            sections (List[ContentSection]): Content sections.

        Returns:
            pd.DataFrame: DataFrame with content sections.
        """
        # Convertir cada sección a un diccionario usando to_dict()
        sections_data = [section.to_dict() for section in sections]

        # Crear DataFrame desde los diccionarios
        df = pd.DataFrame(sections_data)

        # Dedup por (document_id, title): conservar la fila con más contenido
        df = df.loc[df.groupby(["document_id", "title"])["content_length"].idxmax()].reset_index(drop=True)

        # Asegurarse de que los valores None en line_end se manejen correctamente
        if "line_end" in df.columns and df["line_end"].isna().any():
            df["line_end"] = df["line_end"].fillna(-1).astype(int)

        # Obtener los nombres de los campos del schema de manera correcta
        schema = ContentSection.get_schema()
        # En PyArrow, necesitamos usar schema.names para obtener los nombres de los campos
        schema_fields = schema.names

        # Mantener solo las columnas que están en el schema
        existing_fields = [col for col in schema_fields if col in df.columns]
        df = df[existing_fields]

        # Verificar si falta alguna columna del schema y añadirla con valores por defecto
        for field in schema_fields:
            if field not in df.columns:
                # Añadir columna con valor por defecto según el tipo de dato
                field_type = schema.field(field).type
                if pa.types.is_string(field_type):
                    df[field] = ""
                elif pa.types.is_integer(field_type):
                    df[field] = 0
                else:
                    df[field] = None

        return df
