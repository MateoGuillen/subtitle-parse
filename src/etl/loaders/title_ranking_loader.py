"""Loader for title ranking pipeline — saves CSV and Markdown report."""

import os
from typing import List
import pandas as pd
from src.utils.logging_utils import setup_logger


class TitleRankingLoader:
    """
    Persists the title ranking to a CSV file and generates a human-readable
    Markdown report with justification for the top-20 titles.
    """

    def __init__(self, output_dir: str):
        """
        Args:
            output_dir: Directory where CSV and report will be written.
        """
        self.output_dir = output_dir
        self.logger = setup_logger(__name__)

    def save_csv(self, ranking: pd.DataFrame, filename: str = "title_ranking.csv") -> str:
        """Save the full ranking to a CSV file."""
        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, filename)
        ranking.to_csv(path, index=False, float_format="%.6f")
        self.logger.info("Ranking CSV saved: %s", path)
        return path

    def save_report(
        self, ranking: pd.DataFrame, top_n: int = 20,
        filename: str = "title_ranking_report.md",
    ) -> str:
        """Generate a Markdown report with justification for the top-*top_n*
        titles."""
        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, filename)

        top = ranking.head(top_n)

        lines: List[str] = []
        lines.append("# Title Anomaly-Detection Relevance Ranking")
        lines.append("")
        # Detect v2.2 compatibility
        is_v22 = "score_boilerplate" in ranking.columns and "score_numeric" in ranking.columns
        if is_v22:
            lines.append(
                "Ranking generated from **eight** complementary strategies (v2.2): "
                "six legacy + Boilerplate Penalty + Numeric Content (see methodology below)."
            )
        else:
            lines.append(
                "Ranking generated from six complementary strategies "
                "(see methodology below)."
            )
        lines.append("")

        # Summary table
        lines.append("## Top-%d Ranking Summary" % top_n)
        lines.append("")
        if is_v22:
            lines.append(
                "| Rank | Title | Total | Outlier | RF | Contextual | "
                "Section IF | Doc Corr | Economic | Boilerplate | Numeric | Descart |"
            )
            lines.append(
                "|------|-------|-------|---------|----|------------|"
                "------------|----------|----------|-------------|---------|---------|"
            )
            for _, row in top.iterrows():
                lines.append(
                    "| %d | %s | %.4f | %.4f | %.4f | %.4f | %.4f | %.4f | %.4f | %.4f | %.4f | %s |"
                    % (
                        row["rank"],
                        row["display_name"],
                        row["score_total"],
                        row["score_outlier"],
                        row["score_rf"],
                        row["score_contextual"],
                        row["score_section_if"],
                        row["score_corr"],
                        row.get("score_economic", 0.0),
                        row.get("score_boilerplate", 0.0),
                        row.get("score_numeric", 0.0),
                        "yes" if row.get("descartado_boilerplate", False) else "",
                    )
                )
        else:
            lines.append(
                "| Rank | Title | Total Score | Outlier Freq | RF Import | "
                "Contextual | Section IF | Doc Corr | Economic Risk |"
            )
            lines.append(
                "|------|-------|-------------|--------------|-----------|"
                "------------|------------|----------|---------------|"
            )
            for _, row in top.iterrows():
                lines.append(
                    "| %d | %s | %.4f | %.4f | %.4f | %.4f | %.4f | %.4f | %.4f |"
                    % (
                        row["rank"],
                        row["display_name"],
                        row["score_total"],
                        row["score_outlier"],
                        row["score_rf"],
                        row["score_contextual"],
                        row["score_section_if"],
                        row["score_corr"],
                        row.get("score_economic", 0.0),
                    )
                )
        lines.append("")

        # Per-title justification
        lines.append("## Per-Title Justification")
        lines.append("")
        for _, row in top.iterrows():
            lines.append("### %d. %s (score: %.4f)" % (row["rank"], row["display_name"], row["score_total"]))
            lines.append("")
            lines.extend(self._justification_lines(row))
            lines.append("")

        # Methodology
        lines.append("---")
        lines.append("## Methodology")
        lines.append("")
        if is_v22:
            lines.append("**v2.2** — Eight strategies combined with fixed weights (sum=1.00):")
            lines.append("")
            lines.append("| Strategy | Weight | Description |")
            lines.append("|----------|--------|-------------|")
            lines.append("| Outlier Frequency (Tukey IQR) | 0.10 | %% of documents where `len_*`/`tok_*` fall outside [Q1-1.5×IQR, Q3+1.5×IQR] |")
            lines.append("| Proxy Random Forest | 0.15 | Gini importance from RF trained to distinguish original vs. noise-corrupted data |")
            lines.append("| Contextual Anomalies | 0.15 | %% of documents with within-category `|z|>3` for `len_*`/`tok_*` |")
            lines.append("| Section-level Isolation Forest | 0.10 | %% of sections per title flagged as anomalous (bottom 5%% of IF scores) |")
            lines.append("| Document IF Correlation | 0.10 | Absolute difference in mean document-level IF score between `has_*=1` vs `has_*=0` |")
            lines.append("| Economic Risk Correlation | 0.20 | Mean absolute Pearson correlation between `has_*` and economic risk indicators (single bidder, overbudget, collusion, protests) |")
            lines.append("| **Boilerplate Penalty** (v2.2) | 0.10 | `1 - max(md5_top1_share, post_merge_top1_share)` per title. Titles with >90% identical text are heavily penalised. |")
            lines.append("| **Numeric Content** (v2.2) | 0.10 | Mean per-section regex hits for parametric patterns (Gs. monto, %, días, fechas, multa/anticipo). |")
            lines.append("")
            lines.append("All sub-scores are min-max normalised to [0,1] before combination.")
            lines.append("")
            lines.append("**v2.2 reformulation rationale** (see `docs/mejoras_pipeline_seleccion_titulos.md`):")
            lines.append("- Legacy v2.0 weights premiated structural presence (`has_*`) and length proxies that are themselves proxies of boilerplate; v2.2 reduces those weights and adds explicit boilerplate/numeric signals.")
            lines.append("- Generated from `data/processed/title_diagnostics/title_diagnostics.csv` (Fase 1).")
        else:
            lines.append("Six strategies are combined with fixed weights:")
            lines.append("")
            lines.append("| Strategy | Weight | Description |")
            lines.append("|----------|--------|-------------|")
            lines.append(
                "| Outlier Frequency (Tukey IQR) | 0.20 | %% of documents where "
                "`len_*`/`tok_*` fall outside [Q1-1.5×IQR, Q3+1.5×IQR] |"
            )
            lines.append(
                "| Proxy Random Forest | 0.25 | Gini importance from RF trained "
                "to distinguish original vs. noise-corrupted data |"
            )
            lines.append(
                "| Contextual Anomalies | 0.15 | %% of documents with within-"
                "category `|z|>3` for `len_*`/`tok_*` |"
            )
            lines.append(
                "| Section-level Isolation Forest | 0.15 | %% of sections per "
                "title flagged as anomalous (bottom 5%% of IF scores) |"
            )
            lines.append(
                "| Document IF Correlation | 0.10 | Absolute difference in mean "
                "document-level IF score between `has_*=1` vs `has_*=0` |"
            )
            lines.append(
                "| Economic Risk Correlation | 0.15 | Mean absolute Pearson "
                "correlation between `has_*` and economic risk indicators "
                "(single bidder, overbudget, collusion, protests) |"
            )
            lines.append("")
            lines.append(
                "All sub-scores are min-max normalised to [0,1] before "
                "combination."
            )

        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines))

        self.logger.info("Report saved: %s", path)
        return path

    # ------------------------------------------------------------------
    # Justification templates
    # ------------------------------------------------------------------

    @staticmethod
    def _justification_lines(row: pd.Series) -> List[str]:
        """Generate a few bullet-point lines justifying why a title ranks
        where it does, based on its partial scores."""
        slug = row.get("display_name", "unknown")
        parts = {
            "outlier": row.get("score_outlier", 0),
            "rf": row.get("score_rf", 0),
            "ctx": row.get("score_contextual", 0),
            "sec": row.get("score_section_if", 0),
            "corr": row.get("score_corr", 0),
            "econ": row.get("score_economic", 0),
        }
        top_part = max(parts, key=parts.get)

        lines = []

        # --- outlier ---
        if parts["outlier"] >= 0.7:
            lines.append(
                "- **Alta frecuencia de outliers estructurales** (%.2f): "
                "los valores de longitud/tokens para este título presentan "
                "valores extremos frecuentes, lo que indica que su "
                "extensión documental es muy variable y puede delatar "
                "pliegos atípicos." % parts["outlier"]
            )
        elif parts["outlier"] >= 0.4:
            lines.append(
                "- **Outlier freq moderada** (%.2f): existe variabilidad "
                "moderada en la extensión de este título." % parts["outlier"]
            )
        else:
            lines.append(
                "- **Baja frecuencia de outliers** (%.2f): la extensión "
                "de este título es relativamente homogénea entre "
                "documentos." % parts["outlier"]
            )

        # --- RF ---
        if parts["rf"] >= 0.7:
            lines.append(
                "- **Alta importancia en RF proxy** (%.2f): el modelo "
                "encuentra que este título es muy discriminativo para "
                "distinguir documentos originales de versiones "
                "artificialmente alteradas." % parts["rf"]
            )
        elif parts["rf"] >= 0.4:
            lines.append(
                "- **Importancia RF moderada** (%.2f): contribuye de "
                "forma apreciable a la separación entre clases." % parts["rf"]
            )
        else:
            lines.append(
                "- **Baja importancia en RF** (%.2f): es poco "
                "discriminativo en el espacio de features "
                "original vs. ruido." % parts["rf"]
            )

        # --- contextual ---
        if parts["ctx"] >= 0.7:
            lines.append(
                "- **Fuerte efecto contextual** (%.2f): los valores de "
                "este título varían significativamente entre categorías, "
                "por lo que un valor inusual dentro de su grupo "
                "es señal de anomalía." % parts["ctx"]
            )
        elif parts["ctx"] >= 0.4:
            lines.append(
                "- **Efecto contextual moderado** (%.2f): cierta "
                "variación entre categorías." % parts["ctx"]
            )
        else:
            lines.append(
                "- **Bajo efecto contextual** (%.2f): las diferencias "
                "entre categorías son pequeñas." % parts["ctx"]
            )

        # --- section IF ---
        if parts["sec"] >= 0.7:
            lines.append(
                "- **Alta detección a nivel sección** (%.2f): las "
                "secciones con este título son frecuentemente marcadas "
                "como anómalas por Isolation Forest, indicando "
                "que sus propiedades numéricas se desvían del patrón "
                "típico." % parts["sec"]
            )
        elif parts["sec"] >= 0.4:
            lines.append(
                "- **Moderada detección en secciones** (%.2f): algunas "
                "secciones de este título presentan patrones "
                "numéricos atípicos." % parts["sec"]
            )
        else:
            lines.append(
                "- **Baja detección en secciones** (%.2f): las secciones "
                "con este título siguen el patrón general." % parts["sec"]
            )

        # --- corr ---
        if parts["corr"] >= 0.7:
            lines.append(
                "- **Alta correlación con anomalía documental** (%.2f): "
                "la presencia/ausencia de este título está fuertemente "
                "asociada con el score de anomalía a nivel de "
                "documento." % parts["corr"]
            )
        elif parts["corr"] >= 0.4:
            lines.append(
                "- **Correlación moderada con anomalía global** (%.2f): "
                "hay cierta asociación entre este título y la "
                "anomalía del pliego." % parts["corr"]
            )
        else:
            lines.append(
                "- **Baja correlación con anomalía global** (%.2f): "
                "la presencia de este título no se relaciona "
                "significativamente con la anomalía documental." % parts["corr"]
            )

        # --- economic ---
        if parts["econ"] >= 0.7:
            lines.append(
                "- **Alto riesgo económico asociado** (%.2f): la presencia de "
                "este título se correlaciona fuertemente con indicadores de "
                "riesgo económico como oferta única, sobrecostos, colusión o "
                "protestas." % parts["econ"]
            )
        elif parts["econ"] >= 0.4:
            lines.append(
                "- **Riesgo económico moderado** (%.2f): existe cierta "
                "asociación con señales de corrupción económica." % parts["econ"]
            )
        else:
            lines.append(
                "- **Bajo riesgo económico** (%.2f): no se observa una "
                "asociación fuerte con indicadores económicos de riesgo."
                % parts["econ"]
            )

        if top_part == "outlier":
            lines.append(
                "*Factor dominante: la variabilidad estructural de este "
                "título lo convierte en un fuerte indicador de anomalías "
                "documentales.*"
            )
        elif top_part == "rf":
            lines.append(
                "*Factor dominante: la importancia en el modelo proxy "
                "sugiere que alteraciones en este título son difíciles "
                "de distinguir de datos reales, lo que lo hace relevante "
                "para detección.*"
            )
        elif top_part == "ctx":
            lines.append(
                "*Factor dominante: el contexto categórico es crucial "
                "para evaluar la normalidad de este título.*"
            )
        elif top_part == "sec":
            lines.append(
                "*Factor dominante: el perfil numérico de sus secciones "
                "es intrínsecamente anómalo.*"
            )
        elif top_part == "corr":
            lines.append(
                "*Factor dominante: la presencia/ausencia de este título "
                "está fuertemente ligada a la anomalía global del "
                "documento.*"
            )
        elif top_part == "econ":
            lines.append(
                "*Factor dominante: este título está asociado con patrones "
                "económicos de riesgo, lo que lo hace relevante para "
                "detectar corrupción.*"
            )

        return lines
