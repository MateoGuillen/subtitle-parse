"""Loader for the section clustering & schema pipeline."""

import json
import os
from typing import Any, Dict, List
from src.utils.logging_utils import setup_logger


class SectionClusteringLoader:
    """
    Saves the master schemas JSON and per-title reports.
    """

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.logger = setup_logger(__name__)

    def save_master_schema(
        self,
        results: Dict[str, Dict[str, Any]],
        filename: str = "master_schemas.json",
    ) -> str:
        """Save the master schemas JSON file."""
        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, filename)

        output: Dict[str, Any] = {}
        for title, data in results.items():
            output[title] = {
                "clusters": data.get("n_clusters", 0),
                "total_sections": data.get("total_sections", 0),
                "silhouette_scores": data.get("silhouette_scores", {}),
                "cluster_counts": data.get("cluster_counts", {}),
                "samples_per_cluster": self._serialize_samples(
                    data.get("samples_per_cluster", {})
                ),
                "json_schema": data.get("json_schema", {}),
                "schema_justification": data.get(
                    "schema_justification", ""
                ),
                "extraction_prompt": data.get("extraction_prompt", ""),
                "cluster_report": data.get("cluster_report", ""),
                "selected_method": data.get("selected_method", "kmeans"),
                "selected_embedding_model": data.get(
                    "selected_embedding_model", ""
                ),
                "selected_umap_params": data.get("selected_umap_params", {}),
                "clustering_comparison": data.get(
                    "clustering_comparison", []
                ),
            }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        self.logger.info("Master schemas saved: %s", path)
        return path

    def save_title_report(
        self,
        title: str,
        data: Dict[str, Any],
        filename: str = None,
    ) -> str:
        """Save a human-readable Markdown report per title."""
        os.makedirs(self.output_dir, exist_ok=True)

        if filename is None:
            safe_name = title.replace(" ", "_").replace("/", "_")[:60]
            filename = f"report_{safe_name}.md"

        path = os.path.join(self.output_dir, filename)

        lines: List[str] = []
        lines.append(f"# Reporte: {title}")
        lines.append("")
        lines.append(
            f"- Total secciones: {data.get('total_sections', 0)}"
        )
        lines.append(
            f"- Número de clusters: {data.get('n_clusters', 0)}"
        )
        sel_method = data.get("selected_method", "kmeans")
        sel_model = data.get("selected_embedding_model", "")
        sel_umap = data.get("selected_umap_params", {})
        lines.append(f"- Método seleccionado: {sel_method}")
        lines.append(f"- Embedding model: {sel_model}")
        lines.append(
            f"- UMAP: n_components={sel_umap.get('n_components', '?')}, "
            f"n_neighbors={sel_umap.get('n_neighbors', '?')}"
        )
        lines.append("")

        # Comparison table
        comparison = data.get("clustering_comparison", [])
        if comparison:
            lines.append("## Comparación de enfoques de clustering")
            lines.append("")
            lines.append(
                "| Método | Embedding | UMAP (comp, neigh) | Clusters | Silhouette | DB | CH | Score |"
            )
            lines.append(
                "|--------|-----------|--------------------|----------|------------|----|-----|-------|"
            )
            for entry in comparison:
                model_short = entry.get("embedding_model", "").split("/")[-1][:25]
                method = entry.get("clustering_method", "?")
                nc = entry.get("umap_n_components", "?")
                nn = entry.get("umap_n_neighbors", "?")
                k = entry.get("n_clusters", "?")
                sil = f"{entry.get('silhouette', -1):.4f}"
                db = f"{entry.get('davies_bouldin', -1):.2f}"
                ch = f"{entry.get('calinski_harabasz', -1):.0f}"
                score = f"{entry.get('combined_score', -1):.4f}"
                lines.append(
                    f"| {method} | {model_short} | ({nc}, {nn}) | {k} | {sil} | {db} | {ch} | {score} |"
                )
            lines.append("")
            lines.append("")

        # Cluster distribution
        cluster_counts = data.get("cluster_counts", {})
        if cluster_counts:
            lines.append("## Distribución de clusters")
            lines.append("")
            lines.append("| Cluster | Cantidad | Porcentaje |")
            lines.append("|---------|----------|------------|")
            total = sum(cluster_counts.values())
            for cid in sorted(cluster_counts, key=lambda x: int(x)):
                cnt = cluster_counts[cid]
                pct = 100.0 * cnt / total if total else 0
                lines.append(f"| {cid} | {cnt} | {pct:.1f}% |")
            lines.append("")

        # Samples
        samples = data.get("samples_per_cluster", {})
        if samples:
            lines.append("## Muestras por cluster")
            lines.append("")
            for cid in sorted(samples, key=lambda x: int(x)):
                sample_list = samples[cid]
                lines.append(f"### Cluster {cid}")
                lines.append("")
                for i, s in enumerate(sample_list, 1):
                    tag = "**[EXTREMO]**" if s.get("is_extreme") else "Representativo"
                    nro = s.get("nro_licitacion", "?")
                    year = s.get("year", "?")
                    text = s.get("text", "")[:500]
                    lines.append(
                        f"{i}. {tag} - {nro} (year={year})"
                    )
                    lines.append("")
                    lines.append(f"   ```\n   {text}\n   ```")
                    lines.append("")
                lines.append("")

        # Schema
        schema = data.get("json_schema", {})
        if schema:
            lines.append("## Esquema JSON propuesto")
            lines.append("")
            lines.append("```json")
            lines.append(json.dumps(schema, indent=2, ensure_ascii=False))
            lines.append("```")
            lines.append("")

            justification = data.get("schema_justification", "")
            if justification:
                lines.append("### Justificación")
                lines.append("")
                lines.append(justification)
                lines.append("")

        # Extraction prompt
        prompt = data.get("extraction_prompt", "")
        if prompt:
            lines.append("## Prompt de extracción")
            lines.append("")
            lines.append("```")
            lines.append(prompt)
            lines.append("```")
            lines.append("")

        # Cluster report
        report = data.get("cluster_report", "")
        if report:
            lines.append("## Reporte de clusters")
            lines.append("")
            lines.append("```")
            lines.append(report)
            lines.append("```")
            lines.append("")

        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        self.logger.info("Report saved: %s", path)
        return path

    def save_clustering_comparison(
        self,
        results: Dict[str, Dict[str, Any]],
        filename: str = "clustering_comparison.json",
    ) -> str:
        """Save clustering comparison results per title."""
        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, filename)

        output: Dict[str, Any] = {}
        for title, data in results.items():
            comparison = data.get("clustering_comparison", [])
            output[title] = {
                "selected_method": data.get("selected_method", "kmeans"),
                "selected_embedding_model": data.get("selected_embedding_model", ""),
                "selected_umap_params": data.get("selected_umap_params", {}),
                "n_clusters": data.get("n_clusters", 0),
                "total_sections": data.get("total_sections", 0),
                "all_results": comparison,
            }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        self.logger.info("Clustering comparison saved: %s", path)
        return path

    def save_chat_prompts(
        self,
        prompts: List[Dict[str, Any]],
        filename: str = "llm_chat_prompts.json",
    ) -> str:
        """Save chat prompts for manual LLM schema design."""
        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(prompts, f, indent=2, ensure_ascii=False)
        self.logger.info("Chat prompts saved: %s", path)
        return path

    def save_cluster_samples(
        self,
        results: Dict[str, Dict[str, Any]],
        filename: str,
    ) -> str:
        """Save all samples per cluster with stats: unique_texts, pct, sorted by size desc."""
        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, filename)

        output: Dict[str, Any] = {}
        for title, data in results.items():
            samples = data.get("samples_per_cluster", {})
            counts = data.get("cluster_counts", {})
            total = data.get("total_sections", 0)

            sorted_cids = sorted(counts, key=lambda c: counts[c], reverse=True)
            clusters: List[Dict[str, Any]] = []
            for cid in sorted_cids:
                cluster_samples = samples.get(cid, [])
                unique = len({s.get("text", "")[:300] for s in cluster_samples})
                clusters.append({
                    "cluster_id": cid,
                    "count": counts[cid],
                    "pct": round(100.0 * counts[cid] / total, 1) if total else 0,
                    "unique_texts": unique,
                    "samples": [
                        {
                            "nro_licitacion": s.get("nro_licitacion", ""),
                            "year": s.get("year"),
                            "category_id": s.get("category_id"),
                            "text": s.get("text", ""),
                            "is_extreme": s.get("is_extreme", False),
                        }
                        for s in cluster_samples
                    ],
                })

            output[title] = {
                "total_sections": total,
                "n_clusters": len(clusters),
                "clusters": clusters,
            }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        self.logger.info("Cluster samples saved: %s", path)
        return path

    # ---- helpers ----------------------------------------------------------

    @staticmethod
    def _serialize_samples(
        samples: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Ensure samples are JSON-serializable."""
        result: Dict[str, List[Dict[str, Any]]] = {}
        for cid, sample_list in samples.items():
            clean: List[Dict[str, Any]] = []
            for s in sample_list:
                clean.append(
                    {
                        "nro_licitacion": str(s.get("nro_licitacion", "")),
                        "year": s.get("year"),
                        "category_id": s.get("category_id"),
                        "text": s.get("text", "")[:2000],
                        "is_extreme": bool(s.get("is_extreme", False)),
                    }
                )
            result[str(cid)] = clean
        return result
