"""Transformer for the section clustering & schema pipeline.

Handles:
- Embedding generation via Sentence‑Transformers
- UMAP dimensionality reduction
- K‑Means clustering (k∈[2,k_max] via silhouette + Davies-Bouldin + Calinski-Harabasz)
- Representative/extreme sampling per cluster
- Schema validation post-LLM with retry
- LLM‑driven JSON‑schema design and extraction‑prompt construction.
"""

import difflib
import json
from statistics import median
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import (
    silhouette_score,
    davies_bouldin_score,
    calinski_harabasz_score,
)
from sklearn.decomposition import PCA
from sentence_transformers import SentenceTransformer
from src.etl.transformers.llm_provider import LLMProvider
from src.utils.logging_utils import setup_logger
from src.utils.prompt_utils import (
    SCHEMA_DESIGN_SYSTEM_PROMPT,
    SCHEMA_VALIDATION_SYSTEM_PROMPT,
    EXTRACTION_SYSTEM_PROMPT,
    build_extraction_prompt_text,
)


class SectionClusteringTransformer:
    """
    Embeds, clusters, samples, and drives LLM schema/prompt generation
    for each title.
    """

    DEFAULT_UMAP_PARAMS = {"n_components": 10, "n_neighbors": 15}

    def __init__(
        self,
        llm_provider: LLMProvider,
        embedding_model: str = "paraphrase-multilingual-MiniLM-L12-v2",
        random_state: int = 42,
        k_range: Tuple[int, int] = (2, 20),
        validate_schema: bool = True,
        schema_validation_retries: int = 2,
        compare_embeddings: bool = False,
        embedding_models_to_test: Optional[List[str]] = None,
        umap_params_grid: Optional[List[Dict[str, Any]]] = None,
        skip_llm: bool = False,
    ):
        self.llm_provider = llm_provider
        self.embedding_model_name = embedding_model
        self.random_state = random_state
        self.k_range = k_range
        self.validate_schema = validate_schema
        self.schema_validation_retries = schema_validation_retries
        self.compare_embeddings = compare_embeddings
        self.embedding_models_to_test = embedding_models_to_test or []
        self.umap_params_grid = umap_params_grid or []
        self.skip_llm = skip_llm
        self.logger = setup_logger(__name__)
        self._embedder: Optional[SentenceTransformer] = None
        self._rng = np.random.RandomState(random_state)

    # ---- public API -------------------------------------------------------

    def process_title(
        self, title: str, df: pd.DataFrame, max_samples: int = 5000
    ) -> Dict[str, Any]:
        """Run the full clustering + LLM workflow for a single title.

        Args:
            title: Section title.
            df: DataFrame with columns ``nro_licitacion``, ``content_text``,
                ``year``, ``category_id``.
            max_samples: Maximum number of sections to use for clustering
                (to keep runtime manageable). If the DataFrame is larger,
                a random subset is taken.
        """
        self.logger.info("=" * 60)
        self.logger.info("Processing title: %s (%d sections)", title, len(df))
        self.logger.info("=" * 60)

        if len(df) < 3:
            self.logger.warning(
                "Too few sections (%d) for '%s'. Skipping clustering.",
                len(df),
                title,
            )
            return self._build_fallback(title, df)

        total_sections = len(df)
        if len(df) > max_samples:
            self.logger.info(
                "Subsampling to %d sections (from %d) for clustering speed.",
                max_samples,
                len(df),
            )
            df = df.sample(n=max_samples, random_state=self.random_state)

        texts = df["content_text"].tolist()
        n_samples = len(texts)

        # ---- Clustering: find best approach --------------------------------
        comparison_results: List[Dict[str, Any]] = []

        if self.compare_embeddings:
            models_to_try = (
                [self.embedding_model_name]
                + [
                    m
                    for m in self.embedding_models_to_test
                    if m != self.embedding_model_name
                ]
            )
            configs_to_try = self.umap_params_grid or [self.DEFAULT_UMAP_PARAMS]
            best = self._run_grid_comparison(
                title, texts, models_to_try, configs_to_try
            )
            comparison_results = best["comparison_results"]
            cluster_labels = best["labels"]
            centroids = best.get("centroids")
            kmeans_obj = best.get("kmeans")
            reduced = best["reduced"]
            combined_scores = best["combined_scores"]
            selected_method = best["selected_method"]
            selected_embedding = best["selected_embedding"]
            selected_umap = best["selected_umap"]
        else:
            # Single-model path
            embeddings = self._get_embeddings(texts)
            reduced = self._reduce_dimensions(
                embeddings, **self.DEFAULT_UMAP_PARAMS
            )

            # Combined k selection
            (
                optimal_k,
                combined_scores,
            ) = self._find_optimal_k_combined(reduced)
            k_sil = {
                k: v.get("silhouette", 0) for k, v in combined_scores.items()
            }
            self.logger.info(
                "Optimal k=%d for '%s' (silhouettes: %s)",
                optimal_k,
                title,
                {str(k): f"{s:.4f}" for k, s in k_sil.items()},
            )

            # K-Means
            km_labels, kmeans_obj = self._cluster(reduced, optimal_k)
            km_metrics = self._evaluate_clustering(reduced, km_labels)
            km_metrics["combined"] = km_metrics["silhouette"]
            cluster_labels = km_labels
            centroids = kmeans_obj.cluster_centers_
            selected_method = "kmeans"
            selected_embedding = self.embedding_model_name
            selected_umap = dict(self.DEFAULT_UMAP_PARAMS)

            comparison_results.append(self._make_comparison_entry(
                embedding_model=self.embedding_model_name,
                clustering_method="kmeans",
                umap_params=self.DEFAULT_UMAP_PARAMS,
                n_clusters=optimal_k,
                n_noise=0,
                metrics=km_metrics,
            ))

        # ---- Apply labels --------------------------------------------------
        df = df.copy()
        df["cluster_id"] = cluster_labels

        cluster_counts = df["cluster_id"].value_counts().sort_index()
        for cid, cnt in cluster_counts.items():
            self.logger.info("  Cluster %d: %d sections", cid, cnt)

        # ---- Sample (use reduced space for distance to centroids) ----------
        kmeans_for_sample = (
            kmeans_obj if selected_method == "kmeans" else None
        )
        samples = self._sample_clusters(
            df, reduced, kmeans=kmeans_for_sample, centroids=centroids
        )

        # ---- Generate schema & prompt via LLM ------------------------------
        if self.skip_llm:
            self.logger.info("skip_llm=True — usando esquema por defecto (sin LLM).")
            schema_result = self._default_schema(title)
        else:
            schema_result = self._generate_schema_via_llm(
                title, samples,
                data.get("cluster_counts", {}),
                data.get("total_sections", len(df)),
            )

        # ---- Build final extraction prompt ---------------------------------
        extraction_prompt = self._build_extraction_prompt(
            title, schema_result["schema"]
        )

        # ---- Build report --------------------------------------------------
        report = self._build_cluster_report(df, samples)

        return {
            "title": title,
            "total_sections": len(df),
            "n_clusters": len(set(cluster_labels) - {-1}),
            "silhouette_scores": {
                str(k): float(v.get("silhouette", 0))
                for k, v in combined_scores.items()
            },
            "cluster_counts": {
                str(k): int(v) for k, v in cluster_counts.items()
            },
            "samples_per_cluster": samples,
            "json_schema": schema_result["schema"],
            "schema_justification": schema_result["justification"],
            "extraction_prompt": extraction_prompt,
            "cluster_report": report,
            "clustering_comparison": comparison_results,
            "selected_method": selected_method,
            "selected_embedding_model": selected_embedding,
            "selected_umap_params": selected_umap,
        }

    # ---- embedding --------------------------------------------------------

    def _get_embedder(
        self, model_name: Optional[str] = None
    ) -> SentenceTransformer:
        target = model_name or self.embedding_model_name
        if self._embedder is None or (
            hasattr(self._embedder, "_model_name")
            and self._embedder._model_name != target
        ):
            self.logger.info("Loading embedding model: %s", target)
            new_embedder = SentenceTransformer(target)
            new_embedder._model_name = target
            if model_name is None or model_name == self.embedding_model_name:
                self._embedder = new_embedder
            return new_embedder
        return self._embedder

    def _get_embeddings(
        self, texts: List[str], model_name: Optional[str] = None
    ) -> np.ndarray:
        embedder = self._get_embedder(model_name)
        self.logger.info(
            "Generating embeddings for %d texts with %s…",
            len(texts),
            model_name or self.embedding_model_name,
        )
        embeddings = embedder.encode(
            texts, show_progress_bar=True, batch_size=32
        )
        self.logger.info("Embeddings shape: %s", embeddings.shape)
        return embeddings

    # ---- dimensionality reduction -----------------------------------------

    def _reduce_dimensions(
        self,
        embeddings: np.ndarray,
        n_components: int = 10,
        n_neighbors: int = 15,
    ) -> np.ndarray:
        n = embeddings.shape[0]
        max_components = min(50, n - 1, embeddings.shape[1])
        n_components = min(n_components, max_components)
        n_neighbors = min(n_neighbors, n - 1) if n > 1 else 1

        try:
            import umap

            self.logger.info(
                "Reducing dims with UMAP (n_components=%d, n_neighbors=%d)…",
                n_components,
                n_neighbors,
            )
            reducer = umap.UMAP(
                n_components=n_components,
                random_state=self.random_state,
                n_neighbors=n_neighbors,
            )
            reduced = reducer.fit_transform(embeddings)
            self.logger.info("UMAP reduced shape: %s", reduced.shape)
            return reduced
        except ImportError:
            self.logger.warning(
                "UMAP not available; falling back to PCA (n_components=%d).",
                n_components,
            )
            pca = PCA(n_components=n_components, random_state=self.random_state)
            reduced = pca.fit_transform(embeddings)
            self.logger.info("PCA reduced shape: %s", reduced.shape)
            return reduced

    # ---- optimal k (combined metrics) -------------------------------------

    def _evaluate_clustering(
        self, X: np.ndarray, labels: np.ndarray, ignore_noise: bool = False
    ) -> Dict[str, float]:
        if ignore_noise:
            mask = labels != -1
            if mask.sum() < 2:
                return {
                    "silhouette": -1.0,
                    "davies_bouldin": float("inf"),
                    "calinski_harabasz": 0.0,
                    "combined": -1.0,
                }
            X_sub = X[mask]
            labels_sub = labels[mask]
        else:
            X_sub = X
            labels_sub = labels

        n_unique = len(set(labels_sub))
        if n_unique < 2:
            return {
                "silhouette": -1.0,
                "davies_bouldin": float("inf"),
                "calinski_harabasz": 0.0,
                "combined": -1.0,
            }

        sil = silhouette_score(X_sub, labels_sub)
        db = davies_bouldin_score(X_sub, labels_sub)
        ch = calinski_harabasz_score(X_sub, labels_sub)

        return {"silhouette": sil, "davies_bouldin": db, "calinski_harabasz": ch}

    def _find_optimal_k_combined(
        self, X: np.ndarray
    ) -> Tuple[int, Dict[int, Dict[str, float]]]:
        k_min, k_max = self.k_range
        k_range = range(max(2, k_min), min(k_max + 1, X.shape[0]))
        all_scores: Dict[int, Dict[str, float]] = {}

        for k in k_range:
            if k >= X.shape[0]:
                continue
            km = KMeans(n_clusters=k, random_state=self.random_state, n_init=10)
            labels = km.fit_predict(X)
            metrics = self._evaluate_clustering(X, labels)
            all_scores[k] = metrics
            self.logger.debug(
                "  k=%d sil=%.4f db=%.4f ch=%.1f",
                k,
                metrics["silhouette"],
                metrics["davies_bouldin"],
                metrics["calinski_harabasz"],
            )

        if not all_scores:
            return 3, {}

        # Normalize each metric to [0,1] and combine
        sil_values = np.array([s["silhouette"] for s in all_scores.values()])
        db_values = np.array([s["davies_bouldin"] for s in all_scores.values()])
        ch_values = np.array([s["calinski_harabasz"] for s in all_scores.values()])

        def normalize_minmax(vals):
            vmin, vmax = vals.min(), vals.max()
            if vmax == vmin:
                return np.ones_like(vals)
            return (vals - vmin) / (vmax - vmin)

        sil_norm = normalize_minmax(sil_values)
        db_norm = 1.0 - normalize_minmax(
            db_values
        )  # invert: lower DB = better
        ch_norm = normalize_minmax(ch_values)

        combined = sil_norm + db_norm + ch_norm

        best_idx = int(np.argmax(combined))
        optimal_k = list(all_scores.keys())[best_idx]

        # Attach combined score to each entry
        for i, k in enumerate(all_scores):
            all_scores[k]["sil_norm"] = float(sil_norm[i])
            all_scores[k]["db_norm"] = float(db_norm[i])
            all_scores[k]["ch_norm"] = float(ch_norm[i])
            all_scores[k]["combined"] = float(combined[i])

        self.logger.info(
            "Optimal k=%d (combined=%.4f, sil=%.4f, db=%.4f, ch=%.1f)",
            optimal_k,
            combined[best_idx],
            all_scores[optimal_k]["silhouette"],
            all_scores[optimal_k]["davies_bouldin"],
            all_scores[optimal_k]["calinski_harabasz"],
        )
        return optimal_k, all_scores

    # ---- clustering -------------------------------------------------------

    def _cluster(
        self, X: np.ndarray, k: int
    ) -> Tuple[np.ndarray, KMeans]:
        self.logger.info("Clustering with k=%d…", k)
        kmeans = KMeans(
            n_clusters=k, random_state=self.random_state, n_init=10
        )
        labels = kmeans.fit_predict(X)
        return labels, kmeans

    # ---- helpers ----------------------------------------------------------

    def _compute_centroids(
        self, X: np.ndarray, labels: np.ndarray
    ) -> np.ndarray:
        unique = sorted(set(labels) - {-1})
        centroids = np.zeros((len(unique), X.shape[1]))
        for i, cid in enumerate(unique):
            mask = labels == cid
            centroids[i] = X[mask].mean(axis=0)
        return centroids

    def _make_comparison_entry(
        self,
        embedding_model: str,
        clustering_method: str,
        umap_params: Dict[str, Any],
        n_clusters: int,
        n_noise: int,
        metrics: Dict[str, float],
    ) -> Dict[str, Any]:
        return {
            "embedding_model": embedding_model,
            "clustering_method": clustering_method,
            "umap_n_components": umap_params.get("n_components"),
            "umap_n_neighbors": umap_params.get("n_neighbors"),
            "n_clusters": n_clusters,
            "n_noise": n_noise,
            "silhouette": float(metrics.get("silhouette", -1)),
            "davies_bouldin": float(metrics.get("davies_bouldin", -1)),
            "calinski_harabasz": float(metrics.get("calinski_harabasz", -1)),
            "combined_score": float(metrics.get("combined", metrics.get("silhouette", -1))),
        }

    def _run_grid_comparison(
        self,
        title: str,
        texts: List[str],
        models_to_try: List[str],
        configs_to_try: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Try all embedding × UMAP × clustering combinations, pick best."""
        self.logger.info(
            "Comparing %d embedding models × %d UMAP configs for '%s'…",
            len(models_to_try),
            len(configs_to_try),
            title,
        )

        all_results: List[Dict[str, Any]] = []
        best_score = -1e9
        best_entry: Optional[Dict[str, Any]] = None

        for model_name in models_to_try:
            embeddings = self._get_embeddings(texts, model_name=model_name)
            for umap_p in configs_to_try:
                reduced = self._reduce_dimensions(
                    embeddings,
                    n_components=umap_p.get("n_components", 10),
                    n_neighbors=umap_p.get("n_neighbors", 15),
                )

                # K-Means
                try:
                    opt_k, k_combined_scores = self._find_optimal_k_combined(reduced)
                    km_labels, km_model = self._cluster(reduced, opt_k)
                    km_metrics = self._evaluate_clustering(reduced, km_labels)
                    km_metrics["combined"] = km_metrics["silhouette"]  # fallback
                    entry = self._make_comparison_entry(
                        embedding_model=model_name,
                        clustering_method="kmeans",
                        umap_params=umap_p,
                        n_clusters=opt_k,
                        n_noise=0,
                        metrics=km_metrics,
                    )
                    all_results.append(entry)
                    if km_metrics.get("combined", -1) > best_score:
                        best_score = km_metrics.get("combined", -1)
                        best_entry = {
                            "labels": km_labels,
                            "kmeans": km_model,
                            "centroids": km_model.cluster_centers_,
                            "reduced": reduced,
                            "combined_scores": k_combined_scores,
                            "selected_method": "kmeans",
                            "selected_embedding": model_name,
                            "selected_umap": umap_p,
                            "metrics": km_metrics,
                        }
                    self.logger.info(
                        "  [grid] %s / UMAP(%d,%d) / kmeans(k=%d): combined=%.4f",
                        model_name.split("/")[-1][:30],
                        umap_p.get("n_components", 10),
                        umap_p.get("n_neighbors", 15),
                        opt_k,
                        km_metrics.get("combined", -1),
                    )
                except Exception as e:
                    self.logger.warning(
                        "  [grid] K-Means failed for %s / UMAP(%d,%d): %s",
                        model_name.split("/")[-1][:30],
                        umap_p.get("n_components", 10),
                        umap_p.get("n_neighbors", 15),
                        str(e),
                    )

        if best_entry is None:
            self.logger.warning(
                "Grid comparison produced no valid results. Falling back to defaults."
            )
            embeddings = self._get_embeddings(texts)
            reduced = self._reduce_dimensions(
                embeddings, **self.DEFAULT_UMAP_PARAMS
            )
            opt_k, sil_scores = self._find_optimal_k_combined(reduced)
            km_labels, km_model = self._cluster(reduced, opt_k)
            best_entry = {
                "labels": km_labels,
                "kmeans": km_model,
                "centroids": km_model.cluster_centers_,
                "reduced": reduced,
                "combined_scores": sil_scores,
                "selected_method": "kmeans",
                "selected_embedding": self.embedding_model_name,
                "selected_umap": self.DEFAULT_UMAP_PARAMS,
                "metrics": {"combined": 0.0},
            }

        best_entry["comparison_results"] = all_results
        self.logger.info(
            "Best: %s / %s / UMAP(%d,%d) (combined=%.4f)",
            best_entry["selected_embedding"].split("/")[-1][:30],
            best_entry["selected_method"],
            best_entry["selected_umap"].get("n_components", 10),
            best_entry["selected_umap"].get("n_neighbors", 15),
            best_entry["metrics"].get("combined", -1),
        )
        return best_entry

    # ---- sampling ---------------------------------------------------------

    def _sample_clusters(
        self,
        df: pd.DataFrame,
        embeddings: np.ndarray,
        kmeans: Optional[KMeans] = None,
        centroids: Optional[np.ndarray] = None,
        n_near: int = 3,
        n_far: int = 2,
    ) -> Dict[str, List[Dict[str, Any]]]:
        samples: Dict[str, List[Dict[str, Any]]] = {}
        if kmeans is not None:
            centroids = kmeans.cluster_centers_
        elif centroids is None:
            centroids = self._compute_centroids(
                embeddings, df["cluster_id"].values
            )

        unique_labels = sorted(
            l for l in df["cluster_id"].unique() if l != -1
        )
        centroid_map = {
            label: idx for idx, label in enumerate(unique_labels)
        }

        for cluster_id in unique_labels:
            mask = df["cluster_id"] == cluster_id
            indices = np.where(mask)[0]
            if len(indices) == 0:
                continue

            cluster_emb = embeddings[indices]
            centroid = centroids[centroid_map[cluster_id]]

            dists = np.linalg.norm(cluster_emb - centroid, axis=1)

            near_idx = np.argsort(dists)[: min(n_near, len(indices))]
            far_idx = np.argsort(dists)[-min(n_far, len(indices)) :]

            selected = sorted(
                set(
                    [int(indices[i]) for i in near_idx]
                    + [int(indices[i]) for i in far_idx]
                )
            )

            cluster_samples = []
            for idx in selected:
                row = df.iloc[idx]
                text = row["content_text"]
                cluster_samples.append(
                    {
                        "nro_licitacion": row["nro_licitacion"],
                        "year": int(row["year"]) if pd.notna(row["year"]) else None,
                        "category_id": (
                            int(row["category_id"])
                            if pd.notna(row["category_id"])
                            else None
                        ),
                        "text": text[:2000],
                        "is_extreme": idx in [int(indices[i]) for i in far_idx],
                    }
                )
            samples[str(cluster_id)] = cluster_samples

        return samples

    # ---- LLM schema generation --------------------------------------------

    def _validate_schema(self, result: dict) -> Tuple[bool, str]:
        if "schema" not in result or "justification" not in result:
            return False, "Falta la clave 'schema' o 'justification' en la respuesta"

        schema = result["schema"]
        if not isinstance(schema, dict):
            return False, "'schema' debe ser un diccionario"

        if len(schema) == 0:
            return False, "'schema' está vacío"

        if len(schema) > 12:
            return False, f"Demasiados campos ({len(schema)}), máximo 12"

        allowed_field_keys = {"type", "description", "importance", "extraction_hint", "extraction_method"}
        valid_methods = {"literal", "semantic", "structural"}
        for field_name, field_def in schema.items():
            if not isinstance(field_def, dict):
                return False, f"Campo '{field_name}' no es un diccionario"
            ftype = field_def.get("type")
            if ftype not in ("binary", "numeric"):
                return (
                    False,
                    f"Campo '{field_name}' tiene type='{ftype}' (debe ser 'binary' o 'numeric')",
                )
            if not field_def.get("description", "").strip():
                return False, f"Campo '{field_name}' no tiene descripción o está vacía"
            method = field_def.get("extraction_method")
            if method is not None and method not in valid_methods:
                return (
                    False,
                    f"Campo '{field_name}' tiene extraction_method='{method}' (debe ser 'literal', 'semantic' o 'structural')",
                )
            extra = set(field_def.keys()) - allowed_field_keys
            if extra:
                return False, f"Campo '{field_name}' tiene claves extra: {extra}"

        if not result.get("justification", "").strip():
            return False, "'justification' está vacía"

        cluster_values = result.get("cluster_values")
        if cluster_values is not None:
            if not isinstance(cluster_values, dict):
                return False, "'cluster_values' debe ser un diccionario"
            collapsed_fields = []
            for field_name, values in cluster_values.items():
                if field_name not in schema:
                    continue
                if not isinstance(values, dict):
                    continue
                unique_vals = set(v for v in values.values() if v is not None)
                if len(unique_vals) <= 1:
                    collapsed_fields.append(field_name)
            if collapsed_fields and len(collapsed_fields) == len(schema):
                return (
                    False,
                    f"Todos los campos tienen valor constante entre clusters según cluster_values: {collapsed_fields}",
                )

        return True, ""

    def _generate_schema_via_llm(
        self, title: str, samples: Dict[str, List[Dict[str, Any]]],
        cluster_counts: Optional[Dict[str, int]] = None,
        total_sections: int = 0,
    ) -> Dict[str, Any]:
        self.logger.info(
            "Generating JSON schema via %s for '%s'…", self.llm_provider.name(), title
        )

        # Build frequency header
        freq_lines = []
        if cluster_counts and total_sections:
            freq_lines.append(f"Total secciones: {total_sections}")
            freq_lines.append(f"Clusters identificados: {len(cluster_counts)}")
            sorted_clusters = sorted(cluster_counts, key=lambda c: cluster_counts[c], reverse=True)
            freq_lines.append("Distribución de clusters (ordenados por tamaño descendente):")
            for i, cid in enumerate(sorted_clusters, 1):
                cnt = cluster_counts[cid]
                pct = 100.0 * cnt / total_sections
                tag = "← versión dominante/estándar" if i == 1 else ""
                freq_lines.append(f"  Cluster {cid}: {cnt} ({pct:.1f}%) {tag}")
            freq_lines.append("")
        freq_header = "\n".join(freq_lines)

        cluster_descriptions = []
        for cid, sample_list in samples.items():
            texts_for_llm = []
            for s in sample_list:
                tag = "EXTREMO" if s.get("is_extreme") else "REPRESENTATIVO"
                texts_for_llm.append(
                    f"[{tag} - {s['nro_licitacion']}]\n{s['text'][:1500]}"
                )
            cluster_descriptions.append(
                f"=== CLUSTER {cid} ({len(sample_list)} muestras) ===\n"
                + "\n\n".join(texts_for_llm)
            )

        extraction_context = (
            "\n\nIMPORTANTE — CONTEXTO DE EJECUCIÓN:\n"
            "El schema diseñado será ejecutado automáticamente por un sistema separado que procesa "
            "cada sección de forma individual. Ese sistema NO tiene acceso a estos clusters ni a "
            "este análisis. Recibirá únicamente el texto de una sección y los extraction_hints.\n\n"
            "Por lo tanto, cada extraction_hint debe ser autocontenido:\n"
            "- No uses frases como 'como en el cluster dominante' o 'según la versión estándar'\n"
            "- Para campos binarios: especifica qué texto exacto o qué patrón activa el valor 1\n"
            "- Para campos numéricos: lista exactamente qué items contar o qué número extraer\n"
            "- Si el hint requiere buscar variantes textuales, enuméralas explícitamente"
        )

        user_prompt = (
            f"TÍTULO DE LA SECCIÓN: {title}\n\n"
            + freq_header
            + "CLUSTERS ENCONTRADOS:\n\n"
            + "\n\n".join(cluster_descriptions)
            + extraction_context
            + "\n\n"
            + "Analiza los clusters y sus diferencias. Diseña un esquema JSON (solo binarios y numéricos) "
            "que capture las características potencialmente anómalas del texto, diferencie los clusters, "
            "y cuyos extraction_hints sean ejecutables por un sistema automático sobre textos individuales."
        )

        max_attempts = 1 + (self.schema_validation_retries if self.validate_schema else 0)
        last_error = ""

        for attempt in range(max_attempts):
            if attempt == 0:
                system_prompt = SCHEMA_DESIGN_SYSTEM_PROMPT
            else:
                system_prompt = SCHEMA_VALIDATION_SYSTEM_PROMPT.format(
                    razon_rechazo=last_error
                )

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]

            response_text = self.llm_provider.generate(
                messages, response_format={"type": "json_object"}
            )

            try:
                json_str = response_text
                if "```json" in json_str:
                    json_str = json_str.split("```json")[1].split("```")[0].strip()
                elif "```" in json_str:
                    json_str = json_str.split("```")[1].split("```")[0].strip()
                result = json.loads(json_str)

                if self.validate_schema:
                    is_valid, reason = self._validate_schema(result)
                    if is_valid:
                        return result
                    else:
                        last_error = reason
                        self.logger.warning(
                            "Schema validation failed (attempt %d/%d): %s",
                            attempt + 1,
                            max_attempts,
                            reason,
                        )
                else:
                    return result

            except (json.JSONDecodeError, IndexError) as e:
                last_error = f"Error parsing JSON: {str(e)}"
                self.logger.warning(
                    "Failed to parse schema JSON (attempt %d/%d): %s",
                    attempt + 1,
                    max_attempts,
                    str(e),
                )

        self.logger.error(
            "All %d attempts failed for schema generation. Using fallback.",
            max_attempts,
        )
        self.logger.debug("Last raw LLM response: %s", response_text)
        return self._default_schema(title)

    def _default_schema(self, title: str) -> Dict[str, Any]:
        slug = title.replace(" ", "_")[:30]
        return {
            "schema": {
                f"tiene_{slug}": {
                    "type": "binary",
                    "description": f"Indica si la sección '{title}' contiene contenido sustancial",
                },
                f"longitud_texto_{slug}": {
                    "type": "numeric",
                    "description": f"Longitud en caracteres del texto de la sección '{title}'",
                },
            },
            "justification": (
                "Esquema de respaldo generado automáticamente porque el LLM "
                "no devolvió un JSON válido."
            ),
        }

    # ---- chat prompt export -----------------------------------------------

    @staticmethod
    def _merge_similar_clusters(
        samples: Dict[str, List[Dict[str, Any]]],
        cluster_counts: Dict[str, int],
        similarity_threshold: float = 0.95,
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
        """Merge clusters whose representative texts are nearly identical (>threshold)."""
        cids = sorted(samples.keys(), key=int)
        if len(cids) <= 1:
            return samples, cluster_counts

        rep_texts = {}
        for cid in cids:
            sample_list = samples[cid]
            rep = next(
                (s for s in sample_list if not s.get("is_extreme")),
                sample_list[0],
            )
            rep_texts[cid] = rep["text"]

        groups = []
        assigned = set()
        for cid in cids:
            if cid in assigned:
                continue
            group = [cid]
            assigned.add(cid)
            for other in cids:
                if other in assigned:
                    continue
                ratio = difflib.SequenceMatcher(
                    None, rep_texts[cid], rep_texts[other]
                ).ratio()
                if ratio >= similarity_threshold:
                    group.append(other)
                    assigned.add(other)
            groups.append(group)

        merged_samples = {}
        merged_counts = {}
        for group in groups:
            new_id = "_".join(group)
            combined = []
            seen = set()
            for cid in group:
                for s in samples[cid]:
                    key = s["text"][:300]
                    if key not in seen:
                        seen.add(key)
                        combined.append(s)
            merged_samples[new_id] = combined
            merged_counts[new_id] = sum(
                int(cluster_counts.get(cid, 0)) for cid in group
            )

        return merged_samples, merged_counts

    def _build_chat_prompt(
        self,
        title: str,
        samples: Dict[str, List[Dict[str, Any]]],
        total_sections: int,
        cluster_counts: Dict[str, int],
        samples_per_cluster: int = 2,
        truncation_chars: Optional[int] = None,
        merge_similar_clusters: bool = True,
        merge_similarity_threshold: float = 0.95,
    ) -> Dict[str, Any]:
        """Build a chat prompt ready to copy-paste into DeepSeek/ChatGPT/Claude.

        Parameters
        ----------
        samples_per_cluster : int
            How many samples to include per cluster (1 rep + rest extremes). Default 2.
        truncation_chars : int or None
            Max chars per sample. If None, computed dynamically as median×1.5 clamped to [500,2000].
        merge_similar_clusters : bool
            If True, merge clusters whose representative texts are near-identical.
        merge_similarity_threshold : float
            difflib ratio threshold for merging (default 0.95).
        """
        # 1. Merge similar clusters
        merged_samples = samples
        merged_counts = dict(cluster_counts)
        if merge_similar_clusters:
            merged_samples, merged_counts = self._merge_similar_clusters(
                samples, cluster_counts, merge_similarity_threshold
            )

        # 2. Dynamic truncation
        if truncation_chars is None:
            all_lens = []
            for sample_list in merged_samples.values():
                for s in sample_list:
                    all_lens.append(len(s["text"]))
            if all_lens:
                med = median(all_lens)
                truncation_chars = min(max(int(med * 1.5), 500), 2000)
            else:
                truncation_chars = 1500

        # 3. Build cluster descriptions with reduced samples
        cluster_descriptions = []
        for cid, sample_list in merged_samples.items():
            rep = [s for s in sample_list if not s.get("is_extreme")]
            ext = [s for s in sample_list if s.get("is_extreme")]
            selected = []
            if rep:
                selected.append(rep[0])
            remaining = samples_per_cluster - len(selected)
            selected.extend(ext[:remaining])
            if len(selected) < samples_per_cluster:
                selected = sample_list[:samples_per_cluster]

            texts_for_llm = []
            for s in selected:
                tag = "EXTREMO" if s.get("is_extreme") else "REPRESENTATIVO"
                texts_for_llm.append(
                    f"[{tag} - {s['nro_licitacion']}]\n{s['text'][:truncation_chars]}"
                )
            cluster_descriptions.append(
                f"=== CLUSTER {cid} ({len(selected)} muestras) ===\n"
                + "\n\n".join(texts_for_llm)
            )

        # 4. Build frequency header
        freq_lines = []
        if merged_counts and total_sections:
            freq_lines.append(f"Total secciones: {total_sections}")
            freq_lines.append(f"Clusters identificados: {len(merged_counts)}")
            sorted_clusters = sorted(merged_counts, key=lambda c: merged_counts[c], reverse=True)
            freq_lines.append("Distribución de clusters (ordenados por tamaño descendente):")
            for i, cid in enumerate(sorted_clusters, 1):
                cnt = merged_counts[cid]
                pct = 100.0 * cnt / total_sections
                tag = "← versión dominante/estándar" if i == 1 else ""
                freq_lines.append(f"  Cluster {cid}: {cnt} ({pct:.1f}%) {tag}")
            freq_lines.append("")
        freq_header = "\n".join(freq_lines)

        extraction_context = (
            "\n\nIMPORTANTE — CONTEXTO DE EJECUCIÓN:\n"
            "El schema diseñado será ejecutado automáticamente por un sistema separado que procesa "
            "cada sección de forma individual. Ese sistema NO tiene acceso a estos clusters ni a "
            "este análisis. Recibirá únicamente el texto de una sección y los extraction_hints.\n\n"
            "Por lo tanto, cada extraction_hint debe ser autocontenido:\n"
            "- No uses frases como 'como en el cluster dominante' o 'según la versión estándar'\n"
            "- Para campos binarios: especifica qué texto exacto o qué patrón activa el valor 1\n"
            "- Para campos numéricos: lista exactamente qué items contar o qué número extraer\n"
            "- Si el hint requiere buscar variantes textuales, enuméralas explícitamente"
        )

        user_prompt = (
            f"TÍTULO DE LA SECCIÓN: {title}\n\n"
            + freq_header
            + "CLUSTERS ENCONTRADOS:\n\n"
            + "\n\n".join(cluster_descriptions)
            + extraction_context
            + "\n\n"
            + "Analiza los clusters y sus diferencias. Diseña un esquema JSON (solo binarios y numéricos) "
            "que capture las características potencialmente anómalas del texto, diferencie los clusters, "
            "y cuyos extraction_hints sean ejecutables por un sistema automático sobre textos individuales."
        )

        expected_output = (
            '```json\n{\n  "schema": {\n    "campo_ejemplo": '
            '{"type": "binary", "description": "...", "importance": 5, '
            '"extraction_method": "literal", "extraction_hint": "..."},'
            '\n    "campo_numerico": '
            '{"type": "numeric", "description": "...", "importance": 7, '
            '"extraction_method": "semantic", "extraction_hint": "..."}'
            '\n  },\n  '
            '"justification": "..."\n}\n```'
        )

        return {
            "title": title,
            "total_sections": total_sections,
            "n_clusters": len(merged_samples),
            "cluster_counts": merged_counts,
            "truncation_chars": truncation_chars,
            "samples_per_cluster": min(samples_per_cluster, 2),
            "prompt": {
                "system": SCHEMA_DESIGN_SYSTEM_PROMPT,
                "user": user_prompt,
            },
            "expected_output": expected_output,
        }

    # ---- extraction prompt (Pipeline 2) -----------------------------------

    def build_section_extraction_prompt(
        self, titulo: str, nro_licitacion: str, texto: str,
        schema: dict, importance_threshold: int = 7
    ) -> Tuple[str, str]:
        """
        Build system + user prompt for extracting fields from a single text.

        Pipeline 2 uses this to call the LLM extractor per section.
        Filters schema fields by importance >= threshold to reduce tokens.
        Only includes 'type', 'extraction_hint' and 'extraction_method' in
        the schema sent to the extractor — no description/importance/cluster_values.
        """
        schema_para_extractor = {}
        for field_name, field_def in schema.items():
            if field_def.get("importance", 0) >= importance_threshold:
                entry = {
                    "type": field_def["type"],
                    "extraction_hint": field_def.get(
                        "extraction_hint", field_def["description"]
                    ),
                }
                if "extraction_method" in field_def:
                    entry["extraction_method"] = field_def["extraction_method"]
                schema_para_extractor[field_name] = entry

        user_prompt = (
            f"TÍTULO DE LA SECCIÓN: {titulo}\n"
            f"DOCUMENTO: {nro_licitacion}\n\n"
            f"SCHEMA A EXTRAER:\n"
            f"{json.dumps(schema_para_extractor, ensure_ascii=False, indent=2)}\n\n"
            f"TEXTO DE LA SECCIÓN:\n"
            f"{texto}\n\n"
            f"Extrae los campos del schema siguiendo exactamente cada extraction_hint."
        )

        return EXTRACTION_SYSTEM_PROMPT, user_prompt

    def _build_extraction_prompt(
        self, title: str, schema: Dict[str, Any]
    ) -> str:
        return build_extraction_prompt_text(title, schema)

    # ---- cluster report ---------------------------------------------------

    def _build_cluster_report(
        self, df: pd.DataFrame, samples: Dict[str, List[Dict[str, Any]]]
    ) -> str:
        lines: List[str] = []
        lines.append(f"Total sections: {len(df)}")
        n_clusters = len(set(df["cluster_id"]) - {-1})
        lines.append(f"Number of clusters: {n_clusters}")
        if -1 in df["cluster_id"].values:
            n_noise = int((df["cluster_id"] == -1).sum())
            lines.append(f"Noise points: {n_noise} ({100.0 * n_noise / len(df):.1f}%)")
        lines.append("")

        cluster_counts = df["cluster_id"].value_counts().sort_index()
        for cid, cnt in cluster_counts.items():
            pct = 100.0 * cnt / len(df)
            lines.append(f"Cluster {cid}: {cnt} sections ({pct:.1f}%)")

            cluster_samples = samples.get(str(cid), [])
            for s in cluster_samples:
                tag = "EXTREMO" if s.get("is_extreme") else "REPR"
                lines.append(
                    f"  [{tag}] {s['nro_licitacion']} (year={s['year']})"
                )
                lines.append(f"    {s['text'][:200]}…")
            lines.append("")

        return "\n".join(lines)

    # ---- fallback ---------------------------------------------------------

    def _build_fallback(
        self, title: str, df: pd.DataFrame
    ) -> Dict[str, Any]:
        schema = self._default_schema(title)
        extraction_prompt = self._build_extraction_prompt(
            title, schema["schema"]
        )
        return {
            "title": title,
            "total_sections": len(df),
            "n_clusters": 0,
            "silhouette_scores": {},
            "cluster_counts": {},
            "samples_per_cluster": {
                "0": [
                    {
                        "nro_licitacion": row["nro_licitacion"],
                        "year": (
                            int(row["year"])
                            if pd.notna(row.get("year"))
                            else None
                        ),
                        "category_id": (
                            int(row["category_id"])
                            if pd.notna(row.get("category_id"))
                            else None
                        ),
                        "text": row["content_text"][:2000],
                        "is_extreme": False,
                    }
                    for _, row in df.iterrows()
                ]
            },
            "json_schema": schema["schema"],
            "schema_justification": schema["justification"],
            "extraction_prompt": extraction_prompt,
            "cluster_report": "Too few sections; clustering skipped.",
            "clustering_comparison": [],
            "selected_method": "none",
            "selected_embedding_model": self.embedding_model_name,
            "selected_umap_params": {},
        }

    def _get_extraction_prompt_for_title(
        self, title: str, schema: Dict[str, Any]
    ) -> str:
        return self._build_extraction_prompt(title, schema)
