"""Transformer for the section clustering & schema pipeline.

Handles:
- Embedding generation via Sentence‑Transformers
- UMAP dimensionality reduction
- K‑Means clustering (k∈[3,7] via silhouette + Davies-Bouldin + Calinski-Harabasz)
- HDBSCAN clustering as alternative (opt-in)
- Representative/extreme sampling per cluster
- Schema validation post-LLM with retry
- LLM‑driven JSON‑schema design and extraction‑prompt construction.
"""

import json
import time
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
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling


SCHEMA_DESIGN_SYSTEM_PROMPT = """Eres un experto en detección de anomalías en pliegos de licitaciones públicas paraguayas.

Recibes muestras de texto agrupadas en CLUSTERS para una sección específica de un pliego.
Cada cluster representa una variante distinta de cómo aparece ese contenido.

Tu tarea es diseñar un ESQUEMA JSON que capture las diferencias relevantes entre clusters
y que sea potencialmente indicador de anomalías.

REGLAS ESTRICTAS para el esquema:
1. Solo campos BINARIOS (0/1): indican presencia/ausencia de ciertos elementos, cláusulas o características.
2. Solo campos NUMÉRICOS (enteros o floats): cantidades, porcentajes, conteos, valores.
3. NO se permiten: campos de texto libre, listas de strings, objetos anidados complejos.
4. Todos los campos deben ser relevantes para la detección de anomalías.
5. El esquema debe ser común para todas las secciones de este título (algunos campos quedarán en 0 según corresponda).
6. Máximo 12 campos en total.
7. Los campos binarios y numéricos deben estar al mismo nivel (no anidados).

Formato de respuesta DEBE SER EXACTAMENTE:
```json
{{
  "schema": {{
    "menciona_porcentaje_anticipo": {{"type": "binary", "description": "Indica si se menciona un porcentaje de anticipo en el texto"}},
    "porcentaje_anticipo": {{"type": "numeric", "description": "Valor del porcentaje de anticipo mencionado (0 si no aplica)"}},
    "contiene_firma_digital": {{"type": "binary", "description": "Indica si el texto contiene referencia a firma digital o electrónica"}},
    "num_clausulas_especiales": {{"type": "numeric", "description": "Cantidad de cláusulas especiales o condiciones adicionales mencionadas"}}
  }},
  "justification": "Explica brevemente por qué cada campo fue elegido y cómo ayuda a detectar anomalías. Menciona las diferencias clave entre clusters que justifican cada campo."
}}
```

Debes responder ÚNICAMENTE con el JSON mostrado arriba, sin texto adicional fuera del bloque de código."""

SCHEMA_VALIDATION_SYSTEM_PROMPT = """Eres un experto en detección de anomalías en pliegos de licitaciones públicas paraguayas.

Recibes muestras de texto agrupadas en CLUSTERS para una sección específica de un pliego.
Cada cluster representa una variante distinta de cómo aparece ese contenido.

Tu tarea es diseñar un ESQUEMA JSON que capture las diferencias relevantes entre clusters
y que sea potencialmente indicador de anomalías.

Un intento anterior fue RECHAZADO por la siguiente razón:
{razon_rechazo}

REGLAS ESTRICTAS para el esquema:
1. Solo campos BINARIOS (0/1): indican presencia/ausencia de ciertos elementos, cláusulas o características.
2. Solo campos NUMÉRICOS (enteros o floats): cantidades, porcentajes, conteos, valores.
3. NO se permiten: campos de texto libre, listas de strings, objetos anidados complejos.
4. Todos los campos deben ser relevantes para la detección de anomalías.
5. El esquema debe ser común para todas las secciones de este título (algunos campos quedarán en 0 según corresponda).
6. Máximo 12 campos en total.
7. Los campos binarios y numéricos deben estar al mismo nivel (no anidados).

Formato de respuesta DEBE SER EXACTAMENTE:
```json
{{
  "schema": {{
    "menciona_porcentaje_anticipo": {{"type": "binary", "description": "Indica si se menciona un porcentaje de anticipo en el texto"}},
    "porcentaje_anticipo": {{"type": "numeric", "description": "Valor del porcentaje de anticipo mencionado (0 si no aplica)"}},
    "contiene_firma_digital": {{"type": "binary", "description": "Indica si el texto contiene referencia a firma digital o electrónica"}},
    "num_clausulas_especiales": {{"type": "numeric", "description": "Cantidad de cláusulas especiales o condiciones adicionales mencionadas"}}
  }},
  "justification": "Explica brevemente por qué cada campo fue elegido y cómo ayuda a detectar anomalías. Menciona las diferencias clave entre clusters que justifican cada campo."
}}
```

Debes responder ÚNICAMENTE con el JSON mostrado arriba, sin texto adicional fuera del bloque de código.
CORRIGE el error mencionado en tu respuesta."""

EXTRACTION_PROMPT_TEMPLATE = """Eres un extractor de datos estructurados para pliegos de licitaciones públicas paraguayas.

CONTEXTO: Sección "{titulo}" de un pliego de licitación.
{titulo_descripcion}

Debes analizar el texto proporcionado y extraer UNICAMENTE los campos definidos en el siguiente esquema JSON.

ESQUEMA:
{esquema_str}

REGLAS:
1. Campos BINARIOS: 1 si la característica está presente en el texto, 0 si no.
2. Campos NUMÉRICOS: el valor numérico encontrado. Si no hay valor, poner 0.
3. Responde EXCLUSIVAMENTE con un JSON válido, sin texto adicional.
4. No inventes información que no esté en el texto.

TEXTO A ANALIZAR:
{texto}

RESPUESTA (solo JSON):"""


class SectionClusteringTransformer:
    """
    Embeds, clusters, samples, and drives LLM schema/prompt generation
    for each title.
    """

    OPENROUTER_BASE = "https://openrouter.ai/api/v1"
    DEFAULT_UMAP_PARAMS = {"n_components": 10, "n_neighbors": 15}

    def __init__(
        self,
        llm_api_key: str,
        llm_model: str = "openai/gpt-oss-20b:free",
        embedding_model: str = "paraphrase-multilingual-MiniLM-L12-v2",
        random_state: int = 42,
        k_range: Tuple[int, int] = (2, 15),
        use_hdbscan: bool = True,
        validate_schema: bool = True,
        schema_validation_retries: int = 2,
        compare_embeddings: bool = False,
        embedding_models_to_test: Optional[List[str]] = None,
        umap_params_grid: Optional[List[Dict[str, Any]]] = None,
    ):
        self.llm_api_key = llm_api_key
        self.llm_model = llm_model
        self.embedding_model_name = embedding_model
        self.random_state = random_state
        self.k_range = k_range
        self.use_hdbscan = use_hdbscan
        self.validate_schema = validate_schema
        self.schema_validation_retries = schema_validation_retries
        self.compare_embeddings = compare_embeddings
        self.embedding_models_to_test = embedding_models_to_test or []
        self.umap_params_grid = umap_params_grid or []
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

            # HDBSCAN alternative
            if self.use_hdbscan:
                try:
                    hdb_labels, hdb_n_clusters, hdb_n_noise = self._cluster_hdbscan(reduced)
                    if hdb_n_clusters >= 2:
                        hdb_metrics = self._evaluate_clustering(
                            reduced, hdb_labels, ignore_noise=True
                        )
                        hdb_metrics["combined"] = hdb_metrics["silhouette"]
                        comparison_results.append(self._make_comparison_entry(
                            embedding_model=self.embedding_model_name,
                            clustering_method="hdbscan",
                            umap_params=self.DEFAULT_UMAP_PARAMS,
                            n_clusters=hdb_n_clusters,
                            n_noise=hdb_n_noise,
                            metrics=hdb_metrics,
                        ))

                        if hdb_metrics["combined"] > km_metrics["combined"]:
                            self.logger.info(
                                "  -> HDBSCAN beats K-Means (combined=%.4f vs %.4f). Switching.",
                                hdb_metrics["combined"],
                                km_metrics["combined"],
                            )
                            cluster_labels = hdb_labels
                            selected_method = "hdbscan"
                            centroids = self._compute_centroids(reduced, hdb_labels)
                except ImportError:
                    self.logger.warning("HDBSCAN not available; skipping.")
                except Exception as e:
                    self.logger.warning("HDBSCAN failed: %s. Skipping.", str(e))

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
        schema_result = self._generate_schema_via_llm(title, samples)

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

    # ---- HDBSCAN ----------------------------------------------------------

    def _cluster_hdbscan(
        self,
        X: np.ndarray,
        min_cluster_size: int = 50,
        min_samples: int = 10,
    ) -> Tuple[np.ndarray, int, int]:
        import hdbscan

        self.logger.info("Clustering with HDBSCAN…")
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min(min_cluster_size, X.shape[0] // 5),
            min_samples=min_samples,
        )
        labels = clusterer.fit_predict(X)
        n_clusters = len(set(labels) - {-1})
        n_noise = int(np.sum(labels == -1))
        noise_pct = 100.0 * n_noise / len(labels)
        self.logger.info(
            "HDBSCAN: %d clusters, %d noise points (%.1f%%)",
            n_clusters,
            n_noise,
            noise_pct,
        )
        return labels, n_clusters, n_noise

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

                # HDBSCAN
                try:
                    hdb_labels, hdb_nc, hdb_nn = self._cluster_hdbscan(reduced)
                    if hdb_nc >= 2:
                        hdb_metrics = self._evaluate_clustering(
                            reduced, hdb_labels, ignore_noise=True
                        )
                        hdb_metrics["combined"] = hdb_metrics["silhouette"]
                        entry = self._make_comparison_entry(
                            embedding_model=model_name,
                            clustering_method="hdbscan",
                            umap_params=umap_p,
                            n_clusters=hdb_nc,
                            n_noise=hdb_nn,
                            metrics=hdb_metrics,
                        )
                        all_results.append(entry)
                        if hdb_metrics.get("combined", -1) > best_score:
                            best_score = hdb_metrics.get("combined", -1)
                            centroids = self._compute_centroids(reduced, hdb_labels)
                            best_entry = {
                                "labels": hdb_labels,
                                "kmeans": None,
                                "centroids": centroids,
                                "reduced": reduced,
                                "combined_scores": {},
                                "selected_method": "hdbscan",
                                "selected_embedding": model_name,
                                "selected_umap": umap_p,
                                "metrics": hdb_metrics,
                            }
                        self.logger.info(
                            "  [grid] %s / UMAP(%d,%d) / hdbscan(k=%d): combined=%.4f",
                            model_name.split("/")[-1][:30],
                            umap_p.get("n_components", 10),
                            umap_p.get("n_neighbors", 15),
                            hdb_nc,
                            hdb_metrics.get("combined", -1),
                        )
                except ImportError:
                    pass
                except Exception as e:
                    self.logger.warning(
                        "  [grid] HDBSCAN failed: %s", str(e)
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
            extra = set(field_def.keys()) - {"type", "description"}
            if extra:
                return False, f"Campo '{field_name}' tiene claves extra: {extra}"

        if not result.get("justification", "").strip():
            return False, "'justification' está vacía"

        return True, ""

    def _generate_schema_via_llm(
        self, title: str, samples: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        self.logger.info(
            "Generating JSON schema via %s for '%s'…", self.llm_model, title
        )

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

        user_prompt = (
            f"TÍTULO DE LA SECCIÓN: {title}\n\n"
            f"CLUSTERS ENCONTRADOS:\n\n"
            + "\n\n".join(cluster_descriptions)
            + "\n\n"
            + "Analiza los clusters y sus diferencias. "
            "Diseña un esquema JSON (solo binarios y numéricos) "
            "que capture las características potencialmente anómalas "
            "del texto y diferencie los clusters."
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

            response_text = self._call_llm(messages)

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

    def _call_llm(self, messages: List[Dict[str, str]]) -> str:
        import requests

        url = f"{self.OPENROUTER_BASE}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.llm_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.llm_model,
            "messages": messages,
            "max_tokens": 4096,
            "temperature": 0.0,
        }

        for attempt in range(3):
            try:
                resp = requests.post(
                    url, headers=headers, json=payload, timeout=120
                )
                if resp.status_code == 200:
                    data = resp.json()
                    choices = data.get("choices", [])
                    if choices:
                        return choices[0].get("message", {}).get("content", "")
                    return json.dumps(data)
                else:
                    self.logger.error(
                        "LLM API error (attempt %d): %d %s",
                        attempt + 1,
                        resp.status_code,
                        resp.text[:500],
                    )
                    if attempt < 2:
                        time.sleep(2**attempt)
            except requests.exceptions.RequestException as e:
                self.logger.error(
                    "LLM request failed (attempt %d): %s",
                    attempt + 1,
                    str(e),
                )
                if attempt < 2:
                    time.sleep(2**attempt)
        return '{"schema": {}, "justification": "LLM call failed after 3 retries."}'

    # ---- extraction prompt ------------------------------------------------

    def _build_extraction_prompt(
        self, title: str, schema: Dict[str, Any]
    ) -> str:
        titulo_descripcion = {
            "fraude y corrupcion": "Contiene referencias a posibles actos de fraude, corrupción, sobornos, conflictos de interés o irregularidades en el proceso de licitación.",
            "formato y firma de la oferta": "Describe el formato requerido para presentar la oferta y los requisitos de firma (digital o manuscrita).",
            "copias de la oferta cps": "Especifica la cantidad y tipo de copias requeridas de la oferta (impresas, digitales, CD, etc.).",
            "limitacion de responsabilidad": "Define las limitaciones de responsabilidad de las partes contratantes.",
            "planos y disenos": "Describe requisitos de planos, diseños, especificaciones técnicas o documentación gráfica.",
            "porcentaje de garantia de fiel cumplimiento de con": "Establece el porcentaje de garantía de fiel cumplimiento del contrato.",
            "idioma de la oferta": "Especifica el idioma o idiomas en que debe presentarse la oferta.",
            "aclaracion de las ofertas": "Describe el proceso para solicitar aclaraciones sobre las ofertas presentadas.",
            "retiro sustitucion y modificacion de las ofertas": "Regula el retiro, sustitución y modificación de las ofertas antes de la apertura.",
            "audiencia informativa": "Describe la realización de audiencias informativas o reuniones previas a la presentación de ofertas.",
        }.get(title, "")

        esquema_lines = []
        for field_name, field_def in schema.items():
            ftype = field_def.get("type", "binary")
            fdesc = field_def.get("description", "")
            esquema_lines.append(f'  "{field_name}": ({ftype}) {fdesc}')
        esquema_str = "\n".join(esquema_lines)

        return EXTRACTION_PROMPT_TEMPLATE.format(
            titulo=title,
            titulo_descripcion=titulo_descripcion,
            esquema_str=esquema_str,
            texto="{texto}",
        )

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
