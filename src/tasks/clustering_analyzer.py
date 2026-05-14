"""
Script integrado para clustering de pliegos con análisis LLM
El LLM analiza solo las métricas (inercia/silhouette) para determinar k óptimo
Parametrizable para cualquier tipo de análisis (Capacidad Financiera, Experiencia, etc.)
"""
import os
import time
import pandas as pd
import requests
from sqlalchemy import create_engine
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from nltk.corpus import stopwords
from config.settings import DB_CONFIG


class ClusteringAnalyzer:
    """Clase para analizar clustering de pliegos con análisis LLM"""

    def __init__(
        self, api_provider="groq", api_key=None, title_filter="Capacidad Financiera"
    ):
        """
        Inicializa el analizador

        Args:
            api_provider: Proveedor de LLM ("groq", "openrouter", "together")
            api_key: Clave de API del proveedor
            title_filter: Filtro de título para el análisis (ej: "Capacidad Financiera", "Experiencia", etc.)

        Opciones de API providers:
        - "groq": Groq API (gratuita) - https://console.groq.com/
        - "openrouter": OpenRouter (tiene opciones gratuitas) - https://openrouter.ai/
        - "together": Together AI (tiene tier gratuito) - https://api.together.xyz/
        """
        self.api_provider = api_provider
        self.api_key = api_key
        self.title_filter = title_filter
        self.spanish_stopwords = stopwords.words("spanish")

        # Configuración de APIs
        self.api_configs = {
            "groq": {
                "url": "https://api.groq.com/openai/v1/chat/completions",
                "model": "llama-3.3-70b-versatile",  # Modelo gratuito de Groq
                "headers": {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            },
            "openrouter": {
                "url": "https://openrouter.ai/api/v1/chat/completions",
                "model": "meta-llama/llama-3.1-8b-instruct:free",  # Modelo gratuito
                "headers": {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "http://localhost:3000",
                    "X-Title": "Clustering Analysis",
                },
            },
            "together": {
                "url": "https://api.together.xyz/v1/chat/completions",
                "model": "meta-llama/Llama-3-8b-chat-hf",
                "headers": {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            },
        }

    def load_data(self, query=None):
        """Carga datos desde la base de datos"""
        print("📥 Cargando datos desde la base de datos...")

        # Query por defecto usando el filtro de título parametrizable
        if query is None:
            query = f"""
            SELECT nro_licitacion, title, content
            FROM dncp.pliegos
            WHERE title LIKE '%{self.title_filter}%'
            AND content IS NOT NULL
            """

        DB_URI = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(DB_URI)

        df = pd.read_sql_query(query, engine)

        # Limpieza básica
        df = df.dropna(subset=["content"])
        df = df[df["content"].str.strip().astype(bool)]

        print(f"✅ Datos cargados: {len(df)} registros para '{self.title_filter}'")
        return df

    def call_llm_api(self, prompt, max_retries=3):
        """Llama a la API del LLM con reintentos"""
        config = self.api_configs[self.api_provider]

        payload = {
            "model": config["model"],
            "messages": [
                {
                    "role": "system",
                    "content": "Eres un experto en machine learning y clustering. Tu trabajo es analizar métricas de clustering para determinar el número óptimo de clusters.",
                },
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 800,
            "temperature": 0.1,
        }

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    config["url"], headers=config["headers"], json=payload, timeout=30
                )

                if response.status_code == 200:
                    return response.json()["choices"][0]["message"]["content"]
                else:
                    print(f"❌ Error API (intento {attempt+1}): {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)  # Backoff exponencial

            except Exception as e:
                print(f"❌ Error de conexión (intento {attempt+1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)

        return "Error: No se pudo obtener respuesta del LLM"

    def analyze_metrics_with_llm(self, metrics_data):
        """Analiza las métricas de clustering para determinar k óptimo"""
        print("🧠 Analizando métricas con LLM para determinar k óptimo...")

        if not self.api_key:
            print("⚠️  No se proporcionó API key. Usando k con mejor silhouette score.")
            return (
                max(metrics_data, key=lambda x: x["silhouette"])["k"],
                "Sin API key - Seleccionado por mejor silhouette score",
            )

        # Preparar datos de métricas para el LLM
        metrics_summary = "RESULTADOS DE EVALUACIÓN DE CLUSTERS:\n\n"
        metrics_summary += "k  | Inercia    | Silhouette\n"
        metrics_summary += "---|------------|----------\n"

        for metric in metrics_data:
            metrics_summary += f"{metric['k']:2d} | {metric['inertia']:8.2f} | {metric['silhouette']:8.4f}\n"

        # Crear prompt especializado para selección de k
        prompt = f"""
                Eres un experto en machine learning y clustering. Analiza las siguientes métricas de evaluación de clustering para determinar el número óptimo de clusters (k).

                {metrics_summary}

                CONTEXTO:
                - Dataset: Documentos de "{self.title_filter}" de licitaciones públicas
                - Algoritmo: K-Means con vectorización TF-IDF
                - Objetivo: Agrupar documentos similares por contenido

                MÉTRICAS:
                - **Inercia**: Suma de distancias cuadráticas de puntos a centroides (menor = mejor)
                - **Silhouette Score**: Medida de separación entre clusters (0-1, mayor = mejor)

                INSTRUCCIONES:
                1. Analiza el "método del codo" en la inercia (busca el punto donde la mejora se estabiliza)
                2. Evalúa el silhouette score (valores >0.3 son buenos, >0.5 excelentes)  
                3. Considera el balance entre interpretabilidad y calidad de clustering
                4. Para documentos de licitaciones, muy pocos clusters (2-3) pueden ser muy generales, muchos (>10) muy específicos

                RESPONDE ÚNICAMENTE:
                - **K_OPTIMO: [número]** 
                - **JUSTIFICACIÓN: [1-2 frases explicando tu elección basada en las métricas]**

                Ejemplo de respuesta:
                K_OPTIMO: 5
                JUSTIFICACIÓN: El silhouette score alcanza su máximo en k=5 (0.4250) mientras que la inercia muestra una mejora marginal después de este punto, indicando un buen balance entre separación de clusters y parsimonia del modelo.
            """

        analysis = self.call_llm_api(prompt)

        # Extraer k recomendado del análisis
        try:
            lines = analysis.split("\n")
            k_line = [line for line in lines if "K_OPTIMO:" in line.upper()]
            if k_line:
                recommended_k = int(k_line[0].split(":")[1].strip())
                print(f"🎯 LLM recomienda k={recommended_k}")
                return recommended_k, analysis
            else:
                print(
                    "⚠️  No se pudo extraer k del análisis LLM. Usando mejor silhouette."
                )
                return max(metrics_data, key=lambda x: x["silhouette"])["k"], analysis
        except:
            print("⚠️  Error procesando respuesta LLM. Usando mejor silhouette.")
            return max(metrics_data, key=lambda x: x["silhouette"])["k"], analysis

    def find_optimal_clusters_with_llm(self, df, k_range=range(2, 16)):
        """Encuentra el número óptimo de clusters usando métricas y LLM"""
        print("🔍 Evaluando número óptimo de clusters...")

        # Vectorización TF-IDF
        vectorizer = TfidfVectorizer(
            stop_words=self.spanish_stopwords, max_features=1000
        )
        X = vectorizer.fit_transform(df["content"])

        metrics_data = []

        print("Calculando métricas para diferentes valores de k...")
        for k in k_range:
            kmeans = KMeans(n_clusters=k, random_state=42)
            labels = kmeans.fit_predict(X)

            inertia = kmeans.inertia_
            silhouette = silhouette_score(X, labels) if k < len(df) else 0

            metrics_data.append({"k": k, "inertia": inertia, "silhouette": silhouette})

            print(f"k={k:2d} | Inercia={inertia:8.2f} | Silhouette={silhouette:.4f}")

        # Analizar métricas con LLM para determinar k óptimo
        if self.api_key:
            optimal_k, llm_reasoning = self.analyze_metrics_with_llm(metrics_data)
        else:
            # Fallback: usar mejor silhouette score
            optimal_k = max(metrics_data, key=lambda x: x["silhouette"])["k"]
            llm_reasoning = f"Sin LLM disponible. Seleccionado k={optimal_k} por mejor silhouette score."
            print(f"🎯 Seleccionado k={optimal_k} (mejor silhouette score)")

        return optimal_k, vectorizer, X, llm_reasoning

    def perform_clustering(self, df, n_clusters, vectorizer, X):
        """Realiza el clustering con el número óptimo de clusters"""
        print(f"🤖 Realizando clustering con {n_clusters} clusters...")

        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        df["cluster"] = kmeans.fit_predict(X)

        # Mostrar distribución de clusters
        cluster_counts = df["cluster"].value_counts().sort_index()
        print("📊 Distribución de clusters:")
        for cluster_id, count in cluster_counts.items():
            print(f"  Cluster {cluster_id}: {count} documentos")

        return df, kmeans

    def extract_samples(self, df, samples_per_cluster=5):
        """Extrae muestras representativas de cada cluster"""
        print(f"📝 Extrayendo {samples_per_cluster} muestras por cluster...")

        sampled_rows = []

        for cluster_id in df["cluster"].unique():
            cluster_df = df[df["cluster"] == cluster_id]
            sampled = cluster_df.sample(
                n=min(samples_per_cluster, len(cluster_df)), random_state=42
            )
            sampled_rows.append(sampled)

        sampled_df = pd.concat(sampled_rows).reset_index(drop=True)
        print(f"✅ Extraídas {len(sampled_df)} muestras en total")

        return sampled_df

    def generate_cluster_descriptions(self, df):
        """Genera descripciones automáticas para cada cluster basado en muestras"""
        cluster_descriptions = {}

        for cluster_id in sorted(df["cluster"].unique()):
            cluster_data = df[df["cluster"] == cluster_id]

            # Tomar una muestra representativa para describir el cluster
            sample_size = min(3, len(cluster_data))
            samples = cluster_data.sample(n=sample_size, random_state=42)

            # Crear descripción basada en títulos más comunes
            titles = samples["title"].value_counts().head(2).index.tolist()

            if len(titles) > 1:
                description = f"Cluster {cluster_id}: {titles[0]} y similares"
            else:
                description = f"Cluster {cluster_id}: {titles[0] if titles else self.title_filter}"

            cluster_descriptions[cluster_id] = description

        return cluster_descriptions

    def update_database_clusters(self, df, cluster_descriptions):
        """Actualiza la tabla dncp.pliegos con información de clusters"""
        print(
            f"🗄️ Actualizando base de datos con información de clusters para '{self.title_filter}'..."
        )

        try:
            # Crear conexión a la base de datos
            DB_URI = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(DB_URI)

            # Preparar los datos para actualizar
            updates_made = 0

            with engine.begin() as conn:
                for _, row in df.iterrows():
                    nro_licitacion = row["nro_licitacion"]
                    cluster_id = int(row["cluster"])
                    descripcion = cluster_descriptions[cluster_id]

                    # SQL para actualizar registros que coincidan con el filtro de título
                    update_sql = """
                    UPDATE dncp.pliegos 
                    SET cluster = %s, descripcion_cluster = %s
                    WHERE nro_licitacion = %s 
                    AND title LIKE %s
                    AND content IS NOT NULL
                    """

                    result = conn.execute(
                        update_sql,
                        (
                            cluster_id,
                            descripcion,
                            nro_licitacion,
                            f"%{self.title_filter}%",
                        ),
                    )

                    updates_made += result.rowcount

            print(f"✅ Base de datos actualizada: {updates_made} registros modificados")
            return updates_made

        except Exception as e:
            print(f"❌ Error actualizando base de datos: {str(e)}")
            return 0

    def save_results(self, df, sampled_df, llm_reasoning):
        """Guarda todos los resultados en archivos y actualiza base de datos"""
        # Crear nombre de archivo basado en el filtro de título
        title_safe = self.title_filter.lower().replace(" ", "_").replace("/", "_")

        print("💾 Guardando resultados...")

        # Guardar dataset completo con clusters
        df.to_csv(f"{title_safe}_clusters.csv", index=False, encoding="utf-8")
        df.to_excel(f"{title_safe}_clusters.xlsx", index=False, engine="openpyxl")

        # Guardar muestras
        sampled_df.to_csv(f"{title_safe}_muestras.csv", index=False, encoding="utf-8")
        sampled_df.to_excel(
            f"{title_safe}_muestras.xlsx", index=False, engine="openpyxl"
        )

        # Guardar análisis de selección de k
        with open(f"{title_safe}_seleccion_k_llm.txt", "w", encoding="utf-8") as f:
            f.write("SELECCIÓN DE K ÓPTIMO CON LLM\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"FILTRO DE ANÁLISIS: {self.title_filter}\n")
            f.write(
                "OBJETIVO: Determinar el número óptimo de clusters analizando métricas de inercia y silhouette score.\n\n"
            )
            f.write("RAZONAMIENTO DEL LLM:\n")
            f.write("-" * 30 + "\n")
            f.write(llm_reasoning)

        # Generar SQL para muestras
        nros = sampled_df["nro_licitacion"].unique()
        quote_nros = ", ".join(f"'{n}'" for n in nros)

        sql_query = f"""
                    SELECT *
                    FROM dncp.pliegos
                    WHERE nro_licitacion IN ({quote_nros});
                    """

        with open(f"{title_safe}_query_muestras.sql", "w", encoding="utf-8") as f:
            f.write(sql_query)

        print(f"✅ Archivos guardados:")
        print(f"  - {title_safe}_clusters.csv/xlsx")
        print(f"  - {title_safe}_muestras.csv/xlsx")
        print(f"  - {title_safe}_seleccion_k_llm.txt")
        print(f"  - {title_safe}_query_muestras.sql")

        # Generar descripciones de clusters y actualizar base de datos
        cluster_descriptions = self.generate_cluster_descriptions(df)
        updates_made = self.update_database_clusters(df, cluster_descriptions)

        return sql_query, updates_made

    def run_complete_analysis(self, query=None, samples_per_cluster=5):
        """Ejecuta el análisis completo"""
        print(
            f"🚀 Iniciando análisis de clustering con selección inteligente de k para '{self.title_filter}'\n"
        )

        # 1. Cargar datos
        df = self.load_data(query)

        # 2. Encontrar número óptimo de clusters usando LLM
        optimal_k, vectorizer, X, llm_reasoning = self.find_optimal_clusters_with_llm(
            df
        )

        # 3. Mostrar razonamiento del LLM
        print("\n" + "=" * 80)
        print("🧠 SELECCIÓN DE K ÓPTIMO CON LLM")
        print("=" * 80)
        print(llm_reasoning)
        print("=" * 80)

        # 4. Realizar clustering con k seleccionado
        df_clustered, kmeans = self.perform_clustering(df, optimal_k, vectorizer, X)

        # 5. Extraer muestras
        sampled_df = self.extract_samples(df_clustered, samples_per_cluster)

        # 6. Guardar resultados y actualizar base de datos
        sql_query, updates_made = self.save_results(
            df_clustered, sampled_df, llm_reasoning
        )

        print(f"\n📄 SELECT generado para licitaciones muestreadas:\n")
        print(sql_query)
        print(f"\n🗄️ Registros actualizados en base de datos: {updates_made}")

        return {
            "df_complete": df_clustered,
            "df_samples": sampled_df,
            "llm_reasoning": llm_reasoning,
            "sql_query": sql_query,
            "optimal_k": optimal_k,
            "db_updates": updates_made,
        }


# Ejemplo de uso
if __name__ == "__main__":

    # Configuración parametrizable
    TITLE_FILTER = "Apertura de ofertas"
    QUERY = "SELECT * FROM dncp.vm_apertura_de_ofertas"
    SAMPLES_PER_CLUSTER = 5
    GROK_LLM_API_KEY = os.getenv("GROQ_API_KEY")  


    # Inicializar analyzer con filtro parametrizable
    analyzer = ClusteringAnalyzer("groq", GROK_LLM_API_KEY, TITLE_FILTER)

    results = analyzer.run_complete_analysis(
        query=QUERY, samples_per_cluster=SAMPLES_PER_CLUSTER
    )

    print("\n🎉 Análisis completado!")
    print(f"📊 Clusters óptimos determinados por LLM: {results['optimal_k']}")
    print(f"📝 Muestras extraídas: {len(results['df_samples'])}")
    print(f"🗄️ Registros actualizados en BD: {results['db_updates']}")
