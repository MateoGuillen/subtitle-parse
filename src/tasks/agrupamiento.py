"""
Agrupamiento de pliegos usando TF-IDF y KMeans."""

import pandas as pd
from sqlalchemy import create_engine
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from nltk.corpus import stopwords
from config.settings import DB_CONFIG

spanish_stopwords = stopwords.words("spanish")

# TITLE = "Capacidad Financiera"
TITLE = "Apertura de ofertas"
QUERY = "SELECT * FROM dncp.vm_apertura_de_ofertas"

# QUERY = "SELECT * from dncp.v"

DB_URI = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DB_URI)

df = pd.read_sql_query(QUERY, engine)

#  Limpieza básica
df = df.dropna(subset=["content"])
df = df[df["content"].str.strip().astype(bool)]

#  Vectorización TF-IDF
vectorizer = TfidfVectorizer(stop_words=spanish_stopwords, max_features=1000)
X = vectorizer.fit_transform(df["content"])

#  Clusterización KMeans
N_CLUSTERS = 15  # Podés ajustar según tamaño/muestra
kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42)
df["cluster"] = kmeans.fit_predict(X)

#  Selección de muestras por cluster
SAMPLES_PER_CLUSTER = 5
sampled_rows = []

for cluster_id in range(N_CLUSTERS):
    cluster_df = df[df["cluster"] == cluster_id]
    sampled = cluster_df.sample(
        n=min(SAMPLES_PER_CLUSTER, len(cluster_df)), random_state=42
    )
    sampled_rows.append(sampled)

#  Resultado final: muestras agrupadas
sampled_df = pd.concat(sampled_rows).reset_index(drop=True)

#  Mostrar primeros resultados
for i, row in sampled_df.iterrows():
    print(
        f"\n🧾 Ejemplo {i+1} (Cluster {row['cluster']} - Licitación {row['nro_licitacion']})"
    )
    print(f"Título: {row['title']}")
    print(
        f"Contenido:\n{row['content'][:10000]}..."
    )  # Limita a 1000 caracteres para lectura

    # 💾 Guardar todos los resultados con cluster asignado
    df.to_csv("capacidad_financiera_clusters.csv", index=False, encoding="utf-8")

    # 💾 Guardar todos los resultados con cluster asignado en Excel
    df.to_excel("capacidad_financiera_clusters.xlsx", index=False, engine="openpyxl")

    # 💾 Guardar muestras seleccionadas por cluster
    sampled_df.to_csv(
        "capacidad_financiera_muestras.csv", index=False, encoding="utf-8"
    )

    sampled_df.to_excel(
        "capacidad_financiera_muestras.xlsx", index=False, engine="openpyxl"
    )

    # 🔎 Generar SELECT para las licitaciones muestreadas
    nros = sampled_df["nro_licitacion"].unique()
    QUOTE_NROS = ", ".join(f"'{n}'" for n in nros)

    SQL_SELECT = f"""
    SELECT *
    FROM dncp.pliegos
    WHERE nro_licitacion IN ({QUOTE_NROS});
    """

    print("\n📄 SELECT generado para licitaciones muestreadas:\n")
    print(SQL_SELECT)
