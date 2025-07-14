import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from nltk.corpus import stopwords
from config.settings import DB_CONFIG

# 🧠 Parámetros
K_RANGE = range(2, 16)
TITLE = "Capacidad Financiera"

# 🧾 Cargar stopwords español
spanish_stopwords = stopwords.words("spanish")

# 🛢️ Conexión a la base de datos
DB_URI = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DB_URI)

# 📥 Cargar datos desde la vista materializada
QUERY = "SELECT * FROM dncp.vm_capacidad_financiera_v2"
df = pd.read_sql_query(QUERY, engine)

# 🧹 Limpieza
df = df.dropna(subset=["content"])
df = df[df["content"].str.strip().astype(bool)]

# 🔢 Vectorización TF-IDF
vectorizer = TfidfVectorizer(stop_words=spanish_stopwords, max_features=1000)
X = vectorizer.fit_transform(df["content"])

# 📈 Evaluación para distintos k
inertias = []
silhouette_scores = []

print("Calculando métricas para distintos k...")
for k in K_RANGE:
    kmeans = KMeans(n_clusters=k, random_state=42)
    labels = kmeans.fit_predict(X)

    inertias.append(kmeans.inertia_)
    if k < len(df):  # Silhouette score necesita al menos k muestras
        score = silhouette_score(X, labels)
        silhouette_scores.append(score)
        print(f"k={k:2d} | Inertia={kmeans.inertia_:.2f} | Silhouette={score:.4f}")
    else:
        silhouette_scores.append(None)
        print(f"k={k:2d} | Inertia={kmeans.inertia_:.2f} | Silhouette=NA")

# 📊 Gráfica del método del codo
plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
plt.plot(K_RANGE, inertias, marker="o")
plt.title("Método del Codo (Inercia vs k)")
plt.xlabel("Número de clusters (k)")
plt.ylabel("Inercia total")
plt.grid(True)

# 📊 Gráfica de Silhouette Score
plt.subplot(1, 2, 2)
plt.plot(K_RANGE, silhouette_scores, marker="o", color="green")
plt.title("Silhouette Score vs k")
plt.xlabel("Número de clusters (k)")
plt.ylabel("Silhouette Score")
plt.grid(True)

plt.tight_layout()
plt.show()


# Resultados
# Calculando métricas para distintos k...
# k= 2 | Inertia=11878.27 | Silhouette=0.3158
# k= 3 | Inertia=9190.22 | Silhouette=0.3873
# k= 4 | Inertia=8546.51 | Silhouette=0.3042
# k= 5 | Inertia=8150.22 | Silhouette=0.2632
# k= 6 | Inertia=7859.67 | Silhouette=0.2858
# k= 7 | Inertia=7432.12 | Silhouette=0.2954
# k= 8 | Inertia=7251.29 | Silhouette=0.2620
# k= 9 | Inertia=6917.17 | Silhouette=0.2549
# k=10 | Inertia=6743.58 | Silhouette=0.2595
# k=11 | Inertia=6604.81 | Silhouette=0.2565
# k=12 | Inertia=6454.91 | Silhouette=0.2594
# k=13 | Inertia=6340.29 | Silhouette=0.2676
# k=14 | Inertia=6096.00 | Silhouette=0.2806
# k=15 | Inertia=5925.72 | Silhouette=0.2857
