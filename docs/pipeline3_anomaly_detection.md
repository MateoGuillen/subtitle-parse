# Pipeline 3: Detección de Anomalías No Supervisada

## Contexto del Dataset

### Datos disponibles en `dncp`

| Tabla | Registros | Descripción |
|:------|:---------:|:------------|
| `licitaciones` | 29,811 | Procesos de compra |
| `adjudicaciones` | 39,463 | Decisiones de adjudicación |
| `contratos` | 39,691 | Contratos firmados |
| `oferentes` | 78,695 | Participantes |
| `items_licitacion` | 1.8M | Items detallados |
| `pliegos_secciones` | 2.7M | Textos de pliegos |
| **Con ambos datos** | **29,784** | OCDS + Pliegos |

### Variables disponibles por tipo

**Numéricas (OCDS):**
- `monto_estimado`, `presupuesto_monto`, `monto_adjudicado`, `monto_contrato`
- `cantidad_oferentes`, `cantidad_items`, `cantidad_lotes`
- `duracion_consultas_dias`, `duracion_oferta_dias`, `duracion_contrato_dias`
- `garantia_porcentaje`, `costo_pliego`
- `cantidad_enmiendas`, `monto_total_enmiendas`
- `cantidad_pagos`, `monto_total_pagado`

**Categóricas (OCDS):**
- `metodo_contratacion`, `metodo_detalle`
- `categoria_principal`, `categoria_detalle`
- `criterio_adjudicacion`, `criterio_detalle`
- `estado`, `estado_detalle`

**Temporales (OCDS):**
- `fecha_publicacion`, `fecha_apertura`, `fecha_adjudicacion`, `fecha_firma`

**Texto (Pliegos - Pipeline 2):**
- `cluster_id`, `texto_vacio`, `contiene_contenido_adicional`
- `permiso_es_sin_traduccion`, `cantidad_copias_requeridas`
- `num_clausulas_normativas_presentes`, `longitud_texto_palabras`

---

## Feature Engineering

### Variables numéricas a derivar

```python
# Ratios financieros
df['ratio_precio_vs_estimado'] = df['monto_adjudicado'] / df['monto_estimado']
df['ratio_precio_vs_presupuesto'] = df['monto_adjudicado'] / df['presupuesto_monto']
df['desviacion_precio'] = (df['monto_adjudicado'] - df['monto_estimado']) / df['monto_estimado']

# Eficiencia del proceso
df['duracion_total_dias'] = (df['fecha_adjudicacion'] - df['fecha_publicacion']).dt.days
df['velocidad_proceso'] = df['duracion_total_dias'] / df['duracion_oferta_dias']

# Concentración de oferentes
df['ratio_oferentes_vs_items'] = df['cantidad_oferentes'] / df['cantidad_items']
df['oferentes_por_lote'] = df['cantidad_oferentes'] / df['cantidad_lotes']

# Enmiendas (señal de cambios sospechosos)
df['ratio_enmiendas'] = df['cantidad_enmiendas'] / df['duracion_contrato_dias']
df['monto_enmiendas_ratio'] = df['monto_total_enmiendas'] / df['monto_contrato']

# Pagos
df['ratio_pagado_vs_contrato'] = df['monto_total_pagado'] / df['monto_contrato']
df['pagos_por_mes'] = df['cantidad_pagos'] / (df['duracion_dias'] / 30)
```

### Variables categóricas a codificar

```python
# One-Hot Encoding para variables categóricas
df_encoded = pd.get_dummies(df, columns=[
    'metodo_contratacion',
    'categoria_principal',
    'criterio_adjudicacion',
    'estado'
])

# Label Encoding para ordinales
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
df['metodo_encoded'] = le.fit_transform(df['metodo_contratacion'])
```

### Variables de Pipeline 2 (Pliegos)

```python
# Features de texto extraídas por LLM
df['tiene_contenido_adicional'] = df['contiene_contenido_adicional']
df['permiso_sin_traduccion'] = df['permiso_es_sin_traduccion']
df['copias_requeridas'] = df['cantidad_copias_requeridas']
df['clausulas_normativas'] = df['num_clausulas_normativas_presentes']
df['longitud_texto'] = df['longitud_texto_palabras']
df['cluster_id'] = df['cluster_id']
```

---

## Preprocessing Pipeline

```python
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer

# Definir features
numeric_features = [
    'monto_estimado', 'presupuesto_monto', 'cantidad_oferentes',
    'cantidad_items', 'duracion_oferta_dias', 'ratio_precio_vs_estimado',
    'ratio_oferentes_vs_items', 'copias_requeridas', 'clausulas_normativas',
    'longitud_texto'
]

categorical_features = [
    'metodo_contratacion', 'categoria_principal', 'criterio_adjudicacion'
]

# Pipeline de preprocessing
preprocessor = ColumnTransformer(
    transformers=[
        ('num', Pipeline([
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ]), numeric_features),
        ('cat', Pipeline([
            ('imputer', SimpleImputer(strategy='constant', fill_value='unknown')),
            ('encoder', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
        ]), categorical_features)
    ]
)

# Aplicar
X_processed = preprocessor.fit_transform(df)
```

---

## Métodos de Detección de Anomalías

### Método 1: Isolation Forest (Principal)

**Fundamento teórico:**
El Isolation Forest (Liu et al., 2008) se basa en la premisa de que las anomalías son **pocas y diferentes**. Construye un bosque de árboles de aislamiento (iTrees) donde cada nodo divide los datos aleatoriamente. Las anomalías se aíslan más rápido (camino más corto en el árbol) porque requieren menos divisiones para ser separadas del resto.

**Fórmula del score de anomalía:**
```
s(x, n) = 2^(-E(h(x))/c(n))

Donde:
- h(x) = longitud del camino para aislar x
- E(h(x)) = promedio de longitudes en todos los árboles
- c(n) = factor de normalización (longitud promedio de búsqueda fallida)
- n = número de muestras
```

**Implementación:**
```python
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Preprocessing
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_numeric)

# Model
clf = IsolationForest(
    contamination=0.05,  # 5% estimado de fraude
    n_estimators=200,
    max_samples='auto',
    random_state=42,
    n_jobs=-1
)

# Training
clf.fit(X_scaled)

# Predictions
anomaly_labels = clf.predict(X_scaled)  # -1 = anomaly, 1 = normal
anomaly_scores = clf.decision_function(X_scaled)  # lower = more anomalous

# Top anomalies
top_anomalies = np.argsort(anomaly_scores)[:100]  # 100 más sospechosos
```

**Parámetros clave:**
| Parámetro | Valor recomendado | Justificación |
|:----------|:-----------------:|:--------------|
| `contamination` | 0.01 - 0.05 | Tasa de fraude estimada en contrataciones |
| `n_estimators` | 200 | Balance entre precisión y tiempo |
| `max_samples` | 256 | Submuestreo para eficiencia |

**Qué detecta:**
- Montos atípicamente altos o bajos para la categoría
- Cantidades de oferentes anormales
- Duraciones de proceso inusuales
- Combinaciones de variables numéricas inusuales

**Ventajas:**
- Rápido: O(n log n)
- Eficiente en memoria
- Maneja datos de alta dimensionalidad
- No necesita métricas de distancia
- Implementado en scikit-learn

**Desventajas:**
- Falla en anomalías locales (no detecta variaciones de densidad local)
- Parámetro `contamination` debe ser estimado

---

### Método 2: Local Outlier Factor (LOF)

**Fundamento teórico:**
LOF (Breunig et al., 2000) detecta anomalías **locales** comparando la densidad local de un punto con la densidad de sus k vecinos más cercanos. Un punto es anomalía si su densidad local es significativamente menor que la de sus vecinos.

**Fórmula:**
```
LOF_k(p) = (Σ_{o ∈ N_k(p)} lrd_k(o) / lrd_k(p)) / |N_k(p)|

Donde:
- lrd_k(p) = densidad local reachability de p
- N_k(p) = k vecinos más cercanos de p
```

**Implementación:**
```python
from sklearn.neighbors import LocalOutlierFactor

clf = LocalOutlierFactor(
    n_neighbors=20,
    contamination=0.05,
    metric='minkowski'
)

anomaly_labels = clf.fit_predict(X_scaled)
lof_scores = clf.negative_outlier_factor_  # lower = more anomalous
```

**Qué detecta:**
- Proveedores con patrones anormales dentro de su categoría
- Licitaciones sospechosas comparadas con similares
- Anomalías que Isolation Forest puede pasar por alto

**Ventajas:**
- Detecta anomalías locales
- Se adapta a diferentes densidades

**Desventajas:**
- O(n²) en el peor caso, lento con >30K registros
- Sensible al parámetro k

---

### Método 3: DBSCAN (Clustering + Anomalías)

**Fundamento teórico:**
DBSCAN (Ester et al., 1996) agrupa puntos densamente conectados y marca como **ruido** (anomalías) los puntos en regiones de baja densidad.

**Parámetros:**
- `eps`: radio de la vecindad (determinado por k-distancia)
- `min_samples`: mínimo de puntos para formar un cluster

**Implementación:**
```python
from sklearn.cluster import DBSCAN
from sklearn.neighbors import NearestNeighbors

# Determinar eps óptimo con k-distancia
nn = NearestNeighbors(n_neighbors=5)
nn.fit(X_scaled)
distances, indices = nn.kneighbors(X_scaled)
k_distances = np.sort(distances[:, -1])

# Usar elbow point como eps
eps_optimal = k_distances[int(len(k_distances) * 0.9)]  # percentil 90

# DBSCAN
clustering = DBSCAN(eps=eps_optimal, min_samples=5)
labels = clustering.fit_predict(X_scaled)

# Anomalías = puntos con label -1
anomaly_mask = labels == -1
anomalies = X[anomaly_mask]
```

**Qué detecta:**
- Documentos que no pertenecen a ningún grupo conocido
- Proveedores atípicos en el espacio de features
- Tipos de contratación poco comunes

**Ventajas:**
- Da clusters interpretables + anomalías
- No necesita especificar número de clusters

**Desventajas:**
- Sensible a eps y min_samples
- No está diseñado específicamente para detección de anomalías

---

### Método 4: One-Class SVM (Novelty Detection)

**Fundamento teórico:**
One-Class SVM (Schölkopf et al., 2001) aprende una frontera que envuelve los datos "normales". Puntos fuera de la frontera son anomalías.

**Implementación:**
```python
from sklearn.svm import OneClassSVM

clf = OneClassSVM(
    kernel='rbf',
    gamma='scale',
    nu=0.05  # fracción estimada de anomalías
)

anomaly_labels = clf.fit_predict(X_scaled)
decision_scores = clf.decision_function(X_scaled)
```

**Ventajas:**
- Poderoso en espacios de alta dimensionalidad
- Bien para novelty detection

**Desventajas:**
- O(n²) a O(n³), no escala bien con >30K registros
- Selección de kernel y parámetro nu es crítica

---

### Método 5: Autoencoder (Deep Learning)

**Fundamento teórico:**
Un autoencoder (Rumelhart et al., 1986) aprende una representación comprimida de los datos normales. Las anomalías tienen **alto error de reconstrucción** porque el modelo no puede reconstruir patrones que nunca vio.

**Arquitectura:**
```
Input (N features) → Encoder → Latent Space (M features) → Decoder → Output (N features)

Loss = MSE(Input, Output)

Anomaly Score = MSE(Input, Output) por muestra
```

**Implementación:**
```python
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

class ProcurementAutoencoder(nn.Module):
    def __init__(self, input_dim, latent_dim=16):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, latent_dim)
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, input_dim)
        )
    
    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z)

# Training
model = ProcurementAutoencoder(input_dim=X_scaled.shape[1])
optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
criterion = nn.MSELoss()

# Entrenar solo con datos "normales" (o todos si no hay etiquetas)
dataset = TensorDataset(torch.FloatTensor(X_scaled))
loader = DataLoader(dataset, batch_size=256, shuffle=True)

for epoch in range(100):
    for batch in loader:
        x = batch[0]
        x_hat = model(x)
        loss = criterion(x_hat, x)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

# Anomaly scores
with torch.no_grad():
    X_tensor = torch.FloatTensor(X_scaled)
    X_hat = model(X_tensor)
    reconstruction_errors = torch.mean((X_tensor - X_hat)**2, dim=1).numpy()

# Top anomalies
top_anomalies = np.argsort(reconstruction_errors)[-100:]
```

**Qué detecta:**
- Patrones no lineales complejos
- Relaciones entre features que otros métodos no capturan
- Anomalías en datos de texto + numéricos combinados

**Ventajas:**
- Captura patrones complejos
- Maneja datos mixtos

**Desventajas:**
- Requiere más datos y tiempo de entrenamiento
- Arquitectura y hiperparámetros son complejos
- Menos interpretable

---

### Método 6: Ensemble (Recomendado para la tesis)

**Fundamento:**
Combinar múltiples métodos mejora la precisión (Arroyo-Castro, 2025). Un documento es anomalía si **múltiples métodos lo detectan**.

**Implementación:**
```python
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import DBSCAN

# Entrenar múltiples modelos
models = {
    'IF': IsolationForest(contamination=0.05, random_state=42),
    'LOF': LocalOutlierFactor(n_neighbors=20, contamination=0.05),
}

# Obtener scores de cada modelo
all_scores = {}
for name, model in models.items():
    model.fit(X_scaled)
    if hasattr(model, 'decision_function'):
        scores = model.decision_function(X_scaled)
    else:
        scores = model.negative_outlier_factor_
    all_scores[name] = scores

# Normalizar scores a [0, 1]
from sklearn.preprocessing import MinMaxScaler
scaler_scores = MinMaxScaler()
normalized_scores = {}
for name, scores in all_scores.items():
    normalized_scores[name] = scaler_scores.fit_transform(scores.reshape(-1, 1)).flatten()

# Promedio de scores (ensemble)
ensemble_score = np.mean(list(normalized_scores.values()), axis=0)

# Confidence = número de métodos que lo detectan como anomalía
confidence = np.zeros(len(X_scaled))
for name, scores in normalized_scores.items():
    # Umbral: percentil 5 (más bajos = más anómalos)
    threshold = np.percentile(scores, 5)
    confidence[scores <= threshold] += 1

# Confidence score: 0-3 (0 = normal, 3 = muy sospechoso)
```

**Scoring final:**
```
confidence_score = (IF_detecta ? 1 : 0) + (LOF_detecta ? 1 : 0) + (DBSCAN_ruido ? 1 : 0)
```

| Confidence | Interpretación |
|:----------:|:---------------|
| 0 | Normal |
| 1 | Posiblemente anómalo |
| 2 | Sospechoso |
| 3 | Muy sospechoso (revisar manualmente) |

---

## Evaluación (Sin Ground Truth)

### Estrategia 1: Inyección de Anomalías Sintéticas

```python
# Crear anomalías conocidas
n_synthetic = 100
synthetic_indices = np.random.choice(len(X_scaled), n_synthetic, replace=False)

# Modificar features para crear anomalías
X_synthetic = X_scaled.copy()
X_synthetic[synthetic_indices, 0] *= 3  # Monto 3x mayor
X_synthetic[synthetic_indices, 1] *= 0.1  # Cantidad de oferentes muy baja

# Evaluar si el modelo las detecta
scores_synthetic = clf.decision_function(X_synthetic)
detection_rate = np.sum(scores_synthetic[synthetic_indices] < threshold) / n_synthetic
print(f"Tasa de detección de anomalías sintéticas: {detection_rate:.1%}")
```

### Estrategia 2: Validación Cruzada entre Métodos

```python
# Acuerdo entre métodos
methods = ['IF', 'LOF', 'DBSCAN']
agreement_matrix = np.zeros((len(X_scaled), len(methods)))

for i, method in enumerate(methods):
    agreement_matrix[:, i] = (anomaly_labels[method] == -1).astype(int)

# Índice de Jaccard por par
from itertools import combinations
for m1, m2 in combinations(methods, 2):
    intersection = np.sum((agreement_matrix[:, methods.index(m1)] == 1) & 
                         (agreement_matrix[:, methods.index(m2)] == 1))
    union = np.sum((agreement_matrix[:, methods.index(m1)] == 1) | 
                   (agreement_matrix[:, methods.index(m2)] == 1))
    jaccard = intersection / union if union > 0 else 0
    print(f"Jaccard {m1}-{m2}: {jaccard:.3f}")
```

### Estrategia 3: Revisión Experta

```python
# Exportar top-N para revisión manual
top_n = 50
top_indices = np.argsort(ensemble_score)[-top_n:]

# Generar reporte
report = df.iloc[top_indices][[
    'nro_licitacion', 'titulo', 'monto_estimado', 'monto_adjudicado',
    'cantidad_oferentes', 'metodo_contratacion', 'confidence_score'
]]

report.to_csv('anomalies_top50_review.csv', index=False)
```

### Estrategia 4: Métricas Internas

```python
from sklearn.metrics import silhouette_score, davies_bouldin_score

# Para clustering-based (DBSCAN)
if len(set(labels)) > 1:
    sil = silhouette_score(X_scaled[labels != -1], labels[labels != -1])
    db = davies_bouldin_score(X_scaled[labels != -1], labels[labels != -1])
    print(f"Silhouette: {sil:.3f}, Davies-Bouldin: {db:.3f}")
```

---

## Comparación de Métodos

| Método | Tipo de anomalía detectada | Complejidad | Escalabilidad | Interpretabilidad |
|:-------|:--------------------------:|:-----------:|:-------------:|:-----------------:|
| **Isolation Forest** | Outliers multivariados | Baja | Alta | Media |
| **LOF** | Anomalías locales | Media | Media | Media |
| **DBSCAN** | Ruido / documentos atípicos | Media | Media | Alta |
| **One-Class SVM** | Novedades fuera de frontera | Alta | Baja | Baja |
| **Autoencoder** | Patrones no lineales | Alta | Media | Baja |
| **Ensemble** | Combinación de los anteriores | Media | Alta | Media |

### Recomendación por escenario

| Escenario | Método recomendado | Por qué |
|:----------|:------------------:|:--------|
| **Baseline rápido** | Isolation Forest | Rápido, robusto, sin tuning |
| **Anomalías locales** | LOF | Captura densidad local |
| **Patrones complejos** | Autoencoder | Relaciones no lineales |
| **Producción** | Ensemble (IF + LOF) | Mejor precisión |

---

## Arquitectura Recomendada para la Tesis

```
┌─────────────────────────────────────────────────────────────┐
│                    FASE 3B: ANOMALY DETECTION               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ OCDS Data    │    │ Pliegos      │    │ Features     │  │
│  │ (29K rows)   │    │ (Pipeline 2) │    │ Engineering  │  │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘  │
│         │                   │                   │          │
│         └───────────────────┼───────────────────┘          │
│                             ▼                              │
│                    ┌──────────────┐                         │
│                    │ Preprocessing│                         │
│                    │ (Scaling +   │                         │
│                    │  Encoding)   │                         │
│                    └──────┬───────┘                         │
│                           ▼                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              ENSEMBLE DETECTION                     │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │
│  │  │   IF    │  │   LOF   │  │ DBSCAN  │            │   │
│  │  │ (0.05)  │  │ (0.05)  │  │ (ruido) │            │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘            │   │
│  │       │            │            │                  │   │
│  │       └────────────┼────────────┘                  │   │
│  │                    ▼                               │   │
│  │           ┌──────────────┐                         │   │
│  │           │  Aggregation │                         │   │
│  │           │  (Voting +   │                         │   │
│  │           │   Scoring)   │                         │   │
│  │           └──────┬───────┘                         │   │
│  └──────────────────┼─────────────────────────────────┘   │
│                     ▼                                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              OUTPUT                                 │   │
│  │  • Anomaly Score (0-1)                              │   │
│  │  • Confidence (0-3)                                 │   │
│  │  • Top-N anomalies for review                       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Implementación Paso a Paso

| Paso | Descripción | Tiempo est. |
|:----:|:------------|:-----------:|
| 1 | Feature engineering (OCDS + Pliegos) | 3-4 días |
| 2 | Preprocessing pipeline | 1-2 días |
| 3 | Isolation Forest (baseline) | 1 día |
| 4 | LOF + DBSCAN | 1-2 días |
| 5 | Ensemble + scoring | 2-3 días |
| 6 | Evaluación (sintética + cruzada) | 2-3 días |
| 7 | Validación experta (top-50) | 1-2 días |
| 8 | Dashboard Streamlit | 3-4 días |
| 9 | Documentación + hallazgos | 3-4 días |
| **Total** | | **15-20 días** |

---

## Novedades de la tesis (vs. tesis anterior)

| Aspecto | Tesis anterior | Tu tesis |
|:--------|:--------------:|:--------:|
| Datos | Solo OCDS | OCDS + **Pliegos** |
| Features | Numéricos/categóricos | + **Texto estructurado** |
| Método | Isolation Forest | **Ensemble** (IF + LOF + DBSCAN) |
| Análisis | Superficial | **Profundo de contenido** |

---

## Referencias

1. Liu, F.T., Ting, K.M., & Zhou, Z.H. (2008). Isolation Forest. ICDM.
2. Breunig, M.M., et al. (2000). LOF: Identifying Density-Based Local Outliers. SIGMOD.
3. Ester, M., et al. (1996). A Density-Based Algorithm for Discovering Clusters. KDD.
4. Schölkopf, B., et al. (2001). Estimating the Support of a High-Dimensional Distribution. Neural Computation.
5. Arroyo-Castro, J.P. & Chou-Chen, S.W. (2025). Unsupervised Detection of Anomaly in Public Procurement Processes. Springer.

---

## Próximos pasos (cuando se implemente)

1. Feature engineering (OCDS + Pliegos)
2. Preprocessing pipeline
3. Isolation Forest (baseline)
4. LOF + DBSCAN
5. Ensemble + scoring
6. Evaluación (sintética + cruzada)
7. Validación experta (top-50)
8. Dashboard Streamlit
9. Documentación + hallazgos
