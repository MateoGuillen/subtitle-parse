**Asunto:** Avance de tesis — Pipeline ETL completo (OCDS + Pliegos) y decisión pendiente regex vs LLM

Estimado/a tutor/a:

Le escribo para resumir el estado del trabajo de detección de anomalías en contrataciones públicas
(OCDS DNCP Paraguay + pliegos de bases y condiciones), y para plantear una decisión metodológica que
quisiera alinear con usted.

**1. Qué se entregó.** Se construyó y ejecutó un pipeline ETL completo de 7 etapas que va desde las
fuentes públicas del portal de contrataciones (`contrataciones.gov.py`) hasta una base PostgreSQL
poblada con datos estructurados y textuales: **~31,387 licitaciones de pliegos electrónicos, 32,785
PDFs descargados, ~2.78M secciones de pliego** y una matriz de features por licitación
(`document_features_v2`, 31,321 × 108), además de la tabla económica agregada (29,811 × 46).

**2. Decisiones principales y por qué.**
- **Datos estructurados por bulk CSV ZIP, no por la API en vivo del DNCP** (`masivo_{year}.zip`,
  73–462 MB por año): evita límites de tasa y permite reproducibilidad del dataset completo.
- **Solo pliegos electrónicos**: se filtró la lista de documentos a `application/pdf` tipo "Pliego
  Electrónico de Bases y Condiciones" antes de descargar (~31,387 licitaciones); el resto se descartó.
- **Estrategia de extracción por outlines (índice del PDF)**: cada PDF se parsea con Apache Tika y se
  usan sus **bookmarks/outlines** para delimitar secciones; el texto se extrae línea a línea y cada
  título del índice se cruza con su línea en el cuerpo (5 casos de límites resueltos para no perder
  contenido entre páginas ni duplicar).
- **Limpieza sistemática**: unión de guiones de fin de línea, eliminación de líneas de número de
  página, normalización Unicode/espacios y stemming básico; se descartan secciones vacías.
- **Carga idempotente**: PostgreSQL particionado por año, con COPY en batches de 10,000 y checkpoints
  por año (re-ejecutable sin duplicar).

**3. Estado actual.** Los datos ya están limpios y en base. Las features semánticas del pliego
(`document_features_v2`) se extrajeron con un **esquema híbrido: 69 campos por regex + 12 por
fallback LLM** (Llama 3.1 8B-AWQ local, validado 100/100 sobre un golden set humano). El benchmark de
estrategias (`comparativa_estrategias.csv`) confirma que la **estrategia híbrida (H=65.1%) supera al
LLM puro (L_pp=63.1%) y al regex puro (R=61.0%)**, con el postproceso determinista como el mayor
contribuyente.

**4. Decisión a resolver (🟡).** Con los datos limpios en base, queda definir cómo continuar la
transformación de los campos restantes:
- **Regex determinista**: reproducible, sin costo por documento, respaldado por la literatura de red
  flags computables (OCP/Development Gateway 2016; World Bank 2011 "rigged specifications").
- **LLM con postproceso**: mejor cobertura semántica y respaldo reciente (Katona & Fazekas 2024: el
  texto predice oferta única 77%→82%; Peppes et al. 2026).
- Mi propuesta es mantener el **híbrido**: regex por defecto cuando existe patrón plantilla y LLM como
  fallback para lo semántico, con capa de postproceso determinista compartida.

Quedo atento a su devolución para fijar la estrategia y continuar con el modelado de anomalías
(Isolation Forest / red flags / red de co-bidding). Adjunto el detalle gráfico del flujo de datos y el
resumen por diapositivas.

Atentamente,  — Proyecto de tesis DNCP