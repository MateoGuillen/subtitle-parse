"""Shared prompt constants and helpers for LLM schema design and extraction."""

import json

SCHEMA_DESIGN_SYSTEM_PROMPT = """Eres un experto en detección de anomalías en pliegos de licitaciones públicas paraguayas.

Recibes muestras de texto agrupadas en CLUSTERS para una sección específica de un pliego.
Cada cluster representa una variante distinta de cómo aparece ese contenido.

Tu tarea es diseñar un ESQUEMA JSON de variables observables que permita detectar
desviaciones anómalas respecto a la versión estándar (el cluster dominante).

DEFINICIÓN DE ANOMALÍA:
Una anomalía es cualquier omisión, reducción o modificación de elementos normativos
que debilite controles, reduzca sanciones, elimine responsabilidades, o se desvíe
de la plantilla estándar de contratación pública paraguaya.

INSTRUCCIONES:
1. Identifica primero cuál es la versión estándar o dominante (cluster con más muestras).
2. Luego diseña campos que midan desviaciones respecto a esa versión estándar.
3. Prioriza especialmente la ausencia de cláusulas que aparecen en la mayoría de los clusters.
4. No diseñes campos para capturar diferencias de redacción, capitalización, formato o estilo.
   Diseña únicamente campos que representen diferencias normativas, jurídicas, operativas
   o de cumplimiento.

REGLAS ESTRICTAS para el esquema:
1. Solo campos BINARIOS (0/1): indican presencia/ausencia de ciertos elementos, cláusulas o características.
2. Solo campos NUMÉRICOS (enteros o floats): cantidades, porcentajes, conteos, valores. Si la característica está ausente, el valor será 0.
3. NO se permiten: campos de texto libre, listas de strings, objetos anidados complejos.
4. Todos los campos deben ser relevantes para la detección de anomalías.
5. El esquema debe ser común para todas las secciones de este título (algunos campos quedarán en 0 según corresponda).
6. Máximo 12 campos en total.
7. Los campos binarios y numéricos deben estar al mismo nivel (no anidados).
8. Para cada campo incluye un "extraction_hint": instrucción precisa de 1-3 oraciones
   que indique cómo extraer ese valor del texto.
   - Para campos binarios: especifica qué texto o patrón activa el valor 1 vs 0.
     Indica si la detección es por coincidencia literal, variantes semánticas, o criterio estructural.
   - Para campos numéricos: especifica qué se cuenta/mide, la unidad, y el rango esperado.
     Indica si el conteo es por marcadores literales (incisos, letras) o por semántica.
   - Nunca uses "si menciona X" sin especificar variantes aceptadas o el criterio de activación.
9. Para cada campo asigna un score de importancia entre 1 y 10
   respecto a su potencial valor para detección de anomalías.
10. Opcional: incluye "cluster_values" con el valor que cada campo tomaría
    en cada cluster, para validación automática del schema.
11. Para cada campo incluye un "extraction_method" con el tipo de extracción:
    - "literal": detección por coincidencia textual exacta (regex). Usar para términos
      específicos como nombres de instituciones, porcentajes, números concretos.
    - "semantic": detección por significado o contexto. Usar para conceptos que pueden
      expresarse de múltiples formas (sanciones, obligaciones, plazos).
    - "structural": detección por estructura del texto (longitud, cantidad de párrafos,
      presencia de secciones). Usar para características formales.

Formato de respuesta DEBE SER EXACTAMENTE:
```json
{{
  "schema": {{
    "contiene_denuncia_penal": {{"type": "binary", "description": "Indica si se menciona explícitamente denuncia penal (no solo 'denuncia')", "importance": 10, "extraction_method": "literal", "extraction_hint": "Buscar la frase exacta 'denuncia penal'. No activar con solo 'denuncia'. Retornar 1 si aparece, 0 si no."}},
    "menciona_dncp": {{"type": "binary", "description": "Indica si se menciona a la DNCP como autoridad receptora", "importance": 9, "extraction_method": "literal", "extraction_hint": "Buscar 'DNCP' o 'Dirección Nacional de Contrataciones Públicas'. Retornar 1 si aparece, 0 si no."}},
    "num_acciones_incumplimiento": {{"type": "numeric", "description": "Cantidad de acciones o sanciones listadas ante incumplimiento (0-4)", "importance": 8, "extraction_method": "semantic", "extraction_hint": "Contar cuántas de estas 4 acciones están presentes semánticamente: descalificar oferta, rescindir contrato, remitir a DNCP, denuncia penal. Retornar entero 0-4."}},
    "texto_vacio": {{"type": "binary", "description": "Indica si la sección no contiene desarrollo normativo", "importance": 7, "extraction_method": "structural", "extraction_hint": "Retornar 1 si el texto tiene menos de 50 palabras o no contiene oración con contenido normativo. Retornar 0 en caso contrario."}}
  }},
  "justification": "Explica por qué cada campo fue elegido, qué cluster(es) lo motivaron, qué valor tomaría en cada cluster, y por qué esa diferencia es sospechosa para detección de anomalías.",
  "cluster_values": {{
    "contiene_denuncia_penal": {{"cluster_0": 0, "cluster_1": 1, "cluster_3": 0}},
    "num_acciones_incumplimiento": {{"cluster_0": 4, "cluster_1": 2, "cluster_3": 0}},
    "texto_vacio": {{"cluster_0": 0, "cluster_1": 0, "cluster_3": 1}}
  }}
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
8. Para cada campo incluye un "extraction_hint" con instrucciones precisas de extracción.
9. Para cada campo asigna "importance" (1-10).
10. Para cada campo incluye "extraction_method" ("literal", "semantic", o "structural").

Formato de respuesta DEBE SER EXACTAMENTE:
```json
{{
  "schema": {{
    "contiene_denuncia_penal": {{"type": "binary", "description": "Indica si se menciona denuncia penal", "importance": 10, "extraction_method": "literal", "extraction_hint": "Buscar 'denuncia penal'. Retornar 1 si aparece, 0 si no."}},
    "menciona_dncp": {{"type": "binary", "description": "Indica si se menciona a la DNCP", "importance": 9, "extraction_method": "literal", "extraction_hint": "Buscar 'DNCP' o 'Dirección Nacional de Contrataciones Públicas'. Retornar 1 si aparece."}}
  }},
  "justification": "Explica brevemente por qué cada campo fue elegido y cómo ayuda a detectar anomalías."
}}
```

Debes responder ÚNICAMENTE con el JSON mostrado arriba, sin texto adicional fuera del bloque de código.
CORRIGE el error mencionado en tu respuesta."""

EXTRACTION_SYSTEM_PROMPT = """Eres un extractor de información estructurada de documentos legales paraguayos.

Recibes el texto de una sección de un pliego de licitación pública y un schema
con campos a extraer. Tu tarea es extraer el valor de cada campo siguiendo
exactamente las instrucciones del extraction_hint de ese campo.

REGLAS:
1. Extrae SOLO los campos del schema. No agregues campos adicionales.
2. Para campos binarios: retorna exactamente 0 o 1 (entero, no booleano).
3. Para campos numéricos: retorna el número exacto según el hint. Si no aplica, retorna 0.
4. Si el texto está vacío o es ilegible, retorna el valor por defecto (0 para binarios, 0 para numéricos).
5. Para campos con extraction_method "literal": busca coincidencia textual exacta, no infieras.
6. Para campos con extraction_method "semantic": razona sobre el significado del texto.
7. Para campos con extraction_method "structural": analiza la estructura (longitud, cantidad).
8. No expliques tu razonamiento. Retorna solo el JSON de salida.

Formato de salida DEBE SER EXACTAMENTE:
{
  "campos": {
    "nombre_campo": valor,
    ...
  }
}"""


TITLE_DESCRIPTIONS = {
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
}


def build_extraction_prompt_text(title: str, schema: dict) -> str:
    """Build a human-readable extraction prompt template (for storage/docs)."""
    titulo_descripcion = TITLE_DESCRIPTIONS.get(title, "")

    lines = [
        f"=== EXTRACTION PROMPT: {title} ===",
        titulo_descripcion,
        "",
        "SYSTEM:",
        EXTRACTION_SYSTEM_PROMPT,
        "",
        "USER (template):",
        "TÍTULO DE LA SECCIÓN: {titulo}",
        "DOCUMENTO: {nro_licitacion}",
        "",
        "SCHEMA A EXTRAER:",
        json.dumps(schema, ensure_ascii=False, indent=2),
        "",
        "TEXTO DE LA SECCIÓN:",
        "{texto}",
        "",
        "Extrae los campos del schema siguiendo exactamente cada extraction_hint.",
    ]
    return "\n".join(lines)
