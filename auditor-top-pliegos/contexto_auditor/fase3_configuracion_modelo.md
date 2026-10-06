# FASE 3: Variables Ausentes y Configuración del Modelo

## Objetivo
Identificar red flags de la Sección B no presentes, sugerir derivación o solicitud. Configuración inicial Isolation Forest (hiperparámetros, umbral).

## 1. Red Flags Críticos de la Sección B No Presenten en los Datos

### R1. `cantidad_oferentes` (Número total de oferentes recibidos)
- **Ubicación ranking ISF**: Puesto #1 (número de oferentes / oferta única) — el proxy de corrupción con más respaldo empírico.
- **Por qué crítico**: "Oferta única / bajo número de oferentes" es la variable con más evidencia en toda la literatura (Fazekas, Tóth y King 2016; Fazekas y Kocsis 2020; Wittberg y Fazekas 2023; Directrices OCDE 2025).
- **Cómo derivarla de nuestros datos**: Nuestro CSV tiene `ofertentes en consorcio` (variable Alta relevancia ISF), pero no tiene el conteo total de oferentes. **Derivación posible**: Si en el pliego original hay mención al "número de oferentes inscritos" o "sobres recibidos", este campo puede extraerse del `content_text` usando expresión regular para buscar patrones como "se presentaron X oferentes" o "X empresas participaron".
- **Solicitud a DNCP**: Agregar campo `number_of_bidders` o `total_of_tenderers` al dataset de datos abiertos.
- **Preprocesamiento**: Variable numérica continua. Codificar después como booleana `oferta_unica` (1 oferente = 1, >1 = 0) para ISF, ya que el patrón "1 vs varios" es lo que ISF detecta óptimamente.

### R2. `monto_adjudicado` (Monto efectivo adjudicado)
- **Ubicación ranking ISF**: Variable dependiente en el índice de riesgo del FMI (Abdou et al. 2022).
- **Por qué crítico**: El precio adjudicado es la variable central en la mayoría de los índices de riesgo de corrupción. ISF detecta sobreprecios como valores extremos.
- **Cómo derivarla de nuestros datos**: Nuestro CSV tiene `value` con valor 10.0000 o 5.0000 (porcentaje de garantía), pero no el monto adjudicado en moneda local. **Derivación posible**: Si el `content_text` contiene referencias al "monto total adjudicado" o "monto de la oferta ganadora", pueden extraerse. Alternativamente, multiplicar el `value` (10% o 5%) por el monto base del contrato (si está en otros campos del pliego no incluidos en esta muestra de variables).
- **Solicitud a DNCP**: Incluir `awarded_amount` o `monto_adjudicado` en formatos de datos abiertos estandarizados al OCDS.

### R3. `%_desviacion` (Desviación % precio adjudicado vs. precio referencial)
- **Ubicación ranking ISF**: Puesto #2 (desviación del precio adjudicado respecto al precio referencial %) — el red flag que DNCP monitorea oficialmente con umbral +20%/-30%.
- **Por qué crítico**: Es la variable "estelar" para detección de sobreprecio. Abdou et al. (2022) la usan como variable dependiente en su índice de riesgo de corrupción. Distribución con cola pesada favorable a ISF.
- **Cómo derivarla de nuestros datos**: Nuestro CSV tiene `analisis de precios ofertados` (variable ISF Alta), pero no el precio referencial oficial. **Derivación posible**: Si el pliego incluye "precio referencial" o "monto estimado", este puede calcularse como: `%_desviacion = (monto_adjudicado - precio_referencial) / precio_referencial * 100` A partir de `analisis de precios ofertados` y campos de precio base no codificados en esta muestra.
- **Solicitud a DNCP**: Publicar `precio_referencial` junto a cada adjudicación en el portal de datos abiertos.

### R4. `modalidad_contratacion` (Tipo de procedimiento de contratación)
- **Ubicación ranking ISF**: Puesto #3 (modalidad de contratación: directa/excepción vs. abierta).
- **Por qué crítico**: Las modalidades directas concentran riesgo (Abdou et al. 2022; Fazekas, Cingolani y Tóth 2016). ISF detecta anomalía cuando una modalidad inusual se combina con otros indicadores.
- **Cómo derivarla de nuestros datos**: Nuestro CSV no tiene campo directo. **Derivación posible**: Usar `criterios de adjudicacion` + `documentacion requerida para la firma del contrato` para inferir el tipo. Si el pliego menciona "licitación pública", "contratación directa", "adjudicación simplificada", etc., en el `content_text`, pueden codificarse. Alternativamente, clasificar como: `licitación pública` si hay mención a "convocatoria abierta", `contratación directa` si hay mención a "exceptional circumstances", `adjudicación simplificada` si cumplen ciertos umbrales.
- **Solicitud a DNCP**: Incluir `procurement_method` estandarizado al OCDS en los datasets.

### R5. `historial_alertas_entidad` (Historial de alertas de la entidad)
- **Ubicación ranking ISF**: Variable de control del lado de la demanda (Abdou et al. 2022).
- **Por qué crítico**: Permite detectar instituciones atípicas, no solo proveedores atípicos. El FMI la usa como indicador de riesgo del lado de la demanda.
- **Cómo derivarla de nuestros datos**: No derivable directamente de este CSV. **Solicitud**: Historial agregado de alertas rojas/amarillas por entidad RUT/CIF.

### R6. `antigüedad_empresa` (Antigüedad de la empresa proveedora)
- **Por qué crítico**: Variables del lado proveedor (identificación, antigüedad, ubicación, sanciones) son la categoría 4 de la taxonomía. Antigüedad corta puede indicar empresas "fantasma" creadas para una licitación.
- **Cómo derivarla de nuestros datos**: No derivable de `title_normalized`. **Solicitud**: Fecha de constitución de la empresa en el registro público (RUC/DP).

---

## 2. Variables que SÍ Podemos Derivar de Nuestros Datos Existentes

### D1. `numero_oferentes` (Derivado de `ofertentes en consorcio` + conteo)
- **Cómo derivar**: Contar menciones a "empresa", "firma", "oferente" en `content_text` asociado a cada `title_normalized`. Si el pliego dice "X empresas oferentes", extraer el número.
- **Calidad**: Media. Estimación a partir de texto no estructurado, más débil que dato directo, pero preferible a nada.

### D2. `desviacion_porcentaje_precio` (Derivado de `analisis de precios ofertados` + precio referencial)
- **Cómo derivar**: Si el `content_text` contiene ambos valores (precio ofertado y precio referencial), calcular la diferencia porcentual.
- **Calidad**: Alta potencial. El `analisis de precios ofertados` ya es ISF Alta, añadirle el cálculo de desviación lo hace aún más potente.

### D3. `tipo_procedimiento` (Derivado de `criterios adjudicacion` + palabras clave)
- **Cómo derivar**: Buscar en `content_text` palabras clave: "licitación pública", "contratación directa", "exceptional", "simplificada", "concurso". Clasificar automáticamente.
- **Calidad**: Media-Alta. Clasificación heurística pero consistente.

### D4. `antiguedad_aproximada` (Derivada de menciones de "empresa con X años de experiencia")
- **Cómo derivar**: Expresión regular en `content_text` buscando patrones como "empresa con 10 años de experiencia", "más de 5 años en el rubro".
- **Calidad**: Baja-Media. Aproximada pero mejor que nada para variable Proveedor.

---

## 3. Configuración Inicial Sugerida Modelo Isolation Forest

Basado en la literatura (Liu et al. 2008; Herreros-Martínez et al. 2025) y las 125 variables clasificadas:

### Hiperparámetros por defecto:
- `n_estimators=100` — valores por defecto del artículo original (Liu, Ting y Zhou, 2008). Validado empíricamente: aumentar por encima de 100 no mejoró el ranking de anomalías en contexto de compras (Herreros-Martínez et al. 2025).
- `max_samples=256` — permite trabajar con volúmenes reales de la DNCP (~9.500 adjudicaciones promedio anuales según cifras propias del portal de Contrataciones Públicas).
- `contamination='auto'` o enfoque basado en percentil (recomendado).

### Enfoque recomendado para el umbral de corte:
Dado que se desconoce la proporción real de anomalías en un dataset no etiquetado (sección 7, limitaciones), **considerar un enfoque basado en percentil** en lugar de fijar `contamination` de forma arbitraria.
- **Ejemplo**: Calcular los puntajes de anomalía para todos los registros, luego seleccionar el umbral en el percentil 99 (es decir, marcar como anomalía el 1% superior de los puntajes).
- **Justificación**: Mismo criterio que adoptan Herreros-Martínez et al. (2025) por la misma razón: sin datos etiquetados no existe una forma objetiva de fijar qué porcentaje de casos "son" anómalos; cualquier umbral debe presentarse como una decisión metodológica explícita y justificada.

### Variables a incluir en el modelo inicial (basado en clasificación Phase 2):

**Conjunto base Alta relevancia (10 variables):**
1. `analisis de precios ofertados` (Económica/Precio)
2. `ofertentes en consorcio` (Competencia/Concurrencia)
3. `criterios de adjudicacion` (Procedimental)
4. `moneda de la oferta y pago` (Económica/Precio)
5. `tasa de interes por mora` (Económica/Precio)
6. `causales de terminacion del contrato` (Ejecución contractual)
7. `adenda` (Ejecución contractual)
8. `periodo de validez garantía fiel cumplimiento` (Ejecución contractual)
9. `capacidad financiera` (Proveedor)
10. `capacidad tecnica` (Proveedor)

**Conjunto ampliado Media relevancia (añadir 12 variables):**
11. `porcentaje a ser adquirido de la agricultura familiar - cps`
12. `margen de preferencia local - cps`
13. `fuerza mayor`
14. `plazo de reposicion de bienes`
... (continuar con las 22 variables Media de la clasificación Phase 2)

### Configuración de preprocesamiento por variable:

| Variable | Tipo | Preprocesamiento ISF |
|---|---|---|
| `analisis de precios ofertados` | Numérica | Normalización z-score |
| `ofertentes en consorcio` | Categórica booleana | Target encoding o codificación 1/0 (oferta única = 1) |
| `criterios adjudicacion` | Categórica | One-hot encoding (pocas categorías) |
| `moneda oferta y pago` | Numérica/Categórica | Normalización + encoding si múltiples monedas |
| `tasa interes por mora` | Numérica | Normalización z-score |
| `causales terminacion contrato` | Categórica | Target encoding |
| `adenda` | Numérica/Categórica | Count encoding (cantidad adendas) o one-hot |
| `periodo validez garantía` | Numérica | Normalización z-score |
| `capacidad financiera` | Numérica/Categórica | Target encoding (mediana/moda robusta) |
| `capacidad tecnica` | Numérica/Categórica | Target encoding |
| `porcentaje agricultura familiar` | Numérica | Normalización + transformar a desviación vs meta legal |
| `margen preferencia local` | Numérica | Normalización; transformar a "desviación margen aplicado" |
| `fuerza mayor` | Booleana/Categórica | Codificación booleana 0/1 |
| `plazo reposicion bienes` | Numérica | Normalización z-score |

---

## 4. Próximos Pasos

1. **Ejecutar modelo piloto**: Aplicar ISF con las 10 variables Alta al dataset completo (125 variables × 3 nro_licitacion)
2. **Aplicar DIFFI o SHAP**: Identificar qué de las 10 variables Alta contribuyen más al puntaje de anomalía de cada caso
3. **Revisar falsos positivos**: Las 17 variables clasificadas como "Descartar" revisar manualmente un muestreo aleatorio de casos marcados como anomalía por ISF
4. **Solicitar datos ausentes**: Enviar solicitud a DNCP por los 6 red flags críticos (R1-R6) no presentes
5. **Derivar variables D1-D4**: Crear las variables derivadas de los `content_text` y `title_normalized` actuales
6. **Iterar**: Incluir variables Media en segundo modelo y comparar rankings de anomalías

**Archivo generado**: `contexto_auditor/fase3_configuracion_modelo.md`