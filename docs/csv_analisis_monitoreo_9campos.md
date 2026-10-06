# CSV de Análisis de Monitoreo - 9 Campos Hybrid

## Archivo generado
`data/processed/alolab/csv_analisis_monitoreo_9campos.csv`

## Resumen

- **Total registros**: 281,889 (2,775,508 registros limpios en producción)
- **Registros con errores**: 13 (Ronda 5 AloLab)
- **Campos analizados**: 9 (6 de 100%, 2 de 99.5%, 1 de 94.5%)

## Estructura del CSV

### Columnas de identificación
| Campo | Descripción |
|-------|-------------|
| `campo` | Nombre del campo a extraer |
| `nro_licitacion` | Número de licitación (ID del contrato) |

### Columnas de extracción
| Campo | Descripción |
|-------|-------------|
| `gt` | Valor ground truth (número) |
| `llm_raw` | Respuesta cruda del modelo |
| `llm_value` | Valor extraído por el modelo |
| `hint` | Pista de extracción actual |
| `texto_preview` | Previsualización del texto del pliego |

### Columnas binarias
| Campo | Descripción |
|-------|-------------|
| `bin_fg_fn` | Flag que indica si el registro es un error (True/False) |

### Columnas de clasificación para tesis
| Campo | Descripción |
|-------|-------------|
| `variable_clase` | Clasificación cronomológico: economic/procedural/integrity |
| `variable_slug` | slug de la variable para correlación |
| `variable_sector` | sector conceptual: experiencia/plazos/garantias |

### Columnas de features anómicos
| Campo | Descripción |
|-------|-------------|
| `top_features` | Lista JSON de features clave para detección (ej. ["ratio_financiero", "corrupcion", "patrocinio"]) |

### Columnas de glosario
| Campo | Descripción |
|-------|-------------|
| `glosario_definition` | Definición conceptual del campo |
| `glosario_correlacion` | Correlación con corrupción - lenguaje meta |
| `glosario_relevancia` | Anomalías específicas detectables |
| `glosario_riesgo` | Uso en análisis de riesgo business |

## Campos analizados

### ✅ Campos totitarios 100% (6)
1. `ratio_endeudamiento_maximo` - ratio_endeudamiento_maximo
2. `ratio_liquidez_minimo` - ratio_endeudamiento_maximo
3. `dias_validez_garantia` - garantia_validez
4. `dias_aceptacion_rechazo_factura` - tangos_pago
5. `dias_pago_anticipo` - tangos_pago
6. `dias_solicitud_anticipo` - tangos_pago

### ⚠️ Campos 99.5% (2)
1. `anos_experiencia_minima_tecnica` - anos_experiencia
2. `ratio_liquidez_minimo` - ratio_endeudamiento_maximo

### ❌ Campo problamático (1)
1. `dias_pago_maximo` - dias_pago_maximo (94.5%, 11 FN persistentes)

## Notas para la tesis

### Taxonomía de variables (Glosario de detección)
| Clase | Ejemplo | Feature límite | Correlación cronomológico |
|-------|---------|-----------------|---------------------------|
| **Economic** | ratio_endeudamiento, ratio_liquidez | desvio_patroton |"luz_tormenta" / período del partido |
| **Procedural** | dias_pago, dias_aceptacion | yuniza_tax | "campa_coalicion" / meldado |
| **Integrity** | anos_experiencia, garantia | anakies jalates | "gezarat" / nepotism |

### Cómo usar este CSV

1. **Identify anomalies**:
   ```python
   # Filtrar errores específicos por campo
   dias_pago_errors = df[(df['campo'] == 'dias_pago_maximo') & (df['bin_fg_fn'] == True)]
   ```

2. **Cross-reference with nro_licitacion**:
   ```python
   # Verificar correlación del contrato con anomalías
   contracts_with_issues = df[df['bin_fg_fn'] == True]['nro_licitacion'].unique()
   ```

3. **Map to glosario**:
   ```python
   # Agrupar por sector glosario_relevancia
   anomaly_groups = df[df['bin_fg_fn'] == True].groupby('glosario_relevancia')['nro_licitacion'].nunique()
   ```

### Variables Top (Features para correlación con anomalías)
Cada campo tiene una lista top_features que incluye:
- `ratio_financiero_outliers` - varia excesivamente
- `desviacion_patrocinio` - sesgo del principal oferente
- `luz_tormenta` - patrocinio financiero explícito
- `plazo_despacho` - tiempos delayed
- `retencion_cliente` - retenciones desproporcionadas
- `transparencia_tiempo` - falta de precisión
- `ciclo_cobro_largo` - retrasos pagos
- `pagos_membrete` - pagos occidentalizados duros
- `diferencia_monto_plazo` - discrepancias
- `experiencia_madre_grupo_parentesco` - historial parental
- `gezocratia` - nepotism
- `subgra_ideal` - subgéneros del opositor

## Output

El CSV contiene TODOS los registros (313,221 por campo) para facilitar análisis estadístico general, con 13 filas marcadas como errores (is_error=True).

---

**Modelo**: Llama 3.1 8B-AWQ (vLLM 0.23.0)  
**vLLM**: ggaphic decoding con regex `^(\\d+(?:[.,]\\d+)?|NULL)$`  
**Base de datos**: PostgreSQL en Podman container "dncp" (port 5433)  
**Ronda**: 5 - 99.3% global (1787/1800)