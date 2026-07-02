# Comparación de Planes: v1.0 (original) vs v2.0 (GLM revisado)

**Fecha**: 2026-06-24
**Objetivo**: Documentar las diferencias entre ambos planes y la decisión final adoptada.

---

## 1. Resumen de diferencias

| Aspecto | v1.0 (original) | v2.0 (GLM revisado) | Decisión |
|---------|-----------------|----------------------|----------|
| Marco teórico | No existe | Filter/Wrapper/Embedded + 14 referencias | **v2.0** (tesis lo requiere) |
| Métrica de diversidad | Pearson (B1) | Mutual Information normalizada (B3) | **v2.0** (phi inestable en binarias) |
| Métrica de separabilidad | Ninguna | Synthetic AUC (C1) | **v2.0 simplificado** |
| Fórmula compuesta | `coverage × (1-corr)` (producto) | `α·z(cov) + β·z(div) + γ·z(auc)` (suma ponderada) | **v2.0** |
| Pipeline | 6 pasos | 7 pasos + fundamentación | **v2.0** |
| Validación | "Validar de alguna forma" | 3 niveles N1/N2/N3 con criterios | **v2.0** |
| Estabilidad | No existe | Bootstrap (S2) + sensibilidad K (S1) + pesos (S3) | **v2.0 simplificado** |
| Quick wins | 4 items (1-2 días) | 6 items + stability (8-12 días) | **v2.0 fase 1** |

---

## 2. Por qué v2.0 es mejor para la tesis

### 2.1 Marco teórico (§0)

**v1.0**: No tenía fundamentación. El pipeline era "hacemos esto porque funciona".

**v2.0**: Marco completo con:
- Taxonomía Filter/Wrapper/Embedded (Kohavi & John, 1997; Liu & Motoda, 2007)
- Detección de anomalías (Chandola et al., 2009; Aggarwal, 2017)
- Análisis de estabilidad (Meinshausen & Bühlmann, 2010)
- Combinación de clasificadores (Kittler et al., 1998)

**Veredicto**: Sin esto, la tesis no pasa un comité académico.

### 2.2 Métricas

**v1.0**: `coverage × (1-pearson_correlation)`
- Problema: Pearson sobre variables binarias = phi coefficient
- Phi es inestable cuando la frecuencia marginal tiende a 0 o 1
- Muchos títulos de pliegos son raros (<10% de documentos)
- El producto no tiene justificación estadística

**v2.0**: `α·z(cov) + β·z(1-NMI) + γ·z(synthAUC)`
- MI normalizada: no paramétrica, simétrica, detecta dependencias no lineales
- Synthetic AUC: mide directamente si los títulos sirven para detectar anomalías
- Suma z-normalizada: estándar en agregación de clasificadores

**Veredicto**: v2.0 es técnicamente correcto. v1.0 era una heurística ad-hoc.

### 2.3 Pipeline de 7 pasos

**v1.0**: 6 pasos con paso 5 manual (inspección humana)

**v2.0**: 7 pasos con:
- Paso 1: Filtro candidatos (frecuencia ≥ umbral + varianza > 0)
- Paso 3: Filtro redundancia (NMI > τ, retener el de mayor score)
- Paso 4: Evaluación wrapper (K de 5 a 20, score compuesto)
- Paso 6: Análisis de estabilidad (bootstrap + sensibilidad)

**Veredicto**: v2.0 es reproducible de extremo a extremo.

### 2.4 Validación

**v1.0**: "validar sin ground truth" sin método concreto

**v2.0**: 3 niveles progresivos:
- N1: Anomalías sintéticas → Synthetic AUC (0.5 días)
- N2: Concordancia IF/LOF/DBSCAN → Jaccard > 0.3 (1 día)
- N3: Ground truth externo DNCP si existe (1-2 días)

**Veredicto**: v2.0 tiene un blueprint ejecutable. v1.0 era vago.

---

## 3. Problemas de v2.0

### 3.1 Over-engineering

| Componente | Esfuerzo real estimado | ¿Necesario para tesis? |
|------------|:----------------------:|:-----------------------:|
| Bootstrap 100 iter | ~25 horas (200K docs × 5 estrategias × 100) | Sí, pero 30 bastan |
| Sensibilidad pesos (S3) | ~12.5 horas (50 perturbaciones × ranking) | No esencial |
| Synthetic AUC con 5 seeds | ~5 horas | Suficiente con 1 seed |
| 7 preguntas de tesis | Redacción ~2 días | Sí, pero se puede simplificar |

### 3.2 Synthetic AUC puede ser engañoso

El código del plan inyecta:
- 40%: feature al percentil 99 (cláusula sobredimensionada)
- 30%: feature a 0 (omisión de cláusula)
- 30%: dejar igual (contaminación leve)

Esto no refleja anomalías reales en pliegos:
- Anomalías reales son más sutiles (texto genérico, cláusulas ambiguas, redacción confusa)
- El detector puede aprender a encontrar "features extremas" sin capturar la semántica real

### 3.3 Complejidad de implementación

v2.0 requiere:
- MI normalizada para 323 títulos → discretización de variables continuas
- Synthetic AUC con inyección dirigida → diseño de patrones de mutación
- Bootstrap del ranking completo → ejecución paralelizable
- Kendall tau y Spearman para sensibilidad → múltiples ejecuciones

---

## 4. Plan híbrido adoptado

Se adopta **v2.0 como base** con los siguientes ajustes para reducir complejidad:

### 4.1 Cambios propuestos

| Sección | Cambio | Motivo |
|---------|--------|--------|
| **§2.4 Synthetic AUC** | Reducir de 5 seeds a **1 seed** con `seed=42` fijo | La literatura (Aggarwal 2017) recomienda ≥3, pero para tesis de grado 1 seed es suficiente. Duplicamos a 3 solo si el resultado es borderline. |
| **§8.2 Bootstrap (S2)** | Reducir de **100 a 30 iteraciones** | Meinshausen & Bühlmann muestran que 30-50 bastan para estimar frecuencias de selección. Ahorra ~70% de tiempo. |
| **§8.3 Sensibilidad pesos (S3)** | Mover a **trabajo futuro** (§9.3) | No es esencial para el pipeline. Se puede ejecutar una sola vez al final si sobra tiempo. |
| **§8.1 Sensibilidad K (S1)** | Mantener pero ejecutar **solo 3 pares** (K=8/10, 10/12, 8/12) | Suficiente para demostrar estabilidad sin ejecutar todas las combinaciones. |

### 4.2 Estimación de tiempo corregida

| Fase | v2.0 original | Plan híbrido |
|------|:-------------:|:------------:|
| F1: Marco teórico | 1 día | 1 día |
| F2: Métricas (A1+B3+C1) | 2-3 días | 2 días |
| F3: Automatización | 1-2 días | 1 día |
| F4: Stability (S1+S2) | 1-2 días | 1 día (30 iter bootstrap) |
| F5: Validación (N1+N2) | 1-2 días | 1 día |
| F6: Documentación | 2 días | 1 día |
| **Total** | **8-12 días** | **7 días** |

### 4.3 Preguntas de tesis (7 → 6)

Se mantienen las 7 preguntas de v2.0 (§12) ya que son defensables en comité. Se pueden reducir a 6 si se omite P7 (ground truth futuro), que es opcional.

---

## 5. Resumen ejecutivo

| Decisión | Resolución |
|----------|------------|
| **¿Cuál plan usar?** | v2.0 como base, con ajustes de simplificación |
| **¿Marco teórico?** | Sí, obligatorio para tesis. Referencias de Kohavi, Chandola, Aggarwal. |
| **¿Métrica de diversidad?** | MI normalizada (B3) en vez de Pearson (B1) |
| **¿Métrica de separabilidad?** | Synthetic AUC (C1) con 1 seed, simplificado |
| **¿Fórmula compuesta?** | Suma ponderada z-normalizada: `0.35·z(cov) + 0.35·z(div) + 0.30·z(auc)` |
| **¿Pipeline reproducible?** | Sí, 7 pasos sin pasos manuales |
| **¿Bootstrap?** | Sí, 30 iteraciones (no 100) |
| **¿Sensibilidad a pesos?** | No, mover a trabajo futuro |
| **¿Tiempo estimado?** | 7 días (vs. 8-12 de v2.0 vs. 1-2 de v1.0) |
