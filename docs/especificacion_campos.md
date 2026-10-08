# Especificación operativa de los campos híbridos (borrador v0.1 — 08-oct-2026)

> **Para qué**: una sola fuente de verdad por campo. De cada ficha salen tres cosas idénticas: la guía
> del prompt (`extraction_hint` en `master_schemas_v2.json`), la documentación de la regla en
> `extraction_postprocess.py` y las instrucciones del anotador. Hoy esas tres cosas difieren en dos
> campos (`dias_pago_maximo`, `num_documentos_pago`) y eso invalidó la medición de precisión.
> **Estado**: PROPUESTA. Cada ficha tiene una sección "Decisión" que el usuario debe aprobar (✅) o
> cambiar. Nada se aplica a prompts, reglas ni planillas hasta que la ficha esté aprobada.
> **Convención común a todos**: el valor es el que el pliego **fija como requisito o plazo**; si el pliego
> dice "No Aplica", deja el campo en blanco, remite a otro documento ("según EETT", "según plantilla
> SICP") o solo trae la plantilla sin completar, el valor es **NULL**. Nunca 0. Las cifras entre
> paréntesis prevalecen sobre las palabras ("quince (15) días" → 15). "Hábiles", "corridos" y
> "calendario" no cambian el número.

---

## Grupo A — Magnitudes con variación real (prioridad 1 y 2)

### A1. `dias_pago_maximo` — plazo de pago de la factura ⚠ prompt actual INCORRECTO
- **Mide**: en cuántos días se compromete la entidad a pagar una factura aceptada. Para anomalías:
  plazos muy cortos o muy largos respecto del estándar.
- **Regla de decisión (prioridad)**:
  1. Si la sección "Otras formas y condiciones de pago" (o equivalente redactado por la entidad) fija un
     plazo de pago contado desde la factura, la solicitud de pago o la recepción → ese valor.
  2. Si no hay plazo propio → la cláusula estándar "sin exceder sesenta (60) días después de la
     presentación de una factura" → 60.
- **No es el valor**: frecuencia de pago ("cada 30 días", "mensual"); plazo de aceptación de la factura
  (15); plazo para presentar la factura ("hasta el día 20 de cada mes"); plazo para entregar el recibo
  tras cobrar ("remitir el recibo en 8 días"); plazos de retención al fisco ("3 días hábiles").
- **Ejemplos**: "Dentro de los 30 días (de la entrega de la solicitud y factura)" + estándar 60 → **30**.
  "abonará… en un plazo de 30 días contados a partir de la presentación de la factura" → **30**.
  "El pago se realizará dentro de los 5 (cinco) días hábiles, contados a partir de la recepción de la
  solicitud de pago" → **5**. Solo cláusula estándar → **60**.
- **Trampa**: "Los pagos se harán efectivos cada 30 días, dentro de los 60 días calendarios" → **60**
  ("cada 30" es frecuencia). "remitir el recibo de dinero en un plazo no mayor a 8 días" → ignorar → 60.
- **Hoy**: el GT de agosto ya sigue esta regla (151 específicos + 49 estándar); la regla determinista
  ("primer N días") la aproxima (189/200); la **guía del prompt pide el techo estándar** → corregir guía.
- **Decisión**: [ ] aprobar regla "específico > estándar". [ ] Alternativa: medir solo el techo estándar
  (no recomendado: 69% sería 60 y el campo pierde valor).

### A2. `dias_validez_garantia` — vigencia de la garantía de los bienes
- **Mide**: por cuántos días el proveedor garantiza los bienes tras la recepción. Siempre en **días**.
- **Conversión fija**: 1 mes = 30 · 2 = 61 · 3 = 91 · 6 = 182 · 12 meses = 1 año = 365 · 18 meses = 548 ·
  24 meses = 2 años = 730 · 36 meses = 3 años = 1095. Si el texto da días y meses, mandan los días.
- **Un solo valor por documento**: si hay distintos por ítem/lote y no son iguales → **ambiguo**
  (se excluye de la métrica; en producción NULL). Si todos los ítems tienen el mismo → ese.
- **"Mínimo N" / "como mínimo N"** cuenta como N (es el requisito).
- **NULL**: "No Aplica", "según especificaciones técnicas", "según plantilla SICP", fecha fija ("hasta el
  31/01/2024"), garantía por kilómetros o por vida útil sin plazo, garantía del *servicio* (no del bien).
- **Trampas**: "Garantía de cumplimiento de contrato" (otra garantía) → ignorar. "antigüedad de fabricación
  no mayor a 1 año" → no es garantía. "24 horas / 7x24" → es soporte, no garantía.
- **Ejemplos**: "12 (Doce) meses contado desde la entrega" → 365. "6 (seis) meses o 20.000 km" → 182.
  "Para ítems 1 y 3: un año; ítem 2: dos años" → ambiguo.
- **Decisión**: [ ] aprobar. [ ] ¿Ítems con valores distintos: ambiguo (propuesto) o tomar el mínimo?

### A3. `dias_pago_anticipo` — plazo para pagar el anticipo
- **Mide**: días entre la solicitud del anticipo y su pago.
- **Ancla**: la cláusula "Plazo o fecha en la cual **se abonará** al contratista el monto del anticipo,
  siguiente a la fecha de la presentación de la solicitud: N días", o redacciones "a ser efectuado /
  abonado dentro de los N días desde la solicitud".
- **NULL**: la cláusula está vacía, dice "sujeto a disponibilidad del Plan de Caja" sin número, o el pliego
  solo fija el plazo para *solicitar* el anticipo. "No Aplica" → NULL.
- **Trampas**: plazo de **solicitud** (A4), plazo de rechazo del anticipo parcial ("podrá rechazarlo en 5
  días hábiles"), devolución de la garantía ("a más tardar 30 días contados desde el cumplimiento").
- **Ejemplos**: "se abonará… : DENTRO DE LOS 10 (DIEZ) DÍAS" → 10. "dentro de los cinco días hábiles" → 5.
  "a ser efectuado dentro de los 60 días corridos desde la solicitud" → 60. Solo "solicitar dentro de los 3
  días de firmado" → NULL.
- **Decisión**: [ ] aprobar.

### A4. `dias_solicitud_anticipo` — plazo para solicitar el anticipo
- **Mide**: días desde la firma/publicación del contrato para pedir el anticipo.
- **Ancla**: "Plazo máximo para la presentación de la solicitud de pago de anticipo: N días" o "solicitará
  dentro de los N días de la firma".
- **NULL**: "No Aplica", campo en blanco, "se regirá según la Ley 7050" sin número, o pliego que solo fija
  el porcentaje de anticipo.
- **Trampas**: el plazo de **pago** (A3); el plazo de rechazo (5 días hábiles); el plazo contado desde la
  aprobación del plan de trabajo sigue siendo válido (es plazo de solicitud).
- **Ejemplos**: "10 (DIEZ) DÍAS CORRIDOS POSTERIORES A LA FIRMA" → 10. "03 días hábiles posteriores a la
  firma" → 3. "ocho días hábiles" → 8.
- **Decisión**: [ ] aprobar.

### A5. `anos_experiencia_minima_tecnica` — años de experiencia exigidos (capacidad técnica)
- **Mide**: años mínimos de experiencia del oferente o de su personal técnico, en años (0,5 permitido).
- **Regla**: el primer "N años" cuyo contexto (±180 caracteres) mencione "experiencia". Si hay varios
  (empresa y personal), tomar el de la **empresa/oferente**; si solo hay del personal, ese.
- **NULL**: la sección no exige años ("deberá contar con experiencia en…" sin número); solo pide
  cantidad de contratos/referencias; "experiencias dentro de los últimos N años" (ventana temporal, no
  experiencia mínima); años de garantía; antigüedad del inmueble/alquiler; edad.
- **Decisión**: [ ] aprobar. [ ] ¿"últimos N años" se cuenta o no? (propuesto: NO).

### A6. `pct_monto_referencia_tecnica` — % del monto que deben cubrir las referencias técnicas
- **Mide**: porcentaje del monto ofertado/del lote que deben sumar los contratos o facturas presentados
  como referencia. Valor entero tal cual aparece (70, no 0,7).
- **NULL**: no hay porcentaje ligado a "monto/valor/oferta/lote/facturación".
- **Trampas**: reparto de consorcio ("líder 60%, demás 40%", "70%/30%") → NULL; retención 0,4% a la
  DNCP → NULL; porcentaje de anticipo → NULL.
- **Decisión**: [ ] aprobar.

### A7. `num_pasos_verificacion` — pasos del análisis del comité
- **Mide**: cantidad de pasos numerados ("1.", "1º", "1°") del análisis de prohibiciones del comité.
- **Regla**: máximo índice de la lista; numeración con punto o con ordinal. Hoy lo resuelve el regex
  (200/200 GT; 30/30 pseudo). **NULL** si el texto remite a "metodología que disponga la DNCP" sin lista.
- **Nota**: solo toma valores 6/7/8 según la versión de la ley; es un marcador de versión de plantilla,
  no una magnitud. **Decisión**: [ ] mantener en regex, fuera del benchmark LLM.

---

## Grupo B — Casi constantes: la señal es el desvío del estándar (prioridad 3)

### B1. `ratio_liquidez_minimo`
- **Mide**: activo corriente / pasivo corriente mínimo exigido. Estándar DNCP = 1.
- **Regla**: número que sigue a "Liquidez" con "igual o mayor a / ≥ / deberá ser / mínimo". Decimales con
  coma → punto (1,20 → 1.2). Corchetes "[1]" cuentan como 1.
- **NULL**: "[indicar índice]" sin número, "No Aplica", sección sin índices.
- **Métrica**: reportar precisión por separado en el estrato **≠ 1** (ahí vive la señal).
- **Decisión**: [ ] aprobar.

### B2. `ratio_endeudamiento_maximo`
- **Mide**: pasivo total / activo total máximo. Estándar = 0,80. "Coeficiente de solvencia (pasivo/activo)
  igual o menor a N" es sinónimo. Mismas reglas que B1. **Decisión**: [ ] aprobar.

### B3. `num_componentes_formula` — variables de entrada de la fórmula de reajuste
- **Mide**: cuántas variables de entrada tiene la fórmula (sin contar el resultado). Estándar
  Pr = P × IPC1/IPC0 → **3**.
- **Regla**: contar símbolos distintos del lado derecho; coeficientes numéricos (0,60, 0,40) no cuentan;
  umbrales ("15%") no cuentan; si hay dos fórmulas (nacional/importado) con igual cantidad → esa; si
  difieren → ambiguo.
- **NULL**: la sección describe el reajuste en prosa sin ecuación.
- **Ejemplos**: A = P × I.I.B.C.P → 2. P1 = P0·(0,60·S1/S0 + 0,4·IPC1/IPC0) → 5. Polinómica de obras
  con S,So,C,Co,G,Go,Po → 7. OCR roto → ambiguo.
- **Métrica**: precisión en el estrato **≠ 3**. Es el único campo que decide el LLM en producción.
- **Decisión**: [ ] aprobar.

---

## Grupo C — Plantilla pura: reclasificar a binario (prioridad 4, sin anotación)

### C1. `dias_notificacion_fuerza_mayor` → binario `tiene_clausula_fuerza_mayor_7d`
- 7 en el 100% de 29.831 docs con sección (559 textos distintos). Como magnitud no discrimina nada.
- **Decisión**: [ ] convertir a binario (presente/ausente) y sacar del benchmark de precisión.

### C2. `dias_aceptacion_rechazo_factura` → binario `tiene_clausula_aceptacion_15d`
- 15 en el 100%. Idem. **Decisión**: [ ] convertir a binario.

### C3. `num_documentos_pago` ⚠ prompt actual INCORRECTO
- **Mide**: documentos del catálogo genérico DNCP exigidos para cobrar (6 ó 7: el séptimo es el
  certificado laboral). Contar **por nombre**, no por marcador de lista (la lista puede venir como 1-6,
  a-g o sin marcadores). Hoy lo resuelve el regex (200/200 GT, 30/30 pseudo).
- La guía actual dice "ítems de primer nivel, no sub-ítems" → da 1 → **corregir guía** para que coincida.
- Toma solo 2 valores → información baja; equivale a "exige certificado laboral (sí/no)".
- **Decisión**: [ ] mantener en regex con guía corregida, fuera del benchmark LLM. [ ] Alternativa:
  binario `exige_certificado_laboral`.

---

## Qué cambia al aprobar
1. Se reescriben los `extraction_hint` de A1 y C3 (y se alinean los demás con el texto de la ficha).
   Cambia el prompt → nuevo fingerprint → re-correr Gemma-4-12B QAT, Qwen3-14B y Qwen3.8-27B sobre GT
   y pseudo-GT (~1 h GPU).
2. Se corrigen mis 9 anotaciones del pseudo-GT en `dias_pago_maximo` (seguí la guía vieja).
3. La métrica del benchmark pasa a reportarse en 3 columnas por campo: total, estrato no-modal,
   ausencias; C1/C2 salen del promedio.
4. Ronda de calibración (`planillas/calibracion_v1.csv`, 10 ítems × 10 campos de los grupos A y B,
   sobremuestreando el estrato no-modal): usuario y Opus anotan por separado con esta ficha → κ. Si κ < 0,8
   en un campo, se corrige la ficha, no la anotación.
