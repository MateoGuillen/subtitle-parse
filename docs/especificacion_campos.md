# Especificación operativa de los campos híbridos (v1.0 aprobada — 08-oct-2026)

> **Para qué**: una sola fuente de verdad por campo. De cada ficha salen tres cosas idénticas: la guía
> del prompt (`extraction_hint` en `master_schemas_v2.json`), la documentación de la regla en
> `extraction_postprocess.py` y las instrucciones del anotador. Hoy esas tres cosas difieren en dos
> campos (`dias_pago_maximo`, `num_documentos_pago`) y eso invalidó la medición de precisión.
> **Estado**: APROBADA ficha por ficha el 08-oct-2026 tras leer cada sección completa en la BD
> (`scripts/analisis_seccion_*.py`). Pendiente: implementación (ver "Qué cambia al aprobar").
> **Convención común a todos**: el valor es el que el pliego **fija como requisito o plazo**; si el pliego
> dice "No Aplica", deja el campo en blanco, remite a otro documento ("según EETT", "según plantilla
> SICP") o solo trae la plantilla sin completar, el valor es **NULL**. Nunca 0. Las cifras entre
> paréntesis prevalecen sobre las palabras ("quince (15) días" → 15). "Hábiles", "corridos" y
> "calendario" no cambian el número. Con varios valores genuinos del mismo concepto: el **máximo**.

---

## Grupo A — Magnitudes con variación real (prioridad 1 y 2)

### A1. `dias_pago_maximo` — plazo de pago comprometido por factura ✅ APROBADA (08-oct-2026)
- **Mide**: en cuántos días se compromete la entidad a pagar **cada factura** desde que la recibe (o desde la
  solicitud, la recepción o el certificado). Para anomalías: plazos muy cortos o muy largos respecto del
  estándar (60). Con la regla v1: < 60 en 8.333 docs, = 60 en 22.624, > 60 en 35.
- **Qué NO mide** (aclaración del 08-oct, a pedido del usuario): ni la duración total de los pagos del
  contrato, ni la frecuencia ("cada 30 días"), ni el intervalo entre cuotas ("60 días del primer pago"), ni
  el diferimiento del saldo a otro ejercicio. Eso lo miden A9 (`pago_plurianual`) y A10 (`num_tramos_pago`).
  Caso 473007: plazo por factura **30**, pero 50% del contrato supeditado al presupuesto 2026 → A9 = 1,
  A10 = 3. Caso 449660: "cada 30 días, dentro de los 60 días calendarios" → **60**. El nombre de la columna
  no cambia.
- **Evidencia BD** (`scripts/analisis_seccion_pago.py`, 18.418 textos distintos / 31.841 secciones):
  la entidad escribe entre "Otras formas y condiciones de pago… serán las siguientes:" y la cláusula
  estándar "La Contratante efectuará los pagos… sin exceder sesenta (60) días". Plazo propio con número:
  36% de los docs; texto propio sin número: 42%; tramo vacío: 14%; variante sin ancla: 7%. Resultado
  esperado: 60 en 82% de los docs, 21 valores distintos (30: 8,7% · 15: 3,1% · 10: 2,6% · 5: 1,8% · 20: 1,3%).
- **Regla de decisión (prioridad)**:
  1. Plazo propio de pago fijado por la entidad → ese valor. Vale cualquier origen: factura (3.285
     textos), solicitud de pago (1.600), recepción/entrega (714), presentación de documentos (146),
     aprobación del certificado de obra (102) u origen no explícito (1.157).
  2. Sin plazo propio con número → la cláusula "sin exceder N días después de la presentación de una
     factura" (60; si la entidad la modificó, el valor que diga: 15 textos dicen 30 o 5).
  3. Dos plazos de pago genuinos y distintos (<0,5% de los textos) → el **mayor**.
  4. Plazos > 60 se toman tal cual si son plazo de pago ("Plazo de pago: 120 días desde la aprobación
     del certificado" → 120).
- **No es el valor** (trampas medidas en BD): "cada N días" = frecuencia (616 textos; "mensual" sin número
  no aporta valor); aceptación/rechazo de la factura (606); retenciones al Tesoro/DNCP (614); presentar el
  recibo tras cobrar (392); cronograma de cuotas "N días después de la primera solicitud" (299); completar
  el FIS "30 días desde el código de contratación" (231); emplazamiento FIP/FIS "3 días hábiles" (215);
  presentar la factura "los primeros N días de cada mes" (130); emisión de actas; garantías.
- **Ejemplos**: "Dentro de los 30 días (de la entrega de la solicitud y factura)" → **30**. "PAGO MENSUAL,
  DENTRO DE LOS 10 DIAS POSTERIORES A LA PRESENTACION DE LA FACTURA" → **10**. "cada 30 días, dentro de
  los 60 días calendarios" → **60**. "pagos dentro de los 60 días… dentro de los 5 días hábiles siguientes
  a la acreditación presentar el recibo" → **60**. "presentar la solicitud dentro de 30 días desde la
  entrega… El pago se realizará a los 20 días" → **20**. "sin exceder sesenta (30) días" → **30**
  (manda el paréntesis). Solo cláusula estándar → **60**.
- **Hábiles vs corridos** (1.833 textos con "hábiles"): el número **tal cual**, sin convertir; se marca
  aparte el flag `pago_en_dias_habiles` (decisión 08-oct).
- **Flag adicional** `pago_sujeto_plan_caja` ("sujeto a disponibilidad del Plan de Caja / Plan Financiero /
  disponibilidad presupuestaria / transferencia de fondos"): binario, mismo texto, sin costo extra
  (decisión 08-oct). Medido sobre la sección completa (I3): 8.405 docs (27%); los 1.235 textos del
  análisis inicial contaban solo menciones junto a un plazo.
- **Guía del prompt (extraction_hint)**:
  ```
  Plazo en días en que la contratante se compromete a PAGAR al proveedor.
  1) Si el pliego fija un plazo propio de pago (normalmente tras "Otras formas y condiciones de pago"),
     responde ese número, contado desde factura, solicitud de pago, recepción, entrega, presentación de
     documentos o aprobación del certificado.
     Ej: "Dentro de los 30 días (de la entrega de la solicitud y factura)" -> 30.
     Ej: "PAGO MENSUAL, DENTRO DE LOS 10 DIAS POSTERIORES A LA FACTURA" -> 10.
     Ej: "Plazo de pago: 120 (ciento veinte) días, desde la aprobación del certificado" -> 120.
  2) Si no hay plazo propio con número, responde el de la cláusula "sin exceder N días después de la
     presentación de una factura" (normalmente 60; si la entidad lo cambió, el que diga).
  3) Si hay dos plazos de pago distintos, responde el mayor.
  NO son plazo de pago: "cada N días" (frecuencia); aceptación o rechazo de la factura (15); presentar el
  recibo tras cobrar; plazos de cuotas ("N días después de la primera solicitud"); completar el FIS o FIP;
  emplazamientos; presentar la factura "los primeros N días"; retenciones al Tesoro; emisión de actas;
  garantías. Hábiles o calendario no cambian el número. Si palabra y paréntesis difieren, manda el
  paréntesis: "sesenta (30)" -> 30.
  ```
- **Pendiente de implementación**: (1) reemplazar `extraction_hint`; (2) revisar la regla determinista
  ("primer N días") contra las trampas de arriba — FIS, cuotas, recibo y aceptación pueden aparecer antes
  del plazo real; (3) agregar los dos flags; (4) corregir mis 9 anotaciones del pseudo-GT.

### A2. `dias_validez_garantia` — vigencia de la garantía de bienes/servicios ✅ APROBADA (08-oct-2026)
- **Mide**: por cuántos días el proveedor garantiza los bienes **o servicios** tras la entrega/recepción.
  Siempre en **días**. Para anomalías: exigencias de garantía inusualmente largas (barrera) o cortas.
- **Evidencia BD** (`scripts/analisis_seccion_garantia.py`, 6.374 textos distintos / 24.795 secciones):
  plantilla "El plazo de validez de la Garantía de los bienes será el siguiente: ‹texto›". "No Aplica"
  58,7% de los docs · un solo plazo 31,3% · varios plazos 4,3% · remite a EETT 3,5% · texto sin número
  2,0% · solo km/horas o fecha fija 0,2%. Con un plazo: 51 valores distintos (365: 55% · 182 · 730 ·
  1095 · 30 · 91 · 180 · 90…). Unidades: meses 4.250 docs, años 2.652, días 1.015.
- **Conversión fija**: 1 mes = 30 · 2 = 61 · 3 = 91 · 6 = 182 · 12 meses = 1 año = 365 · 18 meses = 548 ·
  24 meses = 2 años = 730 · 36 meses = 3 años = 1095 · 4 años = 1460 · 5 años = 1825. Días escritos se
  toman tal cual ("180 días" → 180; así lo hizo el GT de agosto). Si el texto da días y meses, mandan
  los días.
- **Varios plazos distintos** (por lote/ítem/componente, 1.057 docs) → el **máximo**, y se marca el flag
  `garantia_varia_por_item` (decisión 08-oct). Si todos coinciden → ese valor, sin flag.
- **"Mínimo N" / "como mínimo N"** cuenta como N (2.158 docs).
- **Tiempo o km/horas** ("12 meses o 50.000 km") → el tiempo. Solo km/horas → NULL.
- **Servicios incluidos** (decisión 08-oct; 788 textos dicen "bienes/servicios"): "La garantía de los
  servicios será de 30 (treinta) días corridos posteriores al servicio" → 30.
- **NULL**: "No Aplica"; remite a EETT/suministros requeridos/plantilla SICP; fecha fija; solo km/horas;
  "por toda la vigencia del contrato" sin número (216 textos; decisión 08-oct: NULL, la duración del
  contrato ya está en la tabla económica). Con número sí se toma: "30 días posteriores al plazo de
  vigencia del contrato" → 30.
- **Trampas** (medidas en BD): plazo de reposición/reemplazo (357 textos); vencimiento de productos o de
  otra garantía (342: "vencimiento de los medicamentos de 12 meses", "renovar la garantía de fiel
  cumplimiento 60 días antes del vencimiento"); antigüedad de fabricación (35); soporte/tiempo de
  respuesta "24 horas" (32); garantía de cumplimiento de contrato (14).
- **Ejemplos**: "12 MESES" → 365. "2 AÑOS COMO MINIMO" → 730. "Motor: 12 meses o 50.000 Km" → 365.
  "12 meses para el lote 1. 36 meses para el lote 2." → 1095 + flag. "SESENTA DIAS POSTERIORES A LA
  RECEPCION DEL BIEN" → 60. "Según las especificaciones técnicas de cada ítem." → NULL.
- **Guía del prompt (extraction_hint)**:
  ```
  Plazo de validez de la garantía de los bienes o servicios, en DÍAS.
  Convierte: 1 mes=30, 2 meses=61, 3 meses=91, 6 meses=182, 12 meses o 1 año=365, 18 meses=548,
  24 meses o 2 años=730, 36 meses o 3 años=1095, 4 años=1460, 5 años=1825. Días escritos: tal cual.
  "Como mínimo N" cuenta como N. Si dice tiempo o kilómetros/horas, usa el tiempo.
  Si hay plazos distintos por lote, ítem o componente, responde el MAYOR.
  Ej: "12 MESES" -> 365. "2 AÑOS COMO MINIMO" -> 730. "12 meses lote 1; 36 meses lote 2" -> 1095.
  Responde NULL si: "No Aplica"; remite a especificaciones técnicas o suministros requeridos; fecha
  fija; solo kilómetros u horas; "por toda la vigencia del contrato" sin número.
  NO es la garantía: plazo de reposición o reemplazo; vencimiento de productos (medicamentos,
  insumos); renovación de la garantía de fiel cumplimiento; antigüedad de fabricación; soporte
  "24 horas"; presentación de la garantía de cumplimiento de contrato.
  ```
- **Pendiente de implementación**: hint; flag `garantia_varia_por_item`; revisar la regla determinista
  (hoy toma el primer plazo, no el máximo) y que la conversión de días escritos sea literal.

### Evidencia común A3/A4 (`scripts/analisis_seccion_anticipo.py`, 5.083 textos distintos / 32.182 secciones)
- Plantilla: "El plazo dentro del cual se solicitará el anticipo será (en días corridos) de: ‹texto›" +
  texto legal fijo ("1. El anticipo es la suma de dinero…", 46 variantes) que contiene dos plazos que
  **no son de estos campos**: "podrá rechazarlo en el plazo de cinco (5) días hábiles" (pago parcial) y
  "la Garantía de Anticipo será… devuelta… a más tardar treinta (30) días". Trampa sistemática.
- "No Aplica" 72,8% de los docs; con anticipo 27,2% (8.764 docs), de los cuales 67% es **solo MIPYMES**
  (Ley 7050 art. 188). Decisión 08-oct: se extrae igual; la condición ya es `binary_anticipo_solo_mipymes`.
- Días hábiles en 2.248 docs: número tal cual (misma decisión que A1).

### A3. `dias_pago_anticipo` — plazo para pagar el anticipo ✅ APROBADA (08-oct-2026)
- **Mide**: días entre la solicitud del anticipo y su pago.
- **Ancla**: "Plazo o fecha en la cual **se abonará** al contratista el monto del anticipo, siguiente a la
  fecha de la presentación de la solicitud: N días". Con número cerca: 3.652 docs; número más adelante
  ("Se verificarán las documentaciones… se abonará… a más tardar dentro de los 10 días calendarios"):
  1.063; sin número: 148; sin ancla: 3.901 (redacción libre "a ser efectuado dentro de los N días").
- **Valores**: 10 (1.817 docs) · 30 (622) · 5 (475) · 15 (401) · 60 · 7 · 3 · 20 · 2 · 45 — 15 distintos.
- **Origen distinto de la solicitud** (45 docs, "10 DIAS HÁBILES LUEGO DE LA FIRMA DEL CONTRATO" escrito en
  el ancla de pago): se toma (decisión 08-oct).
- **NULL**: "No Aplica"; ancla vacía; sin número y sujeto a verificación/transferencia del Ministerio/
  disponibilidad presupuestaria → NULL **+ flag `anticipo_sujeto_disponibilidad`** (decisión 08-oct;
  par de `pago_sujeto_plan_caja` de A1); pliego que solo fija el plazo de **solicitud**.
- **Trampa principal**: "La Contratante **abonará** al Proveedor el 5%… Dentro de los diez (10) días
  posterior a la firma, el Proveedor deberá presentar la **solicitud**" → el 10 es A4; A3 = NULL.
  Otras: rechazo del pago parcial (5 días hábiles), devolución de la garantía (30 días), plazo de
  aceptación de la garantía ("24 horas").
- **Formatos**: "DENTRO DE LOS 10 (DIEZ) D Í A S" → 10; "(tres) días como máximo" → 3.
- **Guía del prompt (extraction_hint)**:
  ```
  Plazo en días para que la contratante PAGUE (abone) el anticipo, contado desde la solicitud.
  Busca: "Plazo o fecha en la cual se abonará al contratista el monto del anticipo...: N días"
  (el número puede venir varias frases después: "...se abonará el monto total del anticipo, a más
  tardar dentro de los 10 días" -> 10), o "a ser efectuado/abonado dentro de los N días".
  Si el plazo de pago se cuenta desde la firma del contrato, también vale.
  Ej: "DENTRO DE LOS 10 (DIEZ) DÍAS" -> 10. "(tres) días como máximo" -> 3.
  Responde NULL si: "No Aplica"; el ancla está vacía; el pago depende del Ministerio, de la
  transferencia o de disponibilidad presupuestaria sin número; el pliego solo fija el plazo para
  SOLICITAR el anticipo.
  NO es este plazo: el plazo para presentar la solicitud ("dentro de los 10 días posteriores a la
  firma el proveedor deberá presentar la solicitud", aunque la frase anterior diga "abonará");
  "podrá rechazarlo en el plazo de cinco (5) días hábiles"; devolución de la garantía "treinta (30)
  días"; aceptación de la garantía en horas.
  ```

### A4. `dias_solicitud_anticipo` — plazo para solicitar el anticipo ✅ APROBADA (08-oct-2026)
- **Mide**: días desde la firma (u otro hito) para pedir el anticipo.
- **Ancla**: "Plazo máximo para la presentación de la solicitud de pago de anticipo: N días" (5.830 docs con
  número); sin ancla 2.776 (redacción libre "presentar dentro de los 5 días posteriores a la firma", o solo
  cita a la Ley 7050 sin número); ancla vacía 158 ("Haga clic o pulse aquí para escribir texto").
- **Valores**: 10 (2.849 docs) · 5 (1.794) · 3 (707) · 15 (251) · 7 · 8 · 20 · 30 — 13 distintos.
- **Origen**: firma, publicación, orden de inicio, aprobación del plan de trabajo o hitos propios ("7 días de
  haber recibido de Petropar la estimación de venta") → todos valen.
- **Formatos**: "cinco [05] días" → 5; "10 (diez) corridos posteriores a la firma" → 10 (sin la palabra
  "días"); "(diez) días calendarios" → 10.
- **NULL**: "No Aplica"; ancla vacía/placeholder; solo cita la ley o solo fija el porcentaje.
- **Trampas**: plazo de pago del anticipo (A3); rechazo del pago parcial (5 días hábiles); devolución de la
  garantía (30 días).
- **Guía del prompt (extraction_hint)**:
  ```
  Plazo máximo en días para que el proveedor SOLICITE (presente la solicitud de) el anticipo.
  Busca: "Plazo máximo para la presentación de la solicitud de pago de anticipo: N días" o "deberá
  presentar dentro de los N días posteriores a la firma la solicitud". Vale cualquier hito de inicio
  (firma, orden de inicio, aprobación del plan, recepción de un documento).
  Ej: "10 (DIEZ) DÍAS CORRIDOS POSTERIORES A LA FIRMA" -> 10. "cinco [05] días" -> 5.
  "10 (diez) corridos posteriores a la firma" -> 10.
  Responde NULL si: "No Aplica"; el campo está vacío o dice "Haga clic o pulse aquí"; solo cita la
  ley o el porcentaje del anticipo.
  NO es este plazo: el plazo en que la contratante abonará el anticipo; "podrá rechazarlo en el plazo
  de cinco (5) días hábiles"; devolución de la garantía "treinta (30) días".
  ```
- **Pendiente de implementación (A3+A4)**: hints; flag `anticipo_sujeto_disponibilidad`; revisar que la
  regla determinista excluya el texto legal fijo (desde "1. El anticipo es la suma de dinero").

### Evidencia común A5/A6 (`scripts/analisis_seccion_capacidad.py`, 15.155 textos distintos / 26.911 secciones)
- Sección "Capacidad técnica": texto libre, sin ancla de plantilla. "No Aplica" en 5.344 docs.
- **Fusión con los gemelos regex** (decisión 08-oct): el mismo concepto aparece en "Experiencia requerida"
  (`anos_experiencia_minima`, `pct_monto_referencia_minimo`). Las extracciones por sección se mantienen en
  v3 (trazabilidad); la capa que alimenta Pipeline 3 usa una sola variable por concepto:
  `anos_experiencia_exigidos` = máximo de ambas secciones; `pct_monto_referencia` = el de "Experiencia
  requerida" y, si falta, el de "Capacidad técnica".

### A5. `anos_experiencia_minima_tecnica` — años de experiencia exigidos ✅ APROBADA (08-oct-2026)
- **Mide**: años mínimos de experiencia o antigüedad exigidos al oferente o a su personal. Barrera de
  entrada (Ferwerda 2016, preferred supplier r=0,487).
- **Evidencia**: solo 3.730 docs (14%) mencionan "N años"; por tipo: antigüedad de la empresa ~1.090,
  experiencia del personal 941, ventana "últimos N años" 639, experiencia de la empresa 585, trampas
  ~1.250. Valores dominantes 3 y 5; luego 2, 10, 1.
- **Regla de decisión**:
  1. Cuenta la experiencia de la **empresa**, incluida la **antigüedad en el rubro o existencia legal**
     ("antigüedad mínima de tres (03) años en el rubro" → 3; "Existencia legal de por lo menos tres (3)
     años" → 3) (decisión 08-oct).
  2. Si no hay de la empresa, la del **personal** (profesionales, técnicos, "N años de ejercicio de la
     profesión", "desde la expedición del título", antigüedad del técnico en el plantel). Incluye la
     experiencia en una especialidad: "Experiencia de por lo menos 1 (uno) año como Especialista en
     Medicina Nuclear demostrado con Certificados o Constancias" → **1** (caso 408117, decisión 08-oct).
  3. Varios valores del mismo nivel → el **máximo** (decisión 08-oct).
- **NULL**: "No Aplica"; exige experiencia sin número; solo pide cantidad de contratos/referencias;
  **"en los últimos N años"** (ventana para contar contratos, 639 docs; decisión 08-oct).
- **Trampas**: "Garantizar por 4 años la provisión de repuestos"; "registro sanitario con antelación
  mínima de 3 años"; "no poseer rescisión… en 2 años anteriores"; "vigencia mínima de 2 años";
  garantías; antigüedad o modelo de vehículos/equipos; edad.
- **Guía del prompt (extraction_hint)**:
  ```
  Años mínimos de experiencia o antigüedad que se EXIGEN al oferente o a su personal.
  1) Experiencia o antigüedad de la empresa en el rubro, incluida la existencia legal:
     "antigüedad mínima de tres (03) años en el rubro" -> 3; "mínimo de 2 años de experiencia" -> 2.
  2) Si no hay de la empresa, la del personal: "Ingeniero con como mínimo 3 (tres) años de
     experiencia" -> 3; "5 años de ejercicio de la profesión" -> 5;
     "Experiencia de por lo menos 1 (uno) año como Especialista en Medicina Nuclear" -> 1.
  3) Si hay varios valores, responde el MAYOR.
  Responde NULL si no hay número de años exigido, si solo pide cantidad de contratos, o si los
  años son una ventana: "en los últimos 3 años", "durante los últimos 5 años".
  NO son experiencia: años de provisión de repuestos, garantía, vigencia de documentos, antelación
  de registros sanitarios, años sin rescisiones, antigüedad o modelo de vehículos y equipos, edad.
  ```

### A6. `pct_monto_referencia_tecnica` — % del monto que deben cubrir las referencias ✅ APROBADA (08-oct-2026)
- **Mide**: porcentaje del monto ofertado/del lote/referencial que deben sumar las facturas o contratos
  presentados como experiencia. Entero tal cual (50, no 0,5).
- **Evidencia**: 2.477 docs tienen algún %, pero el % de referencias sobre el monto aparece solo en ~180
  (0,7%): por eso se fusiona con `pct_monto_referencia_minimo` (69% de cobertura). Valores: 50 (82 docs),
  30, 40, 60, 5, 10, 100. Otros %: consorcio 1.944 docs, otros con "monto" 249, varios ~280.
- **Alternativas** ("la sumatoria… como mínimo el 40% del monto referencial; o al menos una referencia
  por el 20%", 10 docs) → el de la **sumatoria** (decisión 08-oct).
- **NULL**: no hay % ligado a facturación/contratos/referencias sobre el monto. La "solvencia/capacidad
  económica del N% del monto" NO es A6: va a la variable nueva A8 (`pct_respaldo_financiero_monto`).
- **Varios % de referencias genuinos no alternativos** ("facturaciones por el 50%… volumen de negocio del
  60%") → el **máximo** (convención común).
- **Trampas**: reparto de consorcio ("la líder deberá cumplir con al menos 60%"); capacidad de entrega
  ("entregar hasta el 30% de los productos en 5 días"); tope de precio ("no deberá superar el 65% del
  precio fijado"); "% de los animales necesarios"; pureza/germinación de semillas; "el 70% del personal
  con carnet"; concentraciones de productos ("alcohol al 70%"); "cumplir el 25% de los requisitos";
  retención 0,4%; anticipo.
- **Guía del prompt (extraction_hint)**:
  ```
  Porcentaje del monto (ofertado, del lote o referencial) que deben alcanzar las facturas, contratos
  o referencias presentados como experiencia. Responde el número tal cual: "50%" -> 50.
  Ej: "facturaciones por un monto equivalente al 50% del monto máximo de lote ofertado" -> 50.
  Si hay alternativas ("la sumatoria como mínimo el 40%... o una referencia por el 20%"), responde
  el de la sumatoria (40).
  Responde NULL si no hay porcentaje ligado al monto de las referencias.
  NO es este porcentaje: reparto entre integrantes de un consorcio; capacidad de entrega; tope de
  precio; porcentaje de personal, de animales o de requisitos; pureza o concentración de productos;
  retención 0,4%; anticipo.
  ```
- **Pendiente de implementación (A5+A6)**: hints; vista/columnas fusionadas para Pipeline 3; revisar las
  reglas deterministas (A5 hoy toma el primer "N años" con "experiencia": no cubre antigüedad en el
  rubro ni excluye "últimos N años" en todas sus formas).

### A8. `pct_respaldo_financiero_monto` — respaldo financiero exigido como % del monto ✅ NUEVA (08-oct-2026)
- **Origen**: decisión del usuario en I4 ("solvencia como variable nueva"). El GT de agosto contaba la
  "solvencia económica del N% del monto" como % de referencias (A6); la ficha A6 no la cubre.
- **Mide**: % del monto (ofertado / máximo del llamado / contrato) que el oferente debe demostrar como respaldo
  financiero. Barrera financiera proporcional al tamaño del contrato.
- **Formas medidas en BD** (853 docs, 2,7%): capital operativo "(Activo Corriente − Pasivo Corriente)… no
  inferior al 15% del monto" (plantilla, 563 docs) · línea de crédito "por un monto no menor al 20% del total
  ofertado" · solvencia/capacidad económica "del 30% como mínimo del monto máximo… extracto bancario" ·
  disponibilidad de fondos. Valores: 15 · 50 · 30 · 20 · 10 · 40 · 60 · 80 · 100.
- **Secciones**: capacidad financiera, capacidad técnica, experiencia requerida y sus requisitos documentales
  (`field_rules_v1.SOLVENCIA_TITLES`). Varios valores → el **máximo** (convención común).
- **NULL**: no exige respaldo proporcional. Flag `exige_respaldo_financiero` (0/1).
- **Trampas**: anticipo "conforme a su disponibilidad presupuestaria… hasta el 20%"; "solvencia en la
  prestación de servicios… volumen de 50%" (es facturación → A6); margen/coeficiente de solvencia (ratios B2);
  garantías, multas, retenciones, consorcio.
- **Método**: regla regex, sin LLM. Columnas en v3: `pct_respaldo_financiero_monto`, `exige_respaldo_financiero`.

### Evidencia común A9/A10 (`scripts/analisis_seccion_pago_estructura.py`, 08-oct-2026)
- Tramo propio de la entidad en "formas y condiciones de pago" (el mismo que A1): 29.489 docs con sección,
  23.023 con texto propio no vacío. Barrido de términos además del plazo: sujeto a disponibilidad/plan de
  caja 29,5% (ya es `pago_sujeto_plan_caja`), días hábiles 10,4% (ya es `pago_en_dias_habiles`),
  **plurianual / pagos en otro ejercicio ~11%**, **pago en tramos con %/cuotas ~1,3%**, contra entrega 2,4%
  (sobre todo PETROPAR; depende de la entidad, descartado), certificado de obra 13% (refleja el tipo de
  contrato, descartado), pago por adelantado 3 docs y moneda extranjera 10 docs (demasiado raros).
- **Validación externa** contra `dncp.pagos_contrato` (276.868 pagos reales con fecha de factura y de pago;
  demora observada = fecha_pago − fecha_factura; mediana global 40 días, p90 157):

  | Grupo (docs con pagos) | pagos por contrato | demora mediana | % pagos > 60 días | paga ≥2 años tras la firma |
  |---|---|---|---|---|
  | A9 = 0 (17.909) | 2 | 30 | 21,0% | 14,8% |
  | **A9 = 1 (2.248)** | **6** | **37** | **34,4%** | **30,0%** |
  | A10 = 0 (19.975) | 2,8 | 30 | 22,4% | 16,4% |
  | **A10 ≥ 2 (182)** | 3 | 34 | **30,2%** | **29,7%** |

  Las dos variables se asocian a pagos más demorados y más tardíos en la ejecución real: no son ruido de
  plantilla.

### A9. `pago_plurianual` — el pago abarca más de un ejercicio fiscal ✅ APROBADA (08-oct-2026)
- **Origen**: caso 473007 (discusión de I4): el plazo por factura (30) esconde que el 50% del contrato queda
  supeditado al presupuesto del año siguiente.
- **Mide**: 1 si el pliego declara que los pagos del contrato caen en **dos o más ejercicios fiscales**
  (ejecución presupuestaria plurianual, Ley 7021/22 art. 43-44 / Ley 2051/03 art. 14). Para anomalías: el
  proveedor financia a la entidad y asume el riesgo de que no se apruebe el presupuesto → desalienta a los
  oferentes sin respaldo financiero.
- **Regla (regex, sin LLM)**, en orden:
  1. Menciona "plurianual" en forma afirmativa → 1 (2.503 docs). Formas típicas: "Por tratarse de una
     contratación de carácter plurianual…", "El llamado es Plurianual, 2025-2026-2027", "ejecución
     presupuestaria plurianual", "los desembolsos serán realizados en forma plurianual".
  2. Asigna pagos o presupuesto a **dos o más años ≥ al de publicación** → 1 (641 docs): "con recursos del
     presupuesto correspondiente al ejercicio fiscal del año 2024 y 2025"; "Ejercicio 2021 y sujeto a
     disponibilidad presupuestaria para el Ejercicio 2022".
  3. "El saldo…" atado a un ejercicio posterior al de publicación → 1 (10 docs): "El pago del 50% restante del
     Certificado Final será abonado en el Ejercicio Fiscal 2024".
  4. Si no → 0 (también si la entidad no escribió texto propio: solo plantilla). Sin sección → NULL.
  Sin el ancla "Otras formas y condiciones de pago" se lee la sección completa. Los años sueltos en lista
  cuentan ("del año 2024 y 2025", "2025-2026-2027").
- **No cuenta** (trampas medidas): plantilla condicional "En caso de que la contratación sea plurianual, los
  pagos… estarán sujetos…" (325 docs, texto genérico); negación "EL LLAMADO NO ES PLURIANUAL" (2 docs);
  "Ejercicio Fiscal 2023" solo, en un llamado publicado en 2022 (ad referéndum para un único ejercicio: un solo
  año, no dos).
- **Precisión**: 29/30 en una muestra aleatoria leída (el error era la negación, ya excluida).
- **Decisión (08-oct)**: sin binario aparte `pago_ad_referendum` (es calendario presupuestario, no forma de pago).
- **Implementado (I3)**: `field_rules_v1.pago_plurianual(text, year)`; columna en v3: **4.333 docs = 1** (14%),
  26.659 = 0, 329 NULL. Los ~1.200 extra respecto del análisis vienen sobre todo de pliegos sin ancla (452) y de
  listas de años; 13/14 correctos en una muestra leída.

### A10. `num_tramos_pago` — pago dividido en tramos o cuotas definidos ✅ APROBADA (08-oct-2026)
- **Mide**: en cuántos tramos con monto o % definido se divide el pago del contrato ("Primer pago: 20%…
  Segundo pago: 40%… Tercer pago: 40%" → 3). 0 = pago por factura o periódico sin tramos definidos. Para
  anomalías: retener una parte grande del pago hasta el final (o diferirla 12-36 meses tras la entrega)
  traslada el riesgo al proveedor; tramos atados a hitos son normales en consultorías y obras.
- **Regla (regex, sin LLM)**, en orden:
  1. Dos o más % de pago que **suman 90-110** → el número de % (162 docs): "Al entregar los bienes: 85% del
     monto total… al entregar las documentaciones legales: el 15%" → 2; "Se pagará dentro del ejercicio 2022:
     el 40%… 2023: el 30%… 2024: el 30%" → 3.
  2. "en N cuotas / pagos / etapas / tramos" → N (51 docs): "el 100% dividido en 6 (seis) pagos trimestrales"
     → 6; "en DOS (2) ETAPAS" → 2.
  3. Ordinales "primer… segundo/tercer pago, cuota o desembolso" → cantidad de ordinales (41 docs).
  4. Un % de pago seguido de "saldo / restante" → 2 (40 docs): "el 55%… el restante 45% será pagado a
     principios del Ejercicio Fiscal 2024" → 2.
  5. Si no → 0. Sin sección → NULL.
- **Trampas medidas**: "con anterioridad al **primer pago**" del FIS (348 docs, plantilla); "para el primer
  pago presentar la documentación"; % de multa, retención, contribución 0,4/0,5%, garantía de fiel
  cumplimiento o de anticipo, póliza, IVA, "avance de 40%" (es condición, no tramo); "100% del monto" (pago
  único).
- **Precisión**: 25/30 tramos genuinos en muestra aleatoria leída; 2 son "anticipo 20% + 80% restante" y 3
  no se pudieron confirmar con el fragmento. Conteo N aproximado (ordinales repetidos por lote pueden
  inflarlo).
- **Decisiones (08-oct)**: el anticipo **no** es tramo ("anticipo 20% + 80% restante" → 0; un ordinal seguido de
  "anticipo" no se cuenta). Se guarda **el número**; para el modelo se usa el binario `num_tramos_pago > 0`
  hasta validar el conteo.
- **Implementado (I3)**: `field_rules_v1.num_tramos_pago(text)`; columna en v3: **331 docs > 0** (2: 184 · 3: 101
  · 4: 23 · 5-12: 23), 30.661 = 0, 329 NULL. Caso 473007: A1 = 30, A9 = 1, A10 = 3.

### A7. `num_pasos_verificacion` — pasos del análisis del comité
- **Mide**: cantidad de pasos numerados ("1.", "1º", "1°") del análisis de prohibiciones del comité.
- **Regla**: máximo índice de la lista; numeración con punto o con ordinal. Hoy lo resuelve el regex
  (200/200 GT; 30/30 pseudo). **NULL** si el texto remite a "metodología que disponga la DNCP" sin lista.
- **Nota**: solo toma valores 6/7/8 según la versión de la ley; es un marcador de versión de plantilla,
  no una magnitud. **Decisión**: [ ] mantener en regex, fuera del benchmark LLM.

---

## Grupo B — Casi constantes: la señal es el desvío del estándar ✅ APROBADO (08-oct-2026)

### Evidencia B1/B2 (`scripts/analisis_seccion_financiera.py`, 16.181 textos distintos / 32.176 secciones)
- "No aplica" 4.104 docs. Plantilla DNCP: "Ratio de Liquidez: activo corriente / pasivo corriente. Deberá
  ser igual o mayor que 1, en promedio… Endeudamiento: pasivo total / activo total. No deberá ser mayor a
  0,80…". Rentabilidad ("no negativa") y eficiencia (≥ 1) son constantes: fuera. Patrimonio neto (1.630
  docs) y línea de crédito (1.109) son montos: fuera por ahora.
- Formatos: "[ 0 , 5 0 ]" → 0.5 · "1, 25" → 1.25 · "= ó 1,00" → 1 · "[1,10]" → 1.1 · coma decimal → punto.

### B1. `ratio_liquidez_minimo` ✅
- **Mide**: activo corriente / pasivo corriente mínimo exigido. Estándar 1,0 (23.127 docs).
- **No estándar: 669 docs (2,8%)**: 1,15 (349, plantilla de consorcios "Socio Líder") · 2,0 · 0,8 · 1,2 ·
  1,25 · 1,1 · 0,9 · 0,5 · 0,7. → valor + flag **`liquidez_no_estandar`** (≠ 1); precisión medida solo en
  ese estrato (decisión 08-oct).
- **NULL**: "No aplica"; "[indicar índice]" o placeholder sin número.
- **Guía del prompt**:
  ```
  Valor mínimo exigido del Ratio/Coeficiente de LIQUIDEZ (activo corriente / pasivo corriente).
  Ej: "Deberá ser igual o mayor que 1, en promedio" -> 1. "igual o mayor a 1,15" -> 1.15.
  "[ 0 , 5 0 ]" -> 0.5. "1, 25" -> 1.25. Usa punto decimal.
  Responde NULL si dice "No aplica" o el número no está completado ("[indicar]").
  NO es este valor: endeudamiento, solvencia, rentabilidad, eficiencia (ingreso/egreso), años.
  ```

### B2. `ratio_endeudamiento_maximo` ✅
- **Mide**: pasivo total / activo total máximo permitido. Estándar 0,80 (22.339 docs).
- **No estándar: 948 docs (4%)**: 1,0 (221) · 0,9 · 0,6 · 0,7 · 0,5… → valor + flag
  **`endeudamiento_no_estandar`** (≠ 0,80) (decisión 08-oct).
- **Sinónimo**: "Coeficiente de Solvencia: Pasivo Total / Activo Total igual o menor a 0,70" → 0.7.
- **Ratio invertido** "Coeficiente de Solvencia: **Activo Total / Pasivo Total** igual o mayor a 1,20" →
  convertir **1/x redondeado a 2 decimales** (→ 0.83) (decisión 08-oct).
- **NULL**: "No aplica"; placeholder; **margen de solvencia de aseguradoras** ("Margen de solvencia: igual
  o superior a 1,50 al 30/06/22", indicador regulatorio de seguros; parte de los 294 docs con valor > 1)
  (decisión 08-oct).
- **Guía del prompt**:
  ```
  Valor máximo permitido del ENDEUDAMIENTO (pasivo total / activo total).
  Ej: "No deberá ser mayor a 0,80 en promedio" -> 0.8. "Coeficiente de Solvencia: Pasivo Total /
  Activo Total igual o menor a 0.70" -> 0.7.
  Si el cociente está invertido (Activo Total / Pasivo Total igual o mayor a X), responde 1/X con
  2 decimales: 1,20 -> 0.83.
  Responde NULL si dice "No aplica", si no hay número, o si es el "margen de solvencia" de una
  aseguradora.
  NO es este valor: liquidez, rentabilidad, eficiencia, patrimonio, calificación de riesgo.
  ```

### B3. `num_componentes_formula` → REEMPLAZADO por `tipo_indice_reajuste` (regex) ✅ (decisión 08-oct)
- **Evidencia** (`scripts/analisis_seccion_reajuste.py`, 8.995 textos / 32.180 secciones): lo informativo
  es el **índice** al que se ata el reajuste, no cuántas variables tiene la fórmula. IPC (Pr = P ×
  IPC1/IPC0, también en prosa) ~50% · polinómica de obras (coeficientes Ak, salario, cemento, gasoil)
  12% · índice de la construcción (A = P × IIBCP) 11% · prosa sin ecuación 7,5% · tipo de cambio del
  dólar 4,6% · salario mínimo + IPC 3,7% · combustible 2,7%. Umbral de activación: 15% en el 94% de los
  docs con umbral (14.953); 10%, 5%, 20% en el resto.
- **Por qué se reemplaza**: era el único campo 100% LLM; contar símbolos con OCR roto ("Pr= P x IPC1 IPC0")
  es donde los modelos fallan; sustento BAJO; el conteo era un sustituto pobre del tipo de índice.
- **Nuevo campo `tipo_indice_reajuste`** (categórico, regex de palabras clave, sin LLM):
  `ipc` · `iibcp` · `polinomica` (coeficientes/insumos de obra) · `salario` (salario mínimo/jornal, con o
  sin IPC) · `combustible` · `tipo_cambio` · `prosa_sin_formula` · `no_sujeto`.
  Prioridad si hay varios: `polinomica` > `tipo_cambio` > `combustible` > `salario` > `iibcp` > `ipc`
  (el más específico gana; IPC aparece como mención genérica en casi todos).
- **Flag `umbral_reajuste_no_estandar`**: umbral de activación ≠ 15%.
- **Lectura de riesgo**: reajuste atado al dólar o a una polinómica a medida es más manipulable que el IPC
  estándar. `num_componentes_formula` deja de ser variable de Pipeline 3 y sale del benchmark LLM; su
  run en staging no se promueve.

## Grupo C — Plantilla pura: binarios por regex, fuera del benchmark ✅ APROBADO (08-oct-2026)

Medido en BD (31.321 docs en v3). Ningún campo de este grupo usa LLM ni anotación humana.

### C1. `dias_notificacion_fuerza_mayor` → binario `tiene_clausula_fuerza_mayor` ✅
- Cláusula "7 días" en el 99,6% de los 29.831 docs con sección; 1.490 docs sin la sección. La señal útil
  es la **ausencia de la sección** (información faltante, Ferwerda r=0,431), no el número.

### C2. `dias_aceptacion_rechazo_factura` → binario `tiene_clausula_aceptacion_factura` ✅
- "a más tardar en quince (15) días" en el 92,2% de los 30.992 docs con sección (el resto: variantes de
  redacción del mismo 15). Sale del benchmark.

### C3. `num_documentos_pago` → binario `exige_certificado_laboral` ✅
- 6 documentos en 26.995 docs, 7 en 3.995: el séptimo es el **certificado laboral** (12,7%). Regex sobre
  la sección de pago. La guía vieja ("ítems de primer nivel, no sub-ítems") queda descartada.

### A7. `num_pasos_verificacion` → variable de CONTROL de versión de plantilla ✅
- 6/7/8 sigue al año (6: 2021-2025, 7: 2024-2025, 8: solo 2025): marca la versión de la plantilla DNCP.
  Se mantiene en regex, fuera del benchmark; en Pipeline 3 **solo como control**, nunca como predictor
  (si no, el detector marcaría 2025 como anómalo).

### `dias_presentacion_garantia` → DESCARTADO ✅
- 10 días en el 100% de los docs. Sin información.

---

## Mapa final (08-oct-2026)

| Método | Campos |
|---|---|
| Regla → LLM en residuales (único lugar del benchmark) | A1 `dias_pago_maximo` · A2 `dias_validez_garantia` · A3 `dias_pago_anticipo` · A4 `dias_solicitud_anticipo` · A5 `anos_experiencia_minima_tecnica` · A6 `pct_monto_referencia_tecnica` |
| Regla/regex sin LLM | B1 `ratio_liquidez_minimo` · B2 `ratio_endeudamiento_maximo` |
| Regex nuevo | `tipo_indice_reajuste` · `tiene_clausula_fuerza_mayor` · `tiene_clausula_aceptacion_factura` · `exige_certificado_laboral` · A8 `pct_respaldo_financiero_monto` · A9 `pago_plurianual` · A10 `num_tramos_pago` |
| Flags nuevos | `pago_en_dias_habiles` · `pago_sujeto_plan_caja` · `garantia_varia_por_item` · `anticipo_sujeto_disponibilidad` · `liquidez_no_estandar` · `endeudamiento_no_estandar` · `umbral_reajuste_no_estandar` |
| Control (no predictor) | `num_pasos_verificacion` |
| Fusión para Pipeline 3 | `anos_experiencia_exigidos` (máx. A5 + `anos_experiencia_minima`) · `pct_monto_referencia` (`pct_monto_referencia_minimo`, si falta A6) |
| Descartados | `num_componentes_formula` (reemplazado) · `dias_presentacion_garantia` |

## Plan de implementación (tras la aprobación del 08-oct-2026)

| Paso | Qué | Quién | Depende de |
|---|---|---|---|
| I1 ✅ | Reescribir `extraction_hint` de A1-A6, B1, B2 desde las fichas; versión nueva del prompt (cambia el fingerprint); `llm_fallback=false` en 7 campos | Opus | — |
| I2 ✅ | Ajustar reglas deterministas a las trampas medidas (A1: FIS/cuotas/recibo/aceptación; A2: máximo + días literales + servicios; A3/A4: excluir el texto legal fijo; A5: antigüedad en el rubro + ventanas "últimos N años"; B2: ratio invertido y margen de solvencia). Un test por cada ejemplo y trampa de las fichas | Opus | — |
| I3 ✅ | Regex nuevos (`tipo_indice_reajuste`, 3 binarios C) + 7 flags; columnas en v3 (migración SQL idempotente) y recálculo | Opus | — |
| I4 ✅ | Re-alinear referencias con las fichas: script que marca ítems del GT de agosto cuyo valor contradice una decisión nueva (validez por ítem, servicios, antigüedad, ventanas…) → propuesta de corrección → **aprobación del usuario**; re-anotar el pseudo-GT Opus con las fichas | Opus + usuario | I1 |
| I5 🟡 | Calibración v2: 60 ítems (A1-A6 × 10, estrato no-modal sobremuestreado), usuario y Opus por separado → κ por campo; κ < 0,8 → se corrige la ficha | usuario + Opus | I4 |
| I6 | Benchmark reducido: Gemma-4-12B QAT (vLLM), Qwen3-14B-AWQ, Gemma-4-26B-A4B × A1-A6, crudo y pipeline, contra GT re-alineado y pseudo-GT; métrica en 3 columnas (total, estrato no-modal, ausencias) | Opus (~40 min GPU) | I1, I2, I4 |
| I7 | GT fuera de muestra (~240 ítems, A1-A6) → precisión por campo y estrato → promote por campo a v3 | usuario → Opus | I5, I6 |
| I8 | Pipeline 3 fase 1 con variables sin LLM (económicas, estructurales, P1, B1/B2, regex nuevos, flags) | Opus | I3 (en paralelo a I4-I7) |

### Resultado I1-I3 (08-oct-2026)
- Código: `src/etl/transformers/field_rules_v1.py`; `RULES` vigente = spec-v1; `RULES_S24` congelado.
- Tests: `tests/test_field_rules_v1.py` (un caso por ejemplo/trampa de las fichas) + suite histórica intacta.
- Acierto de las reglas solas (sin LLM):

| Campo | GT agosto v1 / s24 (200) | pseudo-GT Opus v1 / s24 | casos al LLM (corpus) |
|---|---|---|---|
| dias_pago_maximo | 185 / 189 | 26 / 21 (30) | 0 |
| dias_validez_garantia | 163 / 200 | 28 / 22 (28) | 19 |
| dias_pago_anticipo | 199 / 200 | 28 / 27 (30) | 162 |
| dias_solicitud_anticipo | 197 / 200 | 28 / 25 (30) | 57 |
| anos_experiencia_minima_tecnica | 172 / 199 | 29 / 29 (30) | 249 |
| pct_monto_referencia_tecnica | 181 / 151 | 30 / 30 (30) | 28 |
| ratio_liquidez_minimo | 200 / 200 | 30 / 30 (30) | 0 |
| ratio_endeudamiento_maximo | 199 / 199 | 30 / 30 (30) | 0 |

  Las reglas s24 se ajustaron sobre el GT de agosto (200/200 in-sample); las diferencias v1 vs GT son, en
  su gran mayoría, decisiones de las fichas que el GT no sigue (validez > 365 días o por lote → GT NULL;
  antigüedad en el rubro → GT NULL; plazo de obra "Plazo de pago: N días desde la aprobación del
  certificado" → GT 60; recibo/retención tomados como plazo de pago). Insumo directo para I4.
- Pregunta abierta para I4: "solvencia económica del N% del monto máximo" (capacidad técnica, 8 ítems GT)
  no es % de referencias según la ficha A6; hoy NULL. ¿Variable nueva de capacidad financiera?
- `scripts/build_features_spec_v1.py --apply`: 11 columnas nuevas en v3 (31.321 filas) y
  `dncp.spec_v1_rule_values` (232.604 filas; residual LLM ≈ 515 campo-documento).

### Resultado I4 (aplicado, 08-oct-2026)
- `scripts/generar_i4_realineacion.py` → `planillas/i4_realineacion.csv` + `docs/i4_realineacion_gt.md`:
  85 diferencias (80 GT agosto + 5 pseudo-GT Opus), 83 explicadas por una decisión de ficha, 2 a revisar.
  Nada modificado: espera la aprobación del usuario.
- **Aprobación del usuario: "apruebo todo"** (85/85; 463117 endeudamiento → 0,8 leído en el pliego).
  `scripts/aplicar_i4_gt.py --todo-aprobado` escribe copias versionadas: `data/processed/alolab/gt_por_campo_v1/`
  (`ground_truth` corregido + `ground_truth_agosto` + `realineacion_i4`) y `pseudo_gt_opus/anotaciones_v1.json`;
  bitácora `planillas/i4_aplicado.csv`. Los originales no se tocan (reproducen lo publicado).
- Reglas v1 vs GT v1: 194/194 a 200/200 por campo (residual LLM 0-7) y 28/28-30/30 en pseudo-GT. **Circular**:
  el GT se alineó a las fichas que implementan las reglas; la validación real es I5 (κ) e I7 (fuera de muestra).
- La revisión de I4 destapó 9 errores de las reglas v1 (corregidos con test): trampa "mantenimiento" y
  "repuestos" demasiado amplias, ancla de solicitud vacía sin fallback, "Soporte"/"indisponibilidad" en
  garantía, trampa de anticipo a nivel oración, "solicitará", ancla "se otorgará", "presentación de la
  factura" como solicitud.

### I5 en curso (08-oct-2026)
- `scripts/generar_calibracion_v2.py` → `planillas/calibracion_v2.csv` (ciega) + `_clave.csv`: 60 ítems A1-A6 × 10
  de docs fuera del GT, del pseudo-GT y de la calibración v1; estratos por el valor de la regla spec-v1
  (no modal 5 · modal 2 · ausente 2 · residual 1; A1 no tiene ausentes ni residuales → 8 no modales).
- Anotación Opus independiente (subagente que solo lee la planilla ciega y las fichas, sin código de reglas ni
  clave): `planillas/calibracion_v2_opus.csv`. Usuario: `I5_CALIBRACION_planilla.xlsx`.
- `scripts/evaluar_calibracion_v2.py` → κ usuario-Opus por campo y acuerdo de cada uno con la regla.
