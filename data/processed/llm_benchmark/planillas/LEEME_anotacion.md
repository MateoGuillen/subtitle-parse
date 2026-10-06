# Portada — instrucciones de anotacion (igual que GT-100)
Guia de criterios: valor = NUMERO tal como lo define el campo (dias del plazo
PROPIO del campo; si la seccion lista varios plazos plausibles, marca ambiguo=s i
tu lectura no es la unica defendible). tu_es_null=s si la clausula existe pero la
plantilla esta sin completar, o no hay plazo explicito. NO te dejes influir por
conf_regex (esta oculto a proposito en la IAA).
Limitaciones pre-registradas (van a la tesis): kappa con n=25 tiene IC ancho ->
techo orientativo, no definitivo; estratificado n=75 con conf<0.8 es la validacion
de dominio que faltaba pero tampoco tiene poder para desempates finos.


## planilla_audito_A46_15.csv — INSTRUCCION DISTINTA (leer antes)
En esta planilla NO juzgas el pliego: juzgas la NOTA que el revisor de agosto escribio
(columna `nota_agosto`). Tu tarea es clasificar QUE MECANISMO describe esa razon, marcando
una sola columna de mecanismo con s/n y copiando en `evidencia_frag` el fragmento de texto
o de nota que la respalda:
  - mech_plantilla_vacia: la clausula existe pero estaba sin completar; el regex tomo un
    valor de ejemplo/default como si fuera real
  - mech_clausula_mal_anclada: hay un numero en el texto pero pertenece a otra clausula
    (otro plazo, otra garantia, otro item)
  - mech_sobre_abstencion_revisor: el valor del regex tiene base textual legitima y el
    revisor marco NULL indebidamente (aqui el regex podria estar BIEN)
  - mech_otra: ninguna de las anteriores (explicar en evidencia_frag)
Si estas en desacuerdo con la clasificacion de mecanismo que sugiere la nota, va en
`disenso_sugerido` (columna aparte, NO cuenta en la tipificacion).
TECHO DEL REPORTE (pre-registrado): n=15 es exploratorio-cualitativo; genera/descarta
hipotesis de mecanismo, NO estima proporciones extrapolables a los 160 casos caja-A.


## planilla_audito_a47_12.csv — auditoria de SUSTANCIA del 239/239 (acá sí juzgás el TEXTO)
Pregunta unica por fila: 'valor_en_texto_como_plazo_correcto' = si / no / otro:
SI el numero de la etiqueta humana aparece en el texto Y es el plazo del campo correcto
(no de otra clausula). Gate pre-registrado: GATE-pago (8 filas) necesita >=7 SI para que
el hallazgo pago-M quede FIRME; <=6 SI degrada pago-M a caja-A (A4.5 -> hipotesis).
Las 4 filas descriptivas no activan nada. 'otro_valor_posible_en_texto': si ves un plazo
alternativo legitimo para ESTE campo, escribelo (documenta ambiguedad).
Contexto circularidad: las etiquetas de agosto se generaron con hints emparentados con los
de hoy — esta auditoria existe justamente para verificar sustancia mas alla de eso.
