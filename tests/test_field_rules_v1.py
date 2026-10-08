"""Tests de las reglas de la especificación v1.0 (docs/especificacion_campos.md).

Cada caso es un ejemplo o una trampa de las fichas (textos reales de la BD, recortados).
Correr: /root/vllm-env/bin/python -m pytest tests/test_field_rules_v1.py -q
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.etl.transformers import field_rules_v1 as fr  # noqa: E402

NR = fr.NO_RULE
STD = ("Otras formas y condiciones de pago al proveedor en virtud del contrato serán las siguientes: {own} "
       "2. La Contratante efectuará los pagos, dentro del plazo establecido en este apartado, sin exceder "
       "{std} días después de la presentación de una factura por el proveedor, y después de que la "
       "contratante la haya aceptado. Dicha aceptación o rechazo, deberá darse a más tardar en quince (15) "
       "días posteriores a su presentación. 3. De conformidad al Decreto N° 7781/2006, el proveedor deberá "
       "habilitar su cuenta.")


def pago(own, std="sesenta (60)"):
    return STD.format(own=own, std=std)


# ------------------------------------------------------------------ helpers
@pytest.mark.parametrize("s,v", [
    ("treinta y seis", 36), ("ciento veinte", 120), ("trescientos sesenta y cinco", 365), ("doce", 12)])
def test_words_to_int(s, v):
    assert fr.words_to_int(s) == v


@pytest.mark.parametrize("s,v", [
    ("sesenta (30) días", 30), ("30 (treinta) días", 30), ("cinco [05] días", 5), ("(diez) días", 10),
    ("15 días", 15), ("treinta días", 30), ("(12 doce) meses", 12)])
def test_num_before(s, v):
    unit = s.rfind(" ") + 1
    assert fr.num_before(s, unit)[0] == v


# ------------------------------------------------------------------ A1
@pytest.mark.parametrize("own,expected", [
    ("Mensual. Dentro de los 30 días (de la entrega de la solicitud y factura respectiva).", 30.0),
    ("La Contratante abonará al Proveedor en Guaraníes, en un plazo de 30 días contados a partir de la "
     "presentación de la factura correspondiente.", 30.0),
    ("PAGO MENSUAL, DENTRO DE LOS 10 DIAS POSTERIORES A LA PRESENTACION DE LA FACTURA", 10.0),
    ("Plazo de pago: 120 (ciento veinte ) días, desde la aprobación del certificado de obra y la "
     "presentación de la factura.", 120.0),
    ("El pago se realizará dentro de los 5 (cinco) días hábiles, contados a partir de la recepción de la "
     "solicitud de pago.", 5.0),
    ("El pago se realizará en forma mensual 30 días posteriores a la entrega de los bienes solicitados.", 30.0),
    # trampas
    ("A PLAZOS.- Los pagos se harán efectivos cada 30 días, dentro de los 60 días calendarios contados a "
     "partir de la presentación de las facturas respectivas.", 60.0),
    ("Los pagos se efectuaran dentro de los sesenta (60) días posteriores a la presentación de la factura. "
     "Ademas el proveedor deberá, dentro de los cinco (5) días hábiles siguientes a la acreditación de pago, "
     "presentar el recibo de dinero.", 60.0),
    ("Deberán presentar para el pago, una solicitud a la Convocante, dentro del plazo de 30 (treinta) días "
     "corridos desde la entrega de los bienes ofertados. El pago se realizará a los 20 (veinte) días "
     "posteriores de haber recibido la factura aprobada.", 20.0),
    ("Segunda solicitud de Pago : 50% del monto total, luego de 90 (noventa) días calendario de haberse "
     "presentado la primera solicitud de pago. Todos los pagos serán efectuados dentro de los 60 (sesenta) "
     "días posteriores a la presentación de la solicitud de pago.", 60.0),
    ("LAS FACTURAS DEBERAN SER ENTREGADOS LOS PRIMEROS 10 DIAS DE CADA MES.", 60.0),
    ("Formulario de Informe de Servicios Personales (FIS) deberán ser completados a través del módulo "
     "informático, dentro de los treinta (30) días posteriores a la obtención del código de contratación, y "
     "con anterioridad al primer pago.", 60.0),
    ("La Contratante se expedirá sobre las facturas en un plazo no mayor de diez (10) días calendarios de la "
     "recepción.", 60.0),
    ("Se realizará el segundo pago del 25% del valor total del contrato, luego de los 60 (sesenta) dias del "
     "primer pago conforme a la Disponibilidad Financiera de la Contratante. El pago se realizará dentro de los "
     "30 días de la recepción de la factura correspondiente.", 30.0),
    ("", 60.0),
])
def test_dias_pago_maximo(own, expected):
    assert fr.dias_pago_maximo(pago(own)) == expected


def test_dias_pago_maximo_std_modificado_y_parentesis():
    assert fr.dias_pago_maximo(pago("", std="treinta (30)")) == 30.0
    assert fr.dias_pago_maximo(pago("", std="sesenta (30)")) == 30.0


def test_dias_pago_maximo_sin_dias_va_al_llm():
    assert fr.dias_pago_maximo("Pago contra entrega de los bienes.") is NR


def test_pago_flags():
    f = fr.section_features("formas_y_condiciones_de_pago",
                            pago("El pago se realizará dentro de los 5 (cinco) días hábiles de la solicitud, "
                                 "sujeto a disponibilidad del Plan de Caja. Certificado laboral vigente."))
    assert f == {"pago_en_dias_habiles": 1, "pago_sujeto_plan_caja": 1,
                 "tiene_clausula_aceptacion_factura": 1, "exige_certificado_laboral": 1}
    f = fr.section_features("formas_y_condiciones_de_pago", pago(""))
    assert f["pago_en_dias_habiles"] == 0 and f["pago_sujeto_plan_caja"] == 0 and f["exige_certificado_laboral"] == 0


# ------------------------------------------------------------------ A2
GV = "Periodo de validez de la Garantía de los bienes El plazo de validez de la Garantía de los bienes será el siguiente: {}"


@pytest.mark.parametrize("seg,expected", [
    ("12 MESES", 365.0), ("1 año", 365.0), ("2 AÑOS COMO MINIMO.", 730.0),
    ("El periodo de validez de la garantía es de 12 meses como mínimo a partir de su provisión.", 365.0),
    ("MOTOR: Garantía de 12 (doce) meses o 50.000 Km.", 365.0),
    ("12 meses para el lote 1. 36 meses para el lote 2.", 1095.0),
    ("3 (tres) años para el item N° 1; y 1 (un) año para los items N° 2 y 3.", 1095.0),
    ("SESENTA DIAS POSTERIORES A LA RECEPCION DEL BIEN POR LA CONVOCANTE.", 60.0),
    ("180 DIAS CONTADOS DESDE LA RECEPCION DEFINITIVA DEL BIEN.", 180.0),
    ("CERTIFICADO DE GARANTÍA ESCRITA POR 6 MESES", 182.0),
    ("6 (seis) meses (180 días) desde la entrega", 180.0),
    ("12 (doce) Meses Incluye: Soporte de atención de Hardware. Si la reparación implica la indisponibilidad del "
     "equipo por más de 5 días, el proveedor deberá comunicar.", 365.0),
    ("Garantía con soporte las 24 horas al día, los 7 días de la semana por 36 meses.", 1095.0),
    ("La garantía de los servicios será de 30 (treinta) días corridos posteriores al servicio.", 30.0),
    ("Desde la suscripción del contrato y por lo menos 30 días posteriores al plazo de vigencia del contrato.", 30.0),
    ("No Aplica", None), ("Según las especificaciones técnicas de cada ítem.", None),
    ("Garantía de buen servicio por el periodo de duración del contrato.", None),
    # trampas
    ("El vencimiento de los medicamentos debe ser de (12 doce) meses, contados a partir de la fecha de entrega.", None),
    ("Los bienes deben ser nuevos con una antigüedad de fabricación no mayor a 1 (un) año. Garantía de 2 años.", 730.0),
    ("Garantía escrita mínima de 5 (cinco) años contra Defecto de fabricación.", 1825.0),
    ("El vencimiento de los productos deber ser igual o superior a 18 (dieciocho) meses. Canje en 60 días.", None),
    ("La Garantía de cumplimiento de Contrato deberá ser presentada dentro de los 10 días siguientes. El plazo de "
     "vigencia de la garantía deberá ser por lo menos treinta días posteriores al plazo de entrega.", None),
    ("ITEM 1: garantía de 1.095 (un mil noventa y cinco) días corridos como mínimo. ITEM 2: 365 días.", 1095.0),
    ("1.800 días corridos desde la recepción del bien.", 1800.0),
    ('90 "días posterior a la recepción final de trabajos.', 90.0),
    ("Ver especificaciones técnicas; si no se especifica, será de 1 (año) contado a partir de la recepción", 365.0),
    ("Lote N° 1 Garantía del fabricante por el plazo de 5 años con atención de 7 días semanales por el termino de "
     "24 horas (7x24). Lote N° 2 Un año de Fábrica.", 1825.0),
])
def test_dias_validez_garantia(seg, expected):
    assert fr.dias_validez_garantia(GV.format(seg)) == expected


def test_garantia_varia_por_item():
    f = fr.section_features("periodo_de_validez_de_la_garantia_de_los_bienes",
                            GV.format("12 meses para el lote 1. 36 meses para el lote 2."))
    assert f == {"garantia_varia_por_item": 1}
    f = fr.section_features("periodo_de_validez_de_la_garantia_de_los_bienes", GV.format("12 MESES"))
    assert f == {"garantia_varia_por_item": 0}


# ------------------------------------------------------------------ A3/A4
ANT = ("Solicitud de Pago de Anticipo El plazo dentro del cual se solicitará el anticipo será (en días "
       "corridos) de: {} 1. El anticipo es la suma de dinero que se entrega al proveedor. 5. El Pago del "
       "Anticipo debe ser total. En el caso que se realizare el pago de un porcentaje inferior al 100%, el "
       "proveedor podrá rechazarlo en el plazo de cinco (5) días hábiles. 9. La Garantía de Anticipo será "
       "devuelta al proveedor a más tardar treinta (30) días contados a partir del cumplimiento.")


@pytest.mark.parametrize("seg,sol,pag", [
    ("No Aplica", None, None),
    ("a) Plazo máximo para la presentación de la solicitud de pago de anticipo es de 3 días hábiles "
     "posteriores a la firma del contrato: b) Dirección: Comando c) Horario de atención: de 07:00 a 11:00 "
     "g) Plazo o fecha en la cual se abonará al contratista el monto del anticipo, siguiente a la fecha de "
     "la presentación de la solicitud: 10 (diez) días hábiles siguientes a la fecha de presentación.", 3.0, 10.0),
    ("1. Plazo máximo para la presentación de la solicitud de pago de anticipo: 5 (cinco) días calendarios "
     "posteriores a la firma del contrato 2. Dirección: Luque 6. Plazo o fecha en la cual se abonará al "
     "contratista el monto del anticipo, siguiente a la fecha de la presentación de la solicitud: Se "
     "verificarán las documentaciones requeridas y previa aprobación del Plan de Inversión, se abonará al "
     "Contratista el monto total del anticipo, a más tardar dentro de los 10 días calendarios siguientes.",
     5.0, 10.0),
    ("Plazo o fecha en la cual se abonará al contratista el monto del anticipo, siguiente a la fecha de la "
     "presentación de la solicitud: Según el proceso de verificación, aprobación de solicitud y transferencia "
     "del Ministerio de Hacienda.", None, None),
    ("Anticipo solo para MIPYME: La Contratante abonará al Proveedor el cinco por ciento (5%) del monto mínimo "
     "del Contrato. Dentro de los diez (10) días calendario posterior a la firma del Contrato, el Proveedor "
     "deberá presentar la solicitud de cobro de anticipo acompañada de la garantía.", 10.0, None),
    ("Plazo máximo para la presentación de la solicitud de pago de anticipo: cinco [05] días calendarios "
     "posteriores a la firma del contrato.", 5.0, None),
    ("Plazo máximo para la presentación de la solicitud de pago de anticipo: 10 (diez) corridos posteriores "
     "a la firma del Contrato b. Dirección: Tacuary", 10.0, None),
    ("Plazo o fecha en la cual se abonará al contratista el monto del anticipo, siguiente a la fecha de la "
     "presentación de la solicitud: DENTRO DE LOS 10 (DIEZ) D Í A S. - Forma de amortización: descontado.",
     None, 10.0),
    ("Plazo o fecha en la cual se abonará al contratista el monto del anticipo, siguiente a la fecha de la "
     "presentación de la solicitud: 10 DIAS HÁBILES LUEGO DE LA FIRMA DEL CONTRATO f. Forma de amortización",
     None, 10.0),
    ("20 (veinte) días corridos.", 20.0, None),
    ("El anticipo será del 10%. El proveedor solicitará dentro de los 8 días contados desde la firma del contrato.",
     8.0, None),
    ("La solicitud de pago del anticipo deberá ser presentada por escrito, dentro de los 5(cinco) días corridos de "
     "haber sido adjudicado, en Mesa de Entrada de lunes a viernes de 7:00 a 15:00 horas.", 5.0, None),
    ("a) Plazo máximo para la presentación de la solicitud de otorgamiento de anticipo: b) Dirección: X f) Plazo o "
     "fecha en la cual se otorgará al contratista el monto del anticipo, siguiente a la fecha de la presentación de "
     "la solicitud: Dentro de los 30 (treinta) días", None, 30.0),
    ("a) Plazo máximo para la presentación de la solicitud de pago de anticipo: Haga clic o pulse aquí. Se otorgará "
     "el 30% de Anticipo, el mismo podrá ser solicitado hasta 10 días corridos posteriores a la emisión del Acto "
     "Administrativo.", 10.0, None),
    ("dentro de los 30 (treinta) días corridos", 30.0, None),
    ("Anticipo : 20 % Plazo máximo para la presentación de la solicitud de pago de anticipo: 1 0 ( D I E Z ) "
     "D Í A S C O R R I D O S P O S T E R I O R E S A L A F I R M A D E L C O N T R A T O. -Dirección: X",
     10.0, None),
])
def test_anticipo(seg, sol, pag):
    t = ANT.format(seg)
    assert fr.dias_solicitud_anticipo(t) == sol
    assert fr.dias_pago_anticipo(t) == pag


def test_anticipo_flag_disponibilidad():
    t = ANT.format("Plazo o fecha en la cual se abonará al contratista el monto del anticipo, siguiente a la "
                   "fecha de la presentación de la solicitud: Según el proceso de verificación y transferencia "
                   "del Ministerio de Economía.")
    assert fr.section_features("solicitud_de_pago_de_anticipo", t) == {"anticipo_sujeto_disponibilidad": 1}
    assert fr.section_features("solicitud_de_pago_de_anticipo", ANT.format("No Aplica")) == {
        "anticipo_sujeto_disponibilidad": None}


# ------------------------------------------------------------------ A5
CT = ("Capacidad Técnica El oferente deberá proporcionar evidencia documentada que demuestre su cumplimiento "
      "con los siguientes requisitos de capacidad técnica: {}")


@pytest.mark.parametrize("seg,expected", [
    ("El rubro principal del oferente deberá ser el de CONFECCIÓN, con una antigüedad mínima de tres (03) años "
     "dedicados en el rubro.", 3.0),
    ("1. Existencia legal de por lo menos tres (3) años de antigüedad.", 3.0),
    ("Se requiere un mínimo de 2 años de experiencia demostrable en dicho rubro operando en nuestro país.", 2.0),
    ("1 (un) Ingeniero Electromecánico que posea como mínimo 3 (tres) años de experiencia como supervisor "
     "técnico.", 3.0),
    ("La empresa deberá tener 5 años de experiencia en el rubro. El ingeniero residente deberá contar con 10 "
     "años de experiencia.", 5.0),
    ("Un Arquitecto con al menos 3 (tres) años de ejercicio de la profesión. Un técnico con 5 años de "
     "experiencia.", 5.0),
    ("Listado de clientes, un mínimo de 3 (tres) instituciones públicas, en los últimos 3 (tres) años.", None),
    ("Garantizar por 4 (cuatro) años la provisión de repuestos originales.", None),
    ("No Aplica", None),
    ("Arquitecto residente con experiencia mínima de 5 (cinco) años. 1 (un) carpintero y 1 (un) albañil; los "
     "mismos deberán contar con experiencia mínima de 2 (dos) años.", 5.0),
    ("Experiencia: 1 años 14 puntos 2 años 15 puntos 5 años o más 18 puntos. Todo oferente con una experiencia "
     "menor a un año será descalificado.", 1.0),
    ("Título universitario (la carrera no puede ser menor a 4 años) con 5 años de antigüedad contados desde la "
     "fecha de expedición del título.", 5.0),
    ("El fabricante del Motor deberá tener un mínimo de 20 años de experiencia en el diseño y fabricación de "
     "Motores.", 20.0),
    ("Estar especializado en Medicina Nuclear. Experiencia de por lo menos 1 (uno) año como Especialista en "
     "Medicina Nuclear demostrado con Certificados o Constancias.", 1.0),
    ("Gerente de Proyecto 4 años de experiencia previa en administra grupos de Personal Exigido Arquitecto de "
     "Software Graduado o con malla curricular concluida de la carrera de Licenciatura o Ingeniería en "
     "Informática, de 7 años de experiencia demostrable con el título Universitario. Encargado de "
     "Implementación Graduado de la carrera de Ingeniería Electrónica con mínimo de 6 años de experiencia.", 7.0),
])
def test_anos_experiencia(seg, expected):
    assert fr.anos_experiencia_minima_tecnica(CT.format(seg)) == expected


# ------------------------------------------------------------------ A6
@pytest.mark.parametrize("seg,expected", [
    ("Demostrar la experiencia con facturaciones de venta por un monto equivalente al 50% del monto máximo "
     "de lote ofertado, de los: 5 últimos años.", 50.0),
    ("La sumatoria de los montos de las referencias presentadas deberá ser como mínimo el 40% del monto "
     "referencial de la adquisición. ii. Al menos una referencia de trabajo similar por el 20% del monto "
     "referencial.", 40.0),
    ("la empresa líder del consorcio deberá cumplir con al menos 60% de los criterios de calificación.", None),
    ("El oferente deberá demostrar que cuenta con la capacidad para entregar hasta el 30 % de los productos "
     "adjudicados a los 5 días.", None),
    ("El precio ofertado no deberá superar el 65% del precio fijado en el Acta de Fijación.", None),
    ("Contar con los registros sanitarios habilitantes.", None),
    ("Acreditar solidez y experiencia comercial en la venta de insumos, equipos y/o repuestos, por un volumen "
     "mínimo de 30% del monto total de la oferta, en los cinco (05) últimos años.", 30.0),
    ("DEMOSTRAR STOCK CONTABLE DE REPUESTOS PARA ATENDER EL EQUIPO OFERTADO DE POR LO MENOS 10% DEL MONTO DE LA "
     "OFERTA.", None),
    ("En caso de Consorcio, el líder deberá cumplir con el 60% de los criterios de calificación, y el 40% restante "
     "lo cumplirá los demás integrantes del consorcio, con facturaciones de venta y/o recepciones finales por un "
     "monto equivalente al 50 % como mínimo del monto total ofertado en la presente licitación.", 50.0),
    ("El oferente deberá demostrar poseer una solvencia económica del 30% como mínimo del monto máximo "
     "establecido para el presente llamado.", None),
    ("Los reaseguradores ofrecidos deben totalizar el 100% (cien por ciento) de cobertura en los montos "
     "asegurados.", None),
    ("El monto mínimo de la póliza será por un total de 10% del contrato en guaraníes.", None),
])
def test_pct_monto_referencia(seg, expected):
    assert fr.pct_monto_referencia_tecnica(CT.format(seg)) == expected


# ------------------------------------------------------------------ B1/B2
FIN = "Capacidad Financiera Con el objetivo de calificar la situación financiera del oferente, se considerarán los siguientes índices: {}"


@pytest.mark.parametrize("seg,liq,end", [
    ("a. Ratio de Liquidez: activo corriente / pasivo corriente Deberá ser igual o mayor que 1, en promedio, en "
     "los 3 últimos años b. Endeudamiento: pasivo total / activo total No deberá ser mayor a 0,80 en promedio",
     1.0, 0.8),
    ("Coeficiente de Liquidez: Activo corriente / Pasivo Corriente debe ser igual o mayor a 1,15. Coeficiente de "
     "Solvencia: Pasivo Total / Activo Total igual o menor a 0.70.", 1.15, 0.7),
    ("Coeficiente de Liquidez debe ser igual o mayor a [ 0 , 5 0 ] . Coeficiente de Solvencia: Activo Total / "
     "Pasivo Total igual o mayor a 1,20 (uno coma veinte).", 0.5, 0.83),
    ("Ratio de Liquidez: Deberá ser igual o mayor que 1, 25 en promedio. Endeudamiento: No deberá ser mayor a 1",
     1.25, 1.0),
    ("Ratio de Liquidez: activo corriente / pasivo corriente Deberá ser = ó 1,00 (igual o mayor a uno).", 1.0, None),
    ("Ratio de Liquidez: activo corriente / pasivo corriente Endeudamiento: pasivo total / activo total No "
     "deberá ser mayor a 0,80 en promedio", None, 0.8),
    ("1. Margen de solvencia: igual o superior a 1,50 al 30/06/22 2. Calificación de la aseguradora: BBB", None, None),
    ("Liquidez: Activo corriente / Pasivo Corriente debe ser igual o mayor a 01. Esta información", 1.0, None),
    ("Liquidez: Activo corriente/Pasivo Corriente debe ser igual o mayor a a 1 (uno) .", 1.0, None),
    ("Liquidez: Activo corriente / Pasivo Corriente debe ser igual o mayor a uno. Esta información", 1.0, None),
    ("Liquidez: debe ser igual o mayor a [indicar el índice].", None, None),
    ("NO APLICA", None, None),
])
def test_ratios(seg, liq, end):
    t = FIN.format(seg)
    assert fr.ratio_liquidez_minimo(t) == liq
    assert fr.ratio_endeudamiento_maximo(t) == end


def test_ratio_flags():
    t = FIN.format("Liquidez igual o mayor a 1,15. Endeudamiento: No deberá ser mayor a 0,80")
    assert fr.section_features("capacidad_financiera", t) == {"liquidez_no_estandar": 1,
                                                              "endeudamiento_no_estandar": 0}


# ------------------------------------------------------------------ B3 / C
REA = "Reajuste El precio del contrato estará sujeto a reajustes. La fórmula y el procedimiento para el reajuste serán los siguientes: {}"


@pytest.mark.parametrize("seg,tipo,umbral", [
    ("siempre y cuando la variación del IPC publicado por el BCP haya sufrido una variación igual o mayor al "
     "quince por ciento (15%). Pr= P x IPC1 IPC0", "ipc", 0),
    ("A = P x I.I.B.C.P. con variación igual o mayor al 10%", "iibcp", 1),
    ("Pr: Po [(0,60 x S1/So) + 0,40 x IPC1/IPC0] S1: Salario Mínimo actualizado", "salario", None),
    ("Rm = Pmo [Σ Ak (Ikm/Iko) - 1] Ak: Coeficientes de incidencia del insumo k", "polinomica", None),
    ("variación del Tipo de Cambio del dólar estadounidense igual o mayor al quince por ciento (15%)",
     "tipo_cambio", 0),
    ("Para Nafta PR = PB X FR según precio de Petropar", "combustible", None),
    ("El proveedor podrá solicitar el reajuste conforme a la ley.", "prosa_sin_formula", None),
    ("No aplica", "no_sujeto", None),
])
def test_reajuste(seg, tipo, umbral):
    assert fr.section_features("reajuste", REA.format(seg)) == {"tipo_indice_reajuste": tipo,
                                                                "umbral_reajuste_no_estandar": umbral}


def test_fuerza_mayor():
    assert fr.tiene_clausula_fuerza_mayor("deberá notificar dentro de los siete (7) días") == 1
    assert fr.tiene_clausula_fuerza_mayor("deberá notificar de inmediato") == 0


# ------------------------------------------------------------------ A8 respaldo financiero (variable nueva)
@pytest.mark.parametrize("text,expected", [
    ("Constancia de un Banco de plaza de otorgar una línea de crédito al oferente por un monto no menor al 20% del "
     "total ofertado.", 20.0),
    ("d. Capital Operativo (Activo Corriente - Pasivo Corriente): El oferente deberá poseer un capital positivo y no "
     "inferior al 30% del monto máximo del llamado.", 30.0),
    ("El oferente deberá demostrar su capacidad económica -poseer una solvencia económica del 30% como mínimo del "
     "monto máximo establecido para el presente llamado- por medio de una Constancia y/o extracto bancario.", 30.0),
    ("Constancia de línea de crédito aprobada por un valor igual o superior al 50% (cincuenta por ciento) del valor "
     "total de la oferta.", 50.0),
    ("La contratante conforme a su disponibilidad presupuestaria podrá otorgar Anticipo Financiero hasta del 20% del "
     "monto total del Contrato.", None),
    ("solvencia en la prestación de servicios de postventa, por un volumen mínimo de 50% del monto total de la "
     "oferta.", None),
    ("Ratio de Liquidez: Deberá ser igual o mayor que 1. Endeudamiento: No deberá ser mayor a 0,80.", None),
])
def test_respaldo_financiero(text, expected):
    assert fr.pct_respaldo_financiero_monto(text) == expected
