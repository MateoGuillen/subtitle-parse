import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from src.utils.logging_utils import setup_logger


class DocumentFeaturesV2Transformer:
    def __init__(self, schemas_path: str):
        with Path(schemas_path).open(encoding="utf-8") as f:
            self.schemas = json.load(f)
        self.title_schemas = {k: v for k, v in self.schemas.items() if k != "meta"}
        self.logger = setup_logger(__name__)

    def compute_document_features(self, df_sections: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Computing v2 document-level features via regex (with confidence)…")

        base = df_sections[["nro_licitacion", "year", "category_id"]].drop_duplicates("nro_licitacion").set_index("nro_licitacion")
        all_value_cols = []
        all_meta_cols = []

        title_groups = df_sections.groupby("title_normalized")
        for title_name, grp in title_groups:
            title_slug = title_name.replace(" ", "_")
            schema = self.title_schemas.get(title_slug)
            if not schema:
                self.logger.warning("No schema for title '%s' (slug=%s)", title_name, title_slug)
                continue

            text_per_doc = grp.groupby("nro_licitacion")["content_text"].apply(
                lambda texts: "\n".join(t for t in texts if pd.notna(t)))
            docs_with = set(text_per_doc.index)
            self.logger.info("  %s: %d docs with sections", title_slug, len(text_per_doc))

            for f in schema["fields"]:
                name = f["name"]
                ftype = f["type"]
                method = f.get("extraction_method", "hybrid")

                raw_values = text_per_doc.apply(
                    lambda t: self._extract_field_text(t, title_slug, name, ftype))

                val_series = pd.Series(index=base.index, dtype=object)
                val_series.update(raw_values)

                agg = self._aggregate_series(val_series, ftype)
                agg.name = name
                all_value_cols.append(agg)

                if method == "hybrid":
                    conf_series = pd.Series(1.0, index=base.index, dtype=np.float32)
                    conf_raw = raw_values.apply(
                        lambda v: self._compute_confidence(v, ftype, method, name)
                    ).astype(np.float32)  # pandas>=3: update exige mismo dtype
                    conf_series.update(conf_raw)
                    conf_series.name = f"{name}_conf"
                    all_meta_cols.append(conf_series)

                    src_series = pd.Series("absent", index=base.index, dtype=str)
                    src_raw = raw_values.apply(
                        lambda v: "absent" if v is None else "regex")
                    src_series.update(src_raw)
                    src_series.name = f"{name}_source"
                    all_meta_cols.append(src_series)

        df = pd.concat([base] + all_value_cols + all_meta_cols, axis=1).reset_index()
        df = df.fillna({c: 0 for c in df.columns if not c.endswith(("_conf", "_source"))})
        n_meta = sum(1 for c in df.columns if c.endswith("_conf"))
        self.logger.info(
            "Feature matrix v2: %d rows x %d columns (incl. %d _conf + %d _source).",
            len(df), len(df.columns), n_meta, n_meta,
        )
        return df

    def _compute_confidence(self, value, ftype: str, method: str, field_name: str) -> float:
        if method == "regex":
            return 1.0
        if ftype == "binary":
            return 1.0
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return 0.0
        if ftype == "float":
            if field_name.startswith("pct_") and value > 100.0:
                return 0.3
            if field_name.startswith("ratio_") and (value > 100.0 or value < 0.001):
                return 0.3
        elif ftype == "int":
            if field_name.startswith("dias_") and value > 730:
                return 0.3
            if field_name.startswith("num_") and value > 50:
                return 0.3
            if field_name.startswith("anos_") and value > 50:
                return 0.3
        return 1.0

    def _extract_field_text(self, text: str, title_slug: str, field_name: str, ftype: str):
        if not text:
            return self._default_value(ftype)

        if title_slug == "porcentaje_de_multas":
            if field_name == "pct_multa_diaria":
                return self._re_pct_pct_multas(text)
            if field_name == "binary_permite_rescision_por_multas":
                return self._re_binary(text, r"rescindir|supere.*garant")

        elif title_slug == "tasa_de_interes_por_mora":
            if field_name == "pct_interes_mora_diario":
                return self._re_pct_tasa_mora(text)
            if field_name == "binary_mora_desde_dia_siguiente":
                return self._re_binary(text, r"d[ií]a siguiente del vencimiento")

        elif title_slug == "porcentaje_de_garantia_de_fiel_cumplimiento_de_contrato":
            if field_name == "pct_garantia_cumplimiento":
                return self._re_pct_garantia_cumplimiento(text)
            if field_name == "dias_presentacion_garantia":
                return self._re_dias_presentacion_garantia(text)

        elif title_slug == "experiencia_requerida":
            if field_name == "anos_experiencia_minima":
                return self._re_anos_experiencia_minima(text)
            if field_name == "num_referencias_minimo":
                return self._re_int(text, r"(?:m[ií]nimo\s+(?:de\s+)?|al menos\s+)(\d{1,3})(?:\s*\([^)]+\))?\s*(?:referencias?|contratos?|factur[ae]s?|clientes?)")
            if field_name == "pct_monto_referencia_minimo":
                return self._re_float(text, r"(\d+)\s*%.*monto")
            if field_name == "binary_tiene_requisitos_por_lote":
                return self._re_binary(text, r"LOTE\s*N")
            if field_name == "binary_requiere_experiencia_especifica_marca":
                return self._re_binary(text, r"misma marca|marca ofertada")

        elif title_slug == "plazo_de_reposicion_de_bienes":
            if field_name == "dias_reposicion":
                return self._re_dias_reposicion(text)
            if field_name == "binary_dias_habiles":
                return self._re_binary(text, r"d[ií]as h[aá]biles")
            if field_name == "binary_plazo_segun_autoridad":
                return self._re_binary(text, r"establecido en cada caso")

        elif title_slug == "analisis_de_precios_ofertados":
            if field_name == "pct_diferencia_precio_bajo":
                return self._re_pct_diferencia_precio_bajo(text)
            if field_name == "pct_diferencia_precio_alto":
                return self._re_pct_diferencia_precio_alto(text)
            if field_name == "binary_solicita_explicacion_precios":
                return self._re_binary(text, r"explicaci[oó]n.*detallada.*precio")
            if field_name == "binary_permite_declarar_inaceptable":
                return self._re_binary(text, r"declarado inaceptable")
            if field_name == "binary_tiene_multiplicidad_criterios":
                return self._re_binary(text, r"multiplicidad de criterios")

        elif title_slug == "formas_y_condiciones_de_pago":
            if field_name == "dias_pago_maximo":
                return self._re_int(text, r"(\d+)\s*d[ií]as.{0,60}?factura")
            if field_name == "dias_aceptacion_rechazo_factura":
                return self._re_int(text, r"(\d+)\s*d[ií]as.{0,30}?[aá]ceptaci[oó]n")
            if field_name == "num_documentos_pago":
                return self._re_int(text, r"(\d+)\.\s*(?:Nota|Factura|REPSE)")
            if field_name == "binary_requiere_cuenta_bancaria_estatal":
                return self._re_binary(text, r"cuenta corriente.*banco|SITE|Tesorer[ií]a")

        elif title_slug == "plan_de_entrega_de_los_bienes":
            if field_name == "binary_tiene_plan_entrega":
                return self._re_binary(text, r"[Pp]lan de entrega")
            if field_name == "binary_incluye_cronograma":
                return self._re_binary(text, r"cronograma de cumplimiento")
            if field_name == "binary_requiere_documentos_embarque":
                return self._re_binary(text, r"documentos de embarque")

        elif title_slug == "periodo_de_validez_de_la_garantia_de_los_bienes":
            if field_name == "dias_validez_garantia":
                return self._re_dias_validez_garantia(text)
            if field_name == "binary_cubre_defectos_fabricacion":
                return self._re_binary(text, r"defectos.*fabricaci[oó]n|defectos.*dise[nñ]o")
            if field_name == "binary_tiene_cobertura_especifica":
                return self._re_binary(text, r"cobertura")

        elif title_slug == "capacidad_tecnica":
            if field_name == "anos_experiencia_minima_tecnica":
                return self._re_anos_experiencia_minima_tecnica(text)
            if field_name == "num_referencias_tecnicas_minimo":
                return self._re_int(text, r"(?:m[ií]nimo\s+(?:de\s+)?|como m[ií]nimo\s+)(\d{1,3})(?:\s*\([^)]+\))?\s*(?:constancias?|referencias?\s*t[eé]cnicas|clientes?)")
            if field_name == "pct_monto_referencia_tecnica":
                return self._re_float(text, r"(?:el\s+)?(\d{1,3})(?:[.,]\d+)?\s*%\s*(?:del\s+monto|del\s+precio|de\s+las\s+ventas|de\s+las\s+recepciones)")

        elif title_slug == "capacidad_financiera":
            if field_name == "binary_tiene_requisitos_financieros":
                return self._re_binary(text, r"[íi]ndices|financier")
            if field_name == "ratio_liquidez_minimo":
                return self._re_float(text, r"[lL]iquidez.{0,40}?(\d+[.,]\d+)")
            if field_name == "ratio_endeudamiento_maximo":
                return self._re_float(text, r"[eE]ndeudamiento.{0,40}?(\d+[.,]\d+)")
            if field_name == "binary_requiere_estados_auditados":
                return self._re_binary(text, r"estados.*auditad")

        elif title_slug == "reajuste":
            if field_name == "binary_tiene_formula_reajuste":
                return self._re_binary(text, r"f[oó]rmula.*reajuste|Pr\s*=\s*P")
            if field_name == "num_componentes_formula":
                return self._re_int(text, r"R\s*=\s*\d+\.\d+")
            if field_name == "binary_referencia_ipc":
                return self._re_binary(text, r"IPC|Indice de Precios al Consumidor")
            if field_name == "binary_no_reajuste_si_atraso":
                return self._re_binary(text, r"atrasado.*reajuste")

        elif title_slug == "criterios_de_adjudicacion":
            if field_name == "binary_criterio_menor_precio":
                return self._re_binary(text, r"m[aá]s baja|menor precio")
            if field_name == "binary_multiplicidad_criterios":
                return self._re_binary(text, r"combinaci[oó]n de criterios|multiplicidad")
            if field_name == "binary_contrato_abierto":
                return self._re_binary(text, r"contrato abierto|cantidades m[aá]ximas")
            if field_name == "binary_reserva_disminuir_cantidad":
                return self._re_binary(text, r"disminuir.*cantidad")

        elif title_slug == "forma_de_instrumentacion_de_garantia_de_anticipo":
            if field_name == "binary_requiere_garantia_anticipo":
                return self._re_binary(text, r"garant[ií]a.*anticipo")
            if field_name == "categorical_tipo_garantia":
                return self._extract_categorical(text, r"p[oó]liza|fianza|bancaria|seguro")
            if field_name == "pct_anticipo":
                return self._re_float(text, r"(\d+[.,]\d+)\s*%")

        elif title_slug == "requisitos_de_calificacion":
            if field_name == "num_pasos_verificacion":
                return self._re_num_pasos_verificacion(text)
            if field_name == "binary_verifica_sinarh":
                return self._re_binary(text, r"SINARH|VICE MINISTERIO DE CAPITAL")
            if field_name == "binary_requiere_declaracion_conflictos":
                return self._re_binary(text, r"declaraci[oó]n.*jurada.*conflicto|conflicto de intereses")
            if field_name == "binary_requiere_registro_proveedores":
                return self._re_binary(text, r"Registro de Proveedores del Estado")

        elif title_slug == "solicitud_de_pago_de_anticipo":
            if field_name == "binary_tiene_condiciones_anticipo":
                return self._re_binary(text, r"plazo m[aá]ximo.*solicitud")
            if field_name == "dias_solicitud_anticipo":
                return self._re_dias_solicitud_anticipo(text)
            if field_name == "dias_pago_anticipo":
                return self._re_dias_pago_anticipo(text)
            if field_name == "binary_anticipo_solo_mipymes":
                return self._re_binary(text, r"MIPYMES|Micro.*Peque")

        elif title_slug == "causales_de_terminacion_del_contrato":
            if field_name == "binary_incluye_terminacion_incumplimiento":
                return self._re_binary(text, r"Terminaci[oó]n por Incumplimiento")
            if field_name == "binary_incluye_terminacion_fraude":
                return self._re_binary(text, r"fraude y corrupci[oó]n")
            if field_name == "binary_incluye_terminacion_quiebra":
                return self._re_binary(text, r"Insolvencia o quiebra")
            if field_name == "binary_incluye_terminacion_conveniencia":
                return self._re_binary(text, r"Terminaci[oó]n por conveniencia")

        elif title_slug == "confidencialidad_de_la_informacion":
            if field_name == "binary_tiene_excepciones_confidencialidad":
                return self._re_binary(text, r"excepci[oó]n|except[uo]|no.*confidencial|p[uú]blica.*orden judicial")
            if field_name == "binary_menciona_plazo_confidencialidad":
                return self._re_binary(text, r"plazo|duraci[oó]n|vigencia.*confidencial")

        elif title_slug == "impuestos_y_derechos":
            if field_name == "binary_responsabilidad_integral_proveedor":
                return self._re_binary(text, r"todos los impuestos|responsabilidad.*impuesto")
            if field_name == "binary_menciona_iva":
                return self._re_binary(text, r"IVA|Impuesto al Valor Agregado")
            if field_name == "binary_incluye_exoneraciones":
                return self._re_binary(text, r"exoneraci[oó]n|beneficios tributarios")

        elif title_slug == "suministros_y_especificaciones_tecnicas":
            if field_name == "binary_menciona_normas_oficiales":
                return self._re_binary(text, r"normas oficiales|normas.*Paraguay")
            if field_name == "binary_permite_cambios_codigos":
                return self._re_binary(text, r"cambio de.*c[oó]digos")
            if field_name == "binary_proveedor_puede_rehusar_responsabilidad":
                return self._re_binary(text, r"rehusar responsabilidad")

        elif title_slug == "derechos_intelectuales":
            if field_name == "binary_cesion_derechos_contratante":
                return self._re_binary(text, r"contratante.*derechos.*propiedad|derechos.*corresponden.*contratante")
            if field_name == "binary_prohibe_reproduccion_no_autorizada":
                return self._re_binary(text, r"reproducci[oó]n|distribuci[oó]n|divulgaci[oó]n")

        elif title_slug == "documentacion_requerida_para_la_firma_del_contrato":
            if field_name == "num_categorias_documentos":
                return self._re_num_categorias_documentos(text)
            if field_name == "binary_requiere_certificado_tributario":
                return self._re_binary(text, r"Certificado de Cumplimiento Tributario")
            if field_name == "binary_requiere_certificado_ips":
                return self._re_binary(text, r"Instituto de Previsi[oó]n Social")
            if field_name == "binary_incluye_condiciones_contractuales":
                return self._re_binary(text, r"CONDICIONES CONTRACTUALES")

        elif title_slug == "criterios_de_desempate_de_ofertas":
            if field_name == "binary_menciona_criterios_dncp":
                return self._re_binary(text, r"DNCP")
            if field_name == "binary_incluye_criterios_especificos_adicionales" or field_name == "binary_incluye_criterios_especificos_Adicionales":
                return self._re_binary(text, r"criterio.*adicional|adem[aá]s.*criterio|propio.*criterio")

        elif title_slug == "aclaracion_de_las_ofertas":
            if field_name == "binary_comite_puede_solicitar_aclaracion":
                return self._re_binary(text, r"Comit[eé].*solicitar.*aclaraci[oó]n")
            if field_name == "binary_prohibe_modificacion_sustancia":
                return self._re_binary(text, r"modificaci[oó]n.*sustancia")
            if field_name == "binary_solicitud_respuesta_por_escrito":
                return self._re_binary(text, r"por escrito")

        elif title_slug == "fuerza_mayor":
            if field_name == "dias_notificacion_fuerza_mayor":
                return self._re_int(text, r"(\d+)\s*d[ií]as.{0,40}?[Ff]uerza")
            if field_name == "binary_incluye_ejemplos_tipicos":
                return self._re_binary(text, r"guerras.*revoluciones.*incendios")
            if field_name == "binary_obliga_continuar_cumplimiento":
                return self._re_binary(text, r"continuar.*cumpliendo.*obligaciones")

        self.logger.warning("No regex defined: %s / %s", title_slug, field_name)
        return self._default_value(ftype)

    def _default_value(self, ftype: str):
        if ftype in ("int", "binary"):
            return 0
        elif ftype == "float":
            return 0.0
        elif ftype == "categorical":
            return ""
        return 0

    def _re_float(self, text: str, pattern: str):
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        for g in m.groups():
            if g:
                try:
                    return float(str(g).replace(",", "."))
                except ValueError:
                    continue
        return None

    # --- Gate A1: anclaje por clausula + palabras + plantilla vacia (sesion 21) ---
    NUM_WORDS = {
        'un': 1, 'uno': 1, 'una': 1, 'dos': 2, 'tres': 3, 'cuatro': 4, 'cinco': 5,
        'seis': 6, 'siete': 7, 'ocho': 8, 'nueve': 9, 'diez': 10, 'once': 11,
        'doce': 12, 'trece': 13, 'catorce': 14, 'quince': 15, 'dieciseis': 16,
        'dieciséis': 16, 'diecisiete': 17, 'dieciocho': 18, 'diecinueve': 19,
        'veinte': 20, 'veintiuno': 21, 'veintidos': 22, 'veintidós': 22,
        'veinticinco': 25, 'treinta': 30, 'cuarenta': 40, 'cincuenta': 50,
    }
    MESES_A_DIAS = {1: 30, 2: 61, 3: 91, 4: 121, 5: 152, 6: 182,
                    7: 213, 8: 243, 9: 274, 10: 304, 11: 334, 12: 365}
    _BLANK_RE = re.compile(
        r"haga clic|escriba|___|\.\.\.|texto en blanco|sin completar|"
        r"dejar en blanco|\(completar|\[.*?(completar|escribir).*?\]",
        re.IGNORECASE)

    @classmethod
    def _is_blank_template(cls, text: str) -> bool:
        return bool(cls._BLANK_RE.search(text or ""))

    @classmethod
    def _num_token(cls, tok: str):
        tok = (tok or "").strip().lower()
        if re.fullmatch(r"\d+", tok):
            return int(tok)
        return cls.NUM_WORDS.get(tok)

    @classmethod
    def _num_dias_in(cls, frag: str):
        """N (digito o palabra, con o sin '(palabra)') + 'dias' en un fragmento."""
        m = re.search(
            r"(\d{1,3}|[a-záéíóúñ]+)\s*(?:\(\s*[\w\s]+\s*\)\s*)?d[ií]as",
            frag, re.IGNORECASE)
        return cls._num_token(m.group(1)) if m else None

    def _re_dias_pago_anticipo(self, text: str):
        # Ancla a la clausula de PAGO ("se abonara", "plazo en que se abonara");
        # penaliza oraciones de SOLICITUD (mismo titulo, otro campo).
        if self._is_blank_template(text):
            return None
        for sent in re.split(r"[.;\n]+", text or ""):
            if re.search(r"solicitud|presentaci[oó]n", sent, re.IGNORECASE):
                if not re.search(r"se?\s*abonar[aá]|abonar\s*(?:tras|dentro|en)|"
                                 r"plazo[^.]{0,60}?abon", sent, re.IGNORECASE):
                    continue
            if re.search(r"se?\s*abonar[aá]|abonar\s*(?:tras|dentro|en)|"
                         r"plazo[^.]{0,60}?abon|abon[^.]{0,40}?\d",
                         sent, re.IGNORECASE):
                v = self._num_dias_in(sent)
                if v is not None:
                    return v
        return None

    def _re_dias_solicitud_anticipo(self, text: str):
        # Ancla a la clausula de SOLICITUD; acepta habiles/corridos/calendario y palabras.
        if self._is_blank_template(text):
            return None
        for sent in re.split(r"[.;\n]+", text or ""):
            if not re.search(r"solicitud de pago de anticipo|presentaci[oó]n de la "
                             r"solicitud|se solicitar[aá]", sent, re.IGNORECASE):
                continue
            m = re.search(
                r"(\d{1,3}|[a-záéíóúñ]+)\s*(?:\(\s*[\w\s]+?\s*\)\s*)?d[ií]as"
                r"\s*(?:corridos|h[aá]biles|calendario)?",
                sent, re.IGNORECASE)
            if m:
                v = self._num_token(m.group(1))
                if v is not None:
                    return v
        return None

    HUNDREDS_TENS = {
        'cien': 100, 'ciento': 100, 'doscientos': 200, 'doscientas': 200,
        'trescientos': 300, 'trescientas': 300, 'cuatrocientos': 400,
        'quinientos': 500, 'seiscientos': 600, 'setecientos': 700,
        'ochocientos': 800, 'novecientos': 900, 'veinte': 20, 'treinta': 30,
        'cuarenta': 40, 'cincuenta': 50, 'sesenta': 60, 'setenta': 70,
        'ochenta': 80, 'noventa': 90,
    }

    @classmethod
    def _parse_spanish_number(cls, frag: str):
        """'trescientos sesenta y cinco' -> 365. None si hay palabra desconocida."""
        total, cur, seen = 0, 0, False
        for tok in re.findall(r"[a-záéíóúñ]+", (frag or "").lower()):
            if tok in cls.HUNDREDS_TENS:
                cur += cls.HUNDREDS_TENS[tok]
                seen = True
            elif tok in cls.NUM_WORDS:
                cur += cls.NUM_WORDS[tok]
                seen = True
            elif tok in ("y", "de"):
                continue
            else:
                return None
        return cur if seen and cur > 0 else None

    @classmethod
    def _num_unidad_in(cls, sent: str):
        """(numero, unidad) en una oracion: digito (con parentesis) o palabras
        compuestas. Los digitos entre parentesis mandan: 'sesenta dias (60)' -> 60."""
        m = re.search(
            r"(?:(\d{1,4})\s*(?:\(\s*[\w\s]+?\s*\)\s*)?|"
            r"([a-záéíóúñ]+(?:\s+[a-záéíóúñ]+){0,5}?))\s*(meses?|a[ñn]os?|d[ií]as)",
            sent or "", re.IGNORECASE)
        if not m:
            return None, None
        tail = (sent or "")[m.end():m.end() + 12]
        pm = re.search(r"\(\s*(\d{1,4})\s*\)", tail)
        if pm:
            return int(pm.group(1)), m.group(3).lower()
        if m.group(1):
            return int(m.group(1)), m.group(3).lower()
        return cls._parse_spanish_number(m.group(2)), m.group(3).lower()

    _DEF_RE = re.compile(
        r"ser[aá]|ser[aá]n|es el siguiente|per[ií]odo|tendr[aá]|vigencia|validez",
        re.IGNORECASE)
    _REQ_RE = re.compile(
        r"deber[aá]|debe\b|exig|solicita|requiere|requisito|igual o superior",
        re.IGNORECASE)

    @classmethod
    def _blocks(cls, text: str):
        """Oraciones; si una termina en encabezado ('sera el siguiente:') sin numero,
        se fusiona con la siguiente (layout header-valor en lineas separadas)."""
        sents = re.split(r"[.;\n]+", text or "")
        out = []
        i = 0
        while i < len(sents):
            s = sents[i]
            if (re.search(r"ser[aá]\s+el\s+siguiente\s*:?\s*$|"
                          r"siguiente\s*:?\s*$|a\s+continuaci[oó]n\s*:?\s*$",
                          s, re.IGNORECASE)
                    and not re.search(r"\d", s) and i + 1 < len(sents)):
                s = s + " " + sents[i + 1]
                i += 1
            out.append(s)
            i += 1
        return out

    def _re_dias_validez_garantia(self, text: str):
        # Solo clausulas DEFINITORIAS ("sera ..."); las de requisito ("debera ser
        # igual o superior") van al LLM. Meses/anos solo con ancla definitoria.
        if self._is_blank_template(text):
            return None
        text = text or ""
        m = re.search(r"(\d{1,3})\s*meses?\s*,\s*(\d{1,3})\s*d[ií]as",
                      text, re.IGNORECASE)
        if m:
            return int(m.group(2))  # dias explicitos mandan sobre el mapa
        sents = [s for s in self._blocks(text)
                 if re.search(r"validez|garant[ií]a|vigencia", s, re.IGNORECASE)
                 and not re.search(r"indisponibilidad|reparaci[oó]n|reposici[oó]n|"
                                   r"sustituci[oó]n", s, re.IGNORECASE)]
        defin = [s for s in sents if self._DEF_RE.search(s)
                 and not self._REQ_RE.search(s)]
        for s in defin:  # pasada 1: meses/anos con ancla definitoria
            n, u = self._num_unidad_in(s)
            if n is None or n <= 0:
                continue
            if u.startswith("mes"):
                return self.MESES_A_DIAS.get(n, n * 30)
            if u.startswith("a"):
                return n * 365
        for s in defin:  # pasada 2: dias pelados con ancla definitoria
            n, u = self._num_unidad_in(s)
            if n is not None and n > 0 and u.startswith("d"):
                return n
        return None

    def _re_anos_experiencia_minima_tecnica(self, text: str):
        # Candidatos estrictos "N anos" (digito, parentesis o palabra); se elige el
        # mas cercano DESPUES de una mencion a "experiencia" (regla del GT humano).
        # Se ignoran: numeros de item de lista, oraciones de antiguedad (empresa,
        # no persona) y puntajes ("Experiencia 18" puntos).
        if self._is_blank_template(text):
            return None
        text = text or ""
        cands = []
        for m in re.finditer(
                r"(\d{1,2}|[a-záéíóúñ]+)\s*(?:\(\s*[\w\s]+?\s*\)\s*)?a[ñn]os",
                text, re.IGNORECASE):
            line_start = text.rfind("\n", 0, m.start()) + 1
            if re.fullmatch(r"\s*\d{1,2}\s*[.)\-]\s*", text[line_start:m.start()]):
                continue  # numero de item de lista
            v = (int(m.group(1)) if re.fullmatch(r"\d+", m.group(1))
                 else self._parse_spanish_number(m.group(1)))
            if v is None or v > 50:
                continue
            s0 = max(0, text.rfind(".", 0, m.start()))
            sent = text[s0:m.end() + 60]
            if re.search(r"antigüedad|puntos|pts\b|puntaje|factores de evaluaci[oó]n",
                         sent, re.IGNORECASE):
                continue
            cands.append((m.start(), v))
        if not cands:
            return None
        exp_pos = [m.start() for m in
                   re.finditer(r"experiencia", text, re.IGNORECASE)]
        if exp_pos:
            scored = []
            for pos, v in cands:
                prev = [p for p in exp_pos if p <= pos]
                scored.append((pos - prev[-1] if prev else 10 ** 9, pos, v))
            scored.sort()
            if scored[0][0] <= 120:
                return scored[0][2]
        return None  # sin ancla cercana: al LLM (precision > recall para el gate)

    def _re_int(self, text: str, pattern: str):
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        for g in m.groups():
            if g:
                digits = re.sub(r"\D", "", str(g))
                if digits:
                    return int(digits)
        return None

    def _re_binary(self, text: str, pattern: str):
        return 1 if re.search(pattern, text, re.IGNORECASE | re.DOTALL) else 0

    def _re_pct_pct_multas(self, text: str):
        # Mirrors deterministic_pct in scripts/benchmark_multi_vs_gt.py (Option A).
        if re.search(r"hora de atraso", text, re.IGNORECASE):
            return None
        m = re.search(r"(?:será de|sera de|es de)\s*:?\s*([0-9]+(?:[.,][0-9]+)?)",
                      text, re.IGNORECASE)
        if not m:
            return None
        v = float(m.group(1).replace(",", "."))
        return v if 0.01 <= v <= 100 else None

    def _re_pct_tasa_mora(self, text: str):
        # Mirrors deterministic_pct in scripts/benchmark_multi_vs_gt.py (Option A).
        m = re.search(r"del:?\s*([0-9]+(?:[.,][0-9]+)?)", text, re.IGNORECASE)
        if not m:
            return None
        v = float(m.group(1).replace(",", "."))
        return v if 0.01 <= v <= 100 else None

    def _re_pct_garantia_cumplimiento(self, text: str):
        # "El Porcentaje ... es de: 10,00%"
        m = re.search(r"[0-9]+[.,][0-9]+\s*%", text, re.IGNORECASE)
        if not m:
            return None
        v = float(m.group(0).replace(",", ".").replace("%", "").strip())
        return v if 0.01 <= v <= 100 else None

    def _re_pct_diferencia_precio_bajo(self, text: str):
        # "sea superior al 25% para ofertas por debajo del precio referencial"
        m = re.search(r"superior\s+al\s+([0-9]+(?:[.,][0-9]+)?)\s*%", text, re.IGNORECASE)
        if not m:
            return None
        v = float(m.group(1).replace(",", "."))
        return v if 0.01 <= v <= 100 else None

    def _re_pct_diferencia_precio_alto(self, text: str):
        # "...por debajo del precio referencial y del 15% para ofertas que se
        # encuentren por encima del referencial" (captures the "y del 15%")
        m = re.search(r"y\s+(?:del\s+)?([0-9]+(?:[.,][0-9]+)?)\s*%", text, re.IGNORECASE)
        if not m:
            return None
        v = float(m.group(1).replace(",", "."))
        return v if 0.01 <= v <= 100 else None

    def _re_dias_presentacion_garantia(self, text: str):
        # Plantilla uniforme: "dentro de los 10 días corridos ..." o
        # "dentro de los diez (10) días calendario". GT humano: 10.0 (200/200).
        # El patrón rodea "dentro de los/la ... N días" y resiste la variante
        # literal "diez (10)" (número escrito + entre paréntesis).
        for m in re.finditer(r"dentro de (?:los|la)\s+(\d{1,3})\s*d[ií]as|\bdentro de (?:los|la)\s+(?:diez|quince|doce|ocho|cinco)\s*\((\d{1,3})\)\s*d[ií]as", text, re.IGNORECASE):
            num = m.group(2) or m.group(1)
            return int(num)
        return None

    def _extract_categorical(self, text: str, pattern: str):
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(0).lower().strip() if m else ""

    def _re_anos_experiencia_minima(self, text: str):
        # GT: "experiencia ... de al menos 5 años", "al menos N años",
        # "N (N) años", "últimos N años". Validador: 80.0% (160/200), fp=6.
        m = re.search(
            r"(?:experiencia.{0,120}?|al menos\s+|m[ií]nimo de\s+)(?:de\s+)?(\d{1,3})(?:\s*\([^)]+\))?\s*a[sñ]os",
            text, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        return int(re.sub(r"\D", "", m.group(1)))

    def _re_dias_reposicion(self, text: str):
        # GT: "El plazo de reposición ... será de: N (N) días",
        # "SERA DE MAXIMO N (N) DÍAS". Validador: 92.0% (184/200), fp=0.
        m = re.search(
            r"(?:reposici[oó]n(?: de bienes)?[^\n]*?(?:ser[aá] de|ser[aá]|de)\s*:?\s*|ser[aá] de m[aá]ximo\s+|m[aá]ximo\s+)"
            r"(\d{1,3})(?:\s*\([^)]+\))?\s*d[ií]as",
            text, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        return int(re.sub(r"\D", "", m.group(1)))

    def _re_num_categorias_documentos(self, text: str):
        # GT: contar ítems de nivel 1 (1., 2., 3...) de la sección de
        # documentación. El máximo número de ítem numerado es el total.
        # Validador: 100.0% (200/200), fp=0.
        items = re.findall(r"(?:^|\n)\s*(\d{1,2})\s*\.\s*[A-ZÁÉÍÓÚÑÚ]", text, re.MULTILINE)
        if not items:
            return None
        return max(int(x) for x in items)

    def _re_num_pasos_verificacion(self, text: str):
        # Misma receta que num_categorias (el LLM subcuenta listas: devuelve 1
        # aunque haya "1. Verificará... 2. Verificará..."). Maximo item nivel 1.
        # Validador GT 27-sep: 97/97 (100%) en filas con items; resto -> None (LLM).
        items = re.findall(r"(?:^|\n)\s*(\d{1,2})\s*\.\s*[A-ZÁÉÍÓÚÑÚ]", text, re.MULTILINE)
        if not items:
            return None
        return max(int(x) for x in items)

    @staticmethod
    def _aggregate_series(series: pd.Series, ftype: str):
        def _safe(v):
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return 0 if ftype in ("int", "binary") else (float("nan") if ftype == "float" else "")
            if ftype == "float":
                return float(v)
            if ftype in ("int", "binary"):
                return int(v)
            return str(v)
        return series.apply(_safe)
