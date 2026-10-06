"""
preparar_gt_nuevo_7variables.py
Genera gold standard CSV con 100 muestras por cada una de las 8 variables academicas nuevas (N1-N8)
para clasificacion manual posterior (ground truth).

- Usa BD dncp pliegos_secciones (2775485 filas)
- Muestreo aleatorio sin reemplazo, seed 42, por variable
- Genera prompt por campo (Campo a extraer + Tipo + Guia) compatible con benchmark_multi_vs_gt.py
- Escribe data/processed/alolab/gold_standard_pliego_nuevo_8variables.csv (800 filas)

Columnas (compatibles con gold_standard_26campos.csv + enriquecidas):
  nro_licitacion, campo, variable, title_slug, title_original, text, word_count, page,
  prompt, ground_truth, nota, prioridad, fuente_academica, db_value, llm_raw, llm_value

Ejecucion:
  python scripts/preparar_gt_nuevo_7variables.py
"""
import os
from dotenv import load_dotenv

load_dotenv()

import csv
import random
import json
import pathlib
import psycopg2
from psycopg2.extras import RealDictCursor

# --- Config ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": 5433,
    "dbname": "dncp",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
}
OUT_PATH = pathlib.Path("data/processed/alolab/gold_standard_pliego_nuevo_8variables.csv")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# 7 variables nuevas mapeadas a titulo normalizado exacto
VARIABLES = [
    {
        "variable": "N1",
        "campo": "dias_validez_oferta",
        "type": "int",
        "title_slug": "periodo de validez de las ofertas",
        "description": "Plazo en dias para validez de las ofertas (en dias calendario) exigido en el pliego",
        "extraction_hint": "Busca el PLAZO EN DIAS para la VALIDEZ DE LAS OFERTAS. Patrones: 'N dias', 'N dias calendario', 'plazo de N dias', 'validas por N dias', 'validas (en dias calendario) por: N', 'vigencia de N dias'. El numero debe estar asociado a 'validez de las ofertas' o 'ofertas validas'. NO extraer porcentajes (%) ni montos (Gs.) ni fechas (15 de enero). Rango valido: 1-365. Si es 0 o no se encuentra, responder NULL. Extraer SOLO el numero.",
        "prioridad": "ALTA #2",
        "fuente": "World Bank 2011 Curbing Fraud (rigged specs) + Ferwerda 2016 shortened time span r=0.200 p=0.005 + EPJ 2022 OTD negativo + B1 short submission period",
    },
    {
        "variable": "N2",
        "campo": "dias_validez_garantia_mantenimiento_oferta",
        "type": "int",
        "title_slug": "periodo de validez de la garantia de mantenimiento de oferta",
        "description": "Plazo en dias de validez de la garantia de mantenimiento de oferta (en dias calendario)",
        "extraction_hint": "Busca el PLAZO EN DIAS para la VALIDEZ DE LA GARANTIA DE MANTENIMIENTO DE OFERTA. Patrones: 'N dias', 'N dias calendario', 'plazo de N dias', 'validez de N dias', 'vigencia de N dias'. El numero debe estar junto a 'garantia de mantenimiento' o 'garantia de oferta'. NO extraer porcentajes (%) ni montos. Rango: 1-365. Si no se encuentra, responder NULL. Extraer SOLO el numero.",
        "prioridad": "ALTA",
        "fuente": "IACRC bid security + TI Handbook 2006 integrity in contract management + Reglamento DNCP garantia mantenimiento",
    },
    {
        "variable": "N3",
        "campo": "dias_plazo_entrega_bien",
        "type": "int",
        "title_slug": "plan de entrega de los bienes",
        "description": "Plazo en dias para la entrega de los bienes segun plan de entrega y cronograma",
        "extraction_hint": "Busca el PLAZO EN DIAS para la ENTREGA DE LOS BIENES. Patrones: 'N dias', 'N dias calendario', 'plazo de N dias', 'entrega en N dias', 'dentro de los N dias corridos/habiles', 'a los N dias de la firma'. El numero debe estar asociado a 'entrega', 'plan de entrega' o 'cronograma de cumplimiento'. Si el texto dice 'No Aplica' o 'Conforme a especificaciones tecnicas', responder NULL. NO extraer porcentajes ni montos. Rango: 1-365. Extraer SOLO el numero. Si hay multiples plazos, extraer el plazo principal de entrega inicial.",
        "prioridad": "ALTA",
        "fuente": "World Bank 2011 engineers estimate focal point + EPJ ad hoc subcontract 42% + Decarolis 2014 renegociacion >=50% ahorro perdido",
    },
    {
        "variable": "N4",
        "campo": "requiere_visita_tecnica",
        "type": "binary",
        "title_slug": "visita al sitio de ejecucion del contrato",
        "description": "Si la convocante exige visita obligatoria al sitio de ejecucion del contrato",
        "extraction_hint": "Determina si se REQUIERE VISITA TECNICA obligatoria al sitio. Retornar 1 si el texto indica visita obligatoria con indicaciones (direccion, fecha, hora, acta, coordinar), 0 si dice exactamente 'No Aplica', 'No aplica' o 'No se requiere visita'. NO confundir con 'visita voluntaria' o 'opcional' (tambien 0 si no es obligatoria). Patrones 1: 'La convocante dispone la realizacion de una visita', 'visita al sitio con las siguientes indicaciones:', 'se labrara acta'. Patrones 0: 'No Aplica'. Retornar SOLO 0 o 1, o NULL si ambiguo.",
        "prioridad": "ALTA #2 (EPJ)",
        "fuente": "EPJ 2022 WV 14.1-14.4 + Bali mandatory site visit -35% precios + IACRC mandatory site visit + Naples Camorra case",
    },
    {
        "variable": "N5",
        "campo": "criterio_adjudicacion",
        "type": "categorical",
        "title_slug": "criterios de adjudicacion",
        "description": "Criterio de adjudicacion: menor precio, calidad-precio (MEAT), costo evaluado u otro",
        "extraction_hint": "Clasifica el CRITERIO DE ADJUDICACION en una de estas categorias: 'precio_mas_bajo' (oferta evaluada como la mas baja y cumple requisitos), 'costo_evaluado' (costo integral evaluado), 'calidad_precio' / 'MEAT' (criterios multiples, puntajes tecnicos + economicos, 'mas ventajosa', 'calidad y precio'), 'otro' (contrato abierto, reserva disminuir cantidad). Patrones: 'adjudicara el contrato al oferente cuya oferta haya sido evaluada como la mas baja' -> precio_mas_bajo. 'criterios multiples', 'calidad y precio', 'puntaje' -> calidad_precio. Si es ambiguo o no especifica, responder NULL. Extraer SOLO la categoria exacta.",
        "prioridad": "ALTA #1 (EPJ MEAT 2x)",
        "fuente": "EPJ 2022 MEAT predictor #1 (2x siguiente) + Ferwerda preferred supplier r=0.487 + Fazekas non-price weight + OECD scoring rule",
    },
    {
        "variable": "N6",
        "campo": "multa_jornales_incumplimiento",
        "type": "int",
        "title_slug": "multas por incumplimiento",
        "description": "Multa por incumplimiento expresada en jornales minimos diarios",
        "extraction_hint": "Busca la MULTA POR INCUMPLIMIENTO expresada en JORNALES. Patrones: 'N jornales', 'N (N) jornales minimos diarios', 'multa de N jornales', 'penalidad de N jornales diarios', 'jornal minimo para actividades diversas'. Extraer N como entero. Ej: '2 (dos) jornales diarios' -> 2. '1 jornal minimo' -> 1. NO extraer porcentajes (%) ni montos en guaranies. Si no se encuentra jornales explicitos, responder NULL. Rango: 1-100.",
        "prioridad": "ALTA",
        "fuente": "Fazekas penalidades asimetricas + EPJ obstacles to participation + TI 10-25% dano + Ley 7021 tope multas",
    },
    {
        "variable": "N7",
        "campo": "tiene_requisito_eliminatorio",
        "type": "binary",
        "title_slug": "requisitos de calificacion",
        "description": "Si el pliego establece requisitos eliminatorios que descartan ofertas automatiquement",
        "extraction_hint": "Determina si existen REQUISITOS ELIMINATORIOS que descartan ofertas. Retornar 1 si el texto contiene frases como 'seran desechadas las ofertas que...', 'seran descalificadas las ofertas que no...', 'no seran evaluadas', 'causales de descalificacion', 'requisitos eliminatorios', 'sera desestimada la oferta'. Retornar 0 si no hay lenguaje eliminatorio explicito o solo lista requisitos sin consecuencia de descarte. Retornar SOLO 0 o 1, o NULL si ambiguo.",
        "prioridad": "ALTA",
        "fuente": "World Bank 2011 Rigged specs r=0.487 + EPJ firm other preference 28% + local regulations 33% + Ley 7021 no discriminatoria",
    },
    {
        "variable": "N8",
        "campo": "pct_subcontratacion_maximo",
        "type": "float",
        "title_slug": "subcontratacion",
        "description": "Porcentaje maximo permitido de subcontratacion del contrato (ej: 50 %; 'No Aplica' = NULL)",
        "extraction_hint": "Busca el PORCENTAJE MAXIMO permitido de SUBCONTRATACION. Patrones: 'El porcentaje permitido para la subcontratacion sera de: N%', 'subcontratacion de N%', 'N% del contrato podra ser subcontratado', 'N por ciento (N%)'. El numero debe estar junto a 'subcontratacion'. Si el texto dice 'No Aplica', 'NO APLICA', 'no aplica' o 'No se permite', responder NULL (subcontratacion prohibida). NO extraer montos en guaranies ni otros porcentajes no relacionados con subcontratacion. Rango valido: 0.01-100. Extraer SOLO el numero (sin dividir).",
        "prioridad": "ALTA",
        "fuente": "EPJ 2022 ad hoc subcontracting 42% + no subcontracting 21% + Ferwerda connections between bidders r=0.178 + Ley 7021 reglamentacion subcontratacion",
    },
]

# muestras por variable (N3 ampliada a 200, resto 100)
MUESTRAS = {v["variable"]: 200 if v["variable"] == "N3" else 100 for v in VARIABLES}

def build_prompt(field):
    # formato compatible con gold_standard_26campos.csv: "Campo a extraer: ...\nTipo esperado: ...\nGuia de extraccion: ..."
    return f"Campo a extraer: {field['description']}\nTipo esperado: {field['type']}\nGuia de extraccion: {field['extraction_hint']}"

def main():
    random.seed(42)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # cargar nros ya anotados de N3 desde el CSV deepseek (si existe) para preservarlos
    deepseek_path = pathlib.Path("data/processed/alolab/gold_standard_pliego_nuevo_8variables_deepseek_v4_flash.csv")
    existing_n3 = {}  # nro -> dict(fila original del deepseek)
    if deepseek_path.exists():
        with open(deepseek_path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["variable"] == "N3":
                    existing_n3[r["nro_licitacion"]] = r
        print(f"Preservando {len(existing_n3)} muestras N3 ya anotadas")

    all_rows = []
    for var in VARIABLES:
        campo = var["campo"]
        title = var["title_slug"]
        prompt = build_prompt(var)
        objetivo = MUESTRAS[var["variable"]]
        print(f"Muestreando {var['variable']} {campo} <- '{title}' (objetivo {objetivo}) ...")
        cur.execute(
            """
            SELECT DISTINCT ON (nro_licitacion)
                   nro_licitacion, title_normalized, content_text, word_count, page
            FROM dncp.pliegos_secciones
            WHERE title_normalized = %s
            ORDER BY nro_licitacion, document_id
            """,
            (title,),
        )
        rows = cur.fetchall()
        print(f"  poblacion total: {len(rows)}")

        if var["variable"] == "N3" and existing_n3:
            # mantener las ya anotadas + muestrear el resto hasta objetivo
            ya_tenidas = list(existing_n3.values())
            ya_nros = set(existing_n3.keys())
            restantes = [r for r in rows if str(r["nro_licitacion"]) not in ya_nros]
            faltan = objetivo - len(ya_tenidas)
            if faltan > 0:
                if faltan > len(restantes):
                    faltan = len(restantes)
                    print(f"  ADVERTENCIA: solo {len(restantes)} restantes para N3")
                nuevas = random.sample(restantes, faltan)
            else:
                nuevas = []
            # reusar filas ya anotadas tal cual
            sampled = ya_tenidas
            sampled_nuevas = nuevas
            print(f"  -> conservadas {len(ya_tenidas)} + nuevas {len(nuevas)}")
        else:
            if len(rows) < objetivo:
                print(f"  ADVERTENCIA: poblacion {len(rows)} < {objetivo}")
                sampled = rows
                sampled_nuevas = []
            else:
                sampled = []
                sampled_nuevas = random.sample(rows, objetivo)

        # shuffle deterministico
        if var["variable"] == "N3" and existing_n3 and not sampled_nuevas:
            pass  # ya son las anotadas, mantener orden
        else:
            random.shuffle(sampled_nuevas)

        # filas nuevas -> crear dict estandar
        for r in sampled_nuevas:
            nro = r["nro_licitacion"]
            text = (r["content_text"] or "").replace("\x00", "")
            all_rows.append({
                "nro_licitacion": str(nro),
                "campo": campo,
                "variable": var["variable"],
                "title_slug": title,
                "title_original": r["title_normalized"],
                "text": text,
                "word_count": r["word_count"],
                "page": r["page"],
                "prompt": prompt,
                "db_value": "",
                "llm_raw": "",
                "llm_value": "",
                "ground_truth": "",
                "nota": "",
                "prioridad": var["prioridad"],
                "fuente_academica": var["fuente"],
                "confianza": "",
                "es_revisar": "0",
            })

        # filas ya anotadas N3 -> preservar todos los campos (incluye ground_truth/nota/confianza/es_revisar)
        for r in sampled:
            if isinstance(r, dict) and "ground_truth" in r and "es_revisar" in r:
                all_rows.append(dict(r))
            else:
                # si llego aqui r es un dict RealDict (caso no-N3 sin existing)
                nro = r["nro_licitacion"]
                text = (r["content_text"] or "").replace("\x00", "")
                all_rows.append({
                    "nro_licitacion": str(nro),
                    "campo": campo,
                    "variable": var["variable"],
                    "title_slug": title,
                    "title_original": r["title_normalized"],
                    "text": text,
                    "word_count": r["word_count"],
                    "page": r["page"],
                    "prompt": prompt,
                    "db_value": "",
                    "llm_raw": "",
                    "llm_value": "",
                    "ground_truth": "",
                    "nota": "",
                    "prioridad": var["prioridad"],
                    "fuente_academica": var["fuente"],
                })
        print(f"  -> total {len(all_rows)} acumuladas para esta variable")

    conn.close()

    # mezclar global? mantener agrupado por variable para facilitar anotacion?
    # Para gold standard manual es mejor agrupado por variable; no mezclar.
    # Ordenar por variable N1..N7 y luego nro_licitacion
    order = {v["variable"]: i for i, v in enumerate(VARIABLES)}
    all_rows.sort(key=lambda x: (order[x["variable"]], x["nro_licitacion"]))

    # Escribir CSV
    fieldnames = ["nro_licitacion","campo","variable","title_slug","title_original","text","word_count","page","prompt","db_value","llm_raw","llm_value","ground_truth","nota","prioridad","fuente_academica","confianza","es_revisar"]
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"\nCSV escrito: {OUT_PATH} ({len(all_rows)} filas, 100 x {len(VARIABLES)} variables)")
    # resumen
    from collections import Counter
    cnt = Counter(r["variable"] for r in all_rows)
    for v in VARIABLES:
        print(f"  {v['variable']} {v['campo']}: {cnt[v['variable']]}")

    # check duplicados nro_licitacion por variable (no debe haber)
    # verificar encoding
    print(f"\nPrimeras 2 filas preview:")
    for r in all_rows[:2]:
        print(f"  {r['variable']} {r['nro_licitacion']} text_len={len(r['text'])} page={r['page']}")

if __name__ == "__main__":
    main()
