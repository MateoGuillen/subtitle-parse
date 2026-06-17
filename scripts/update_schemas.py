"""Update master_schemas.json with proper JSON schemas based on sample analysis."""

import json
import os

MASTER_PATH = os.path.join(
    os.path.dirname(__file__), "..",
    "data", "processed", "section_clustering", "master_schemas.json"
)

SCHEMAS = {
    "fraude y corrupcion": {
        "schema": {
            "menciona_denuncia_penal": {
                "type": "binary",
                "description": "Indica si el texto menciona presentación de denuncia penal ante instancias correspondientes"
            },
            "menciona_sancion_dncp": {
                "type": "binary",
                "description": "Indica si se menciona remisión de antecedentes a la DNCP para aplicación de sanciones"
            },
            "menciona_soborno": {
                "type": "binary",
                "description": "Indica si se menciona explícitamente el acto de ofrecer/dar soborno"
            },
            "menciona_colusion": {
                "type": "binary",
                "description": "Indica si se menciona colusión o acuerdo entre partes para propósito inapropiado"
            },
            "menciona_rescision_contrato": {
                "type": "binary",
                "description": "Indica si se menciona rescisión del contrato como consecuencia de fraude"
            },
            "usa_mayusculas_convocante": {
                "type": "binary",
                "description": "Indica si 'Convocante' está escrito con mayúscula inicial (formal vs. informal)"
            },
            "num_actos_fraude_mencionados": {
                "type": "numeric",
                "description": "Cantidad de actos de fraude/corrupción enumerados en el texto"
            },
            "menciona_etapa_oferta": {
                "type": "binary",
                "description": "Indica si se menciona la etapa de oferta como momento de detección"
            },
            "menciona_ejecucion_contrato": {
                "type": "binary",
                "description": "Indica si se menciona la etapa de ejecución de contrato"
            }
        },
        "justification": "Fraude y corrupción presenta variación en los tipos de actos mencionados (soborno, colusión), las consecuencias (denuncia penal, sanción DNCP, rescisión) y el formato del texto (mayúsculas/minúsculas). La presencia/ausencia de ciertos tipos de penalidades puede indicar documentos anómalos."
    },
    "formato y firma de la oferta": {
        "schema": {
            "menciona_firma_electronica": {
                "type": "binary",
                "description": "Indica si se menciona la posibilidad de firma electrónica"
            },
            "menciona_firma_fisica": {
                "type": "binary",
                "description": "Indica si se menciona la posibilidad de firma física o manuscrita"
            },
            "menciona_representante_legal": {
                "type": "binary",
                "description": "Indica si se menciona firma por representante legal o apoderado"
            },
            "menciona_formulario_oferta": {
                "type": "binary",
                "description": "Indica si se menciona el formulario de oferta como documento a firmar"
            },
            "contiene_enlaces_url": {
                "type": "binary",
                "description": "Indica si el texto contiene enlaces URL a páginas externas"
            },
            "menciona_lista_precios": {
                "type": "binary",
                "description": "Indica si se menciona la lista de precios como parte de la oferta"
            },
            "menciona_anexos": {
                "type": "binary",
                "description": "Indica si se mencionan anexos o documentos adicionales a firmar"
            },
            "num_formatos_exigidos": {
                "type": "numeric",
                "description": "Cantidad de formatos o documentos que requieren firma según el texto"
            }
        },
        "justification": "Las variaciones incluyen si se exige firma electrónica, física o ambas, si se mencionan representantes legales, y si hay enlaces URL. Documentos sin requisitos de firma claros o con formatos inusuales son potencialmente anómalos."
    },
    "copias de la oferta cps": {
        "schema": {
            "menciona_oferta_original": {
                "type": "binary",
                "description": "Indica si se menciona la presentación de la oferta original"
            },
            "especifica_cantidad_copias": {
                "type": "binary",
                "description": "Indica si se especifica una cantidad concreta de copias requeridas"
            },
            "cantidad_copias_requeridas": {
                "type": "numeric",
                "description": "Número de copias requeridas (0 si no se especifica)"
            },
            "menciona_copia_impresa": {
                "type": "binary",
                "description": "Indica si se mencionan copias impresas o físicas"
            },
            "menciona_copia_digital": {
                "type": "binary",
                "description": "Indica si se mencionan copias digitales o electrónicas"
            },
            "requiere_identificacion_copias": {
                "type": "binary",
                "description": "Indica si las copias deben estar debidamente identificadas"
            },
            "menciona_excepcion_copias": {
                "type": "binary",
                "description": "Indica si se menciona que la convocante puede requerir o no copias"
            }
        },
        "justification": "Los clusters varían entre textos que especifican cantidad de copias, tipos (impresa/digital), y requisitos de identificación. Cantidades inusuales o ausencia de especificaciones pueden indicar anomalías."
    },
    "limitacion de responsabilidad": {
        "schema": {
            "menciona_negligencia_grave": {
                "type": "binary",
                "description": "Indica si se menciona negligencia grave como excepción a la limitación"
            },
            "menciona_mala_fe": {
                "type": "binary",
                "description": "Indica si se menciona mala fe o dolo como excepción"
            },
            "menciona_indemnizacion": {
                "type": "binary",
                "description": "Indica si se menciona indemnización o compensación por daños"
            },
            "menciona_danos_directos": {
                "type": "binary",
                "description": "Indica si se mencionan daños directos como categoría"
            },
            "menciona_danos_consecuenciales": {
                "type": "binary",
                "description": "Indica si se mencionan daños consecuenciales o indirectos"
            },
            "menciona_limite_monetario": {
                "type": "binary",
                "description": "Indica si se menciona un límite monetario a la responsabilidad"
            },
            "menciona_propiedad_intelectual": {
                "type": "binary",
                "description": "Indica si se menciona violación de propiedad intelectual como excepción"
            },
            "menciona_incumplimiento_confidencialidad": {
                "type": "binary",
                "description": "Indica si se menciona incumplimiento de confidencialidad como excepción"
            }
        },
        "justification": "Las limitaciones de responsabilidad varían en las excepciones (negligencia grave, mala fe, propiedad intelectual), tipos de daños cubiertos, y si establecen límites monetarios. La ausencia de excepciones clave puede ser anómala."
    },
    "planos y disenos": {
        "schema": {
            "dice_no_aplica": {
                "type": "binary",
                "description": "Indica si el texto dice 'No Aplica' indicando que no hay planos"
            },
            "menciona_archivos_pdf": {
                "type": "binary",
                "description": "Indica si se mencionan archivos PDF adjuntos en el SICP"
            },
            "menciona_cantidad_planos": {
                "type": "binary",
                "description": "Indica si se especifica una cantidad concreta de planos o diseños"
            },
            "menciona_especificaciones_tecnicas": {
                "type": "binary",
                "description": "Indica si se mencionan especificaciones técnicas junto con los planos"
            },
            "menciona_planos_estructurales": {
                "type": "binary",
                "description": "Indica si se mencionan planos estructurales o de ingeniería"
            },
            "menciona_planos_arquitectonicos": {
                "type": "binary",
                "description": "Indica si se mencionan planos arquitectónicos"
            },
            "menciona_planos_instalaciones": {
                "type": "binary",
                "description": "Indica si se mencionan planos de instalaciones (eléctrica, sanitaria, etc.)"
            },
            "num_tipos_planos": {
                "type": "numeric",
                "description": "Cantidad de tipos o categorías de planos mencionados"
            }
        },
        "justification": "La distinción principal entre clusters es si dice 'No Aplica' o si lista planos específicos. La ausencia de planos cuando se esperan, o la mención de planos cuando no aplica, son indicadores de anomalías."
    },
    "porcentaje de garantia de fiel cumplimiento de con": {
        "schema": {
            "porcentaje_garantia": {
                "type": "numeric",
                "description": "Porcentaje de garantía de fiel cumplimiento mencionado (valor numérico, ej: 10.0)"
            },
            "plazo_presentacion_dias": {
                "type": "numeric",
                "description": "Cantidad de días plazo para presentar la garantía (ej: 10)"
            },
            "menciona_garantia_cumplimiento": {
                "type": "binary",
                "description": "Indica si se usa el término 'Garantía de Cumplimiento de Contrato'"
            },
            "menciona_garantia_fiel_cumplimiento": {
                "type": "binary",
                "description": "Indica si se usa el término 'Garantía de Fiel Cumplimiento de Contrato'"
            },
            "menciona_proveedor": {
                "type": "binary",
                "description": "Indica si la obligación recae sobre 'el proveedor'"
            },
            "menciona_adjudicatario": {
                "type": "binary",
                "description": "Indica si la obligación recae sobre 'el adjudicatario'"
            },
            "menciona_porcentaje_variable": {
                "type": "binary",
                "description": "Indica si el porcentaje puede variar según el tipo de contratación"
            }
        },
        "justification": "Los porcentajes de garantía (5%, 10%) y plazos varían entre clusters. Porcentajes fuera del rango típico (5-10%) o plazos inusuales son indicadores de anomalías. La terminología usada también varía."
    },
    "idioma de la oferta": {
        "schema": {
            "solo_castellano": {
                "type": "binary",
                "description": "Indica si el texto exige únicamente idioma castellano"
            },
            "permite_traduccion_oficial": {
                "type": "binary",
                "description": "Indica si permite oferta en otro idioma con traducción oficial"
            },
            "menciona_traductor_publico": {
                "type": "binary",
                "description": "Indica si se menciona traductor público matriculado en Paraguay"
            },
            "menciona_castellano_y_guarani": {
                "type": "binary",
                "description": "Indica si se menciona castellano y guaraní como idiomas oficiales"
            },
            "exige_traduccion_legalizada": {
                "type": "binary",
                "description": "Indica si la traducción debe ser legalizada o apostillada"
            },
            "menciona_idioma_extranjero_especifico": {
                "type": "binary",
                "description": "Indica si se menciona un idioma extranjero específico (ej: inglés)"
            }
        },
        "justification": "Los clusters varían entre exigir solo castellano vs. permitir traducción oficial. La mención de traductor público, legalización o idiomas específicos diferencia clusters. Exigir solo castellano sin excepción puede ser restrictivo."
    },
    "aclaracion de las ofertas": {
        "schema": {
            "aclaracion_obligatoria": {
                "type": "binary",
                "description": "Indica si la aclaración es obligatoria ('solicitará' en lugar de 'podrá solicitar')"
            },
            "menciona_comite_evaluacion": {
                "type": "binary",
                "description": "Indica si se menciona al Comité de Evaluación como solicitante"
            },
            "menciona_plazo_aclaracion": {
                "type": "binary",
                "description": "Indica si se menciona un plazo específico para las aclaraciones"
            },
            "menciona_forma_escrita": {
                "type": "binary",
                "description": "Indica si las aclaraciones deben ser por escrito"
            },
            "menciona_revision_evaluacion_comparacion": {
                "type": "binary",
                "description": "Indica si se mencionan los propósitos: revisión, evaluación, comparación"
            },
            "menciona_calificacion_ofertas": {
                "type": "binary",
                "description": "Indica si se menciona la calificación de ofertas como propósito"
            },
            "menciona_notificacion_resultados": {
                "type": "binary",
                "description": "Indica si se menciona la notificación de resultados de la aclaración"
            },
            "plazo_aclaracion_dias": {
                "type": "numeric",
                "description": "Cantidad de días plazo para presentar aclaraciones (0 si no se menciona)"
            }
        },
        "justification": "La distinción clave es si la aclaración es obligatoria ('solicitará') u opcional ('podrá solicitar'), y si menciona plazos. Documentos sin plazos o con lenguaje permisivo pueden ser anómalos."
    },
    "retiro sustitucion y modificacion de las ofertas": {
        "schema": {
            "menciona_ofertas_fisicas": {
                "type": "binary",
                "description": "Indica si el texto distingue explícitamente entre ofertas físicas y electrónicas"
            },
            "requiere_comunicacion_escrita": {
                "type": "binary",
                "description": "Indica si requiere comunicación por escrito para retiro/sustitución"
            },
            "requiere_firma_comunicacion": {
                "type": "binary",
                "description": "Indica si la comunicación debe estar debidamente firmada"
            },
            "menciona_plazo_retiro": {
                "type": "binary",
                "description": "Indica si se menciona un plazo límite para retirar ofertas"
            },
            "permite_modificacion_parcial": {
                "type": "binary",
                "description": "Indica si se permite modificación parcial de la oferta"
            },
            "menciona_ofertas_electronicas": {
                "type": "binary",
                "description": "Indica si se menciona el procedimiento para ofertas electrónicas"
            },
            "menciona_sustitucion_total": {
                "type": "binary",
                "description": "Indica si la sustitución debe ser total (no parcial)"
            },
            "menciona_hora_limite": {
                "type": "binary",
                "description": "Indica si se menciona una hora límite para retiro/sustitución"
            }
        },
        "justification": "Los clusters varían en si distinguen ofertas físicas de electrónicas, si requieren comunicación escrita firmada, y plazos. La ausencia de requisitos de forma puede ser indicador de anomalía."
    },
    "audiencia informativa": {
        "schema": {
            "menciona_derecho_solicitar": {
                "type": "binary",
                "description": "Indica si se menciona el derecho del oferente a solicitar audiencia"
            },
            "menciona_explicacion_fundamentos": {
                "type": "binary",
                "description": "Indica si la audiencia es para explicar fundamentos de la decisión"
            },
            "menciona_plazo_solicitud": {
                "type": "binary",
                "description": "Indica si se menciona un plazo para solicitar la audiencia"
            },
            "menciona_resultado_adjudicacion": {
                "type": "binary",
                "description": "Indica si la audiencia se refiere al resultado de adjudicación"
            },
            "menciona_notificacion_previa": {
                "type": "binary",
                "description": "Indica si se requiere notificación previa del resultado antes de la audiencia"
            },
            "menciona_audiencia_presencial": {
                "type": "binary",
                "description": "Indica si la audiencia es presencial"
            },
            "menciona_audiencia_virtual": {
                "type": "binary",
                "description": "Indica si se permite audiencia virtual o electrónica"
            },
            "plazo_solicitud_dias": {
                "type": "numeric",
                "description": "Cantidad de días plazo para solicitar audiencia (0 si no se menciona)"
            }
        },
        "justification": "Las audiencias varían en si mencionan plazos, modalidad (presencial/virtual), y el propósito. Plazos inusualmente cortos o largos, o ausencia de modalidad pueden ser anómalos."
    }
}

EXTRACTION_PROMPT_TEMPLATE = """Eres un extractor de datos estructurados para pliegos de licitaciones públicas paraguayas.

CONTEXTO: Sección "{titulo}" de un pliego de licitación.
{titulo_descripcion}

Debes analizar el texto proporcionado y extraer UNICAMENTE los campos definidos en el siguiente esquema JSON.

ESQUEMA:
{esquema_str}

REGLAS:
1. Campos BINARIOS: 1 si la característica está presente en el texto, 0 si no.
2. Campos NUMÉRICOS: el valor numérico encontrado. Si no hay valor, poner 0.
3. Responde EXCLUSIVAMENTE con un JSON válido, sin texto adicional.
4. No inventes información que no esté en el texto.

TEXTO A ANALIZAR:
{texto}

RESPUESTA (solo JSON):"""

TITULO_DESCRIPCION = {
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


def build_extraction_prompt(title, schema):
    esquema_lines = []
    for field_name, field_def in schema.items():
        ftype = field_def.get("type", "binary")
        fdesc = field_def.get("description", "")
        esquema_lines.append(f'  "{field_name}": ({ftype}) {fdesc}')
    esquema_str = "\n".join(esquema_lines)
    desc = TITULO_DESCRIPCION.get(title, "")
    return EXTRACTION_PROMPT_TEMPLATE.format(
        titulo=title,
        titulo_descripcion=desc,
        esquema_str=esquema_str,
        texto="{texto}",
    )


def main():
    with open(MASTER_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    for title, schema_info in SCHEMAS.items():
        if title not in data:
            print(f"Title '{title}' not in master JSON, skipping.")
            continue

        schema = schema_info["schema"]
        justification = schema_info["justification"]
        prompt = build_extraction_prompt(title, schema)

        data[title]["json_schema"] = schema
        data[title]["schema_justification"] = justification
        data[title]["extraction_prompt"] = prompt

        print(f"Updated '{title}': {len(schema)} fields")

    with open(MASTER_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to {MASTER_PATH}")
    print(f"Total titles updated: {len(SCHEMAS)}")


if __name__ == "__main__":
    main()
