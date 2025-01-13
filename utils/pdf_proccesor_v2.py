import re
import pdfplumber
from PyPDF2 import PdfReader
import pandas as pd

class ProcesadorPDF:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path

    def limpiar_texto(self, texto):
        """
        Función para limpiar texto duplicado y reemplazar guiones
        """
        texto = re.sub(r'(\w)\1+', r'\1', texto)  # Eliminar letras duplicadas
        texto = texto.replace("--", "-")  # Reemplazar guiones dobles por un solo guion
        texto = texto.replace("::", ":")  # Reemplazar múltiples comas consecutivas por una sola coma
        texto = texto.replace("..", ".")  # Reemplazar puntos consecutivos
        texto = re.sub(r',+', ',', texto)  # Reemplazar comas múltiples por una sola
        return re.sub(r'\s+', ' ', texto).strip()  # Limpiar espacios extras

    def extraer_lineas_pdf(self):
        """
        Extrae las líneas del PDF y devuelve una lista de tuplas con el texto limpio y la página correspondiente
        """
        lineas_pdf = []  # Lista para almacenar las tuplas (texto, página)
        combinaciones_conocidas = [
            ('REQUISITOS DE PARTICIPACIÓN Y CRITERIOS DE', 'EVALUACIÓN'),
            ('SUMINISTROS REQUERIDOS - ESPECIFICACIONES', 'TÉCNICAS')
        ]
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page_number, page in enumerate(pdf.pages, start=1):  # Enumerar páginas
                texto_pagina = page.extract_text()
                if texto_pagina:  # Verificar si hay texto en la página
                    lineas = texto_pagina.split('\n')
                    i = 0  # Usamos un índice para iterar por las líneas
                    while i < len(lineas):
                        linea_limpia = self.limpiar_texto(lineas[i].strip())
                        
                        if i + 1 < len(lineas):
                            siguiente_linea = self.limpiar_texto(lineas[i + 1].strip())
                            
                            # Verificar si la combinación actual de líneas es una de las combinaciones conocidas
                            for parte1, parte2 in combinaciones_conocidas:
                                if linea_limpia == parte1 and siguiente_linea == parte2:
                                    linea_limpia += " " + siguiente_linea  # Combinamos las líneas
                                    i += 1  # Incrementamos el índice para evitar procesar la siguiente línea por separado
                                    break  # No es necesario seguir buscando combinaciones

                        if linea_limpia:  # Agregar la línea combinada
                            lineas_pdf.append((linea_limpia, page_number))
                        i += 1
        return lineas_pdf

    def extraer_outline_con_posiciones(self, initial_csv_path, final_csv_path):
        """
        Extrae el outline (tabla de contenido) y lo enriquece con posiciones
        """
        reader = PdfReader(self.pdf_path)
        outline = reader.outline  # Obtener la tabla de contenido
        
        if not outline:
            print("No se encontró tabla de contenido en el PDF.")
            return

        resultados_outline = []

        with pdfplumber.open(self.pdf_path) as pdf:
            for item in outline:
                if isinstance(item, list):  # Subniveles del outline
                    for subitem in item:
                        self.procesar_outline_item(reader, pdf, subitem, resultados_outline)
                else:
                    self.procesar_outline_item(reader, pdf, item, resultados_outline)

        # Guardar el archivo inicial con solo títulos y páginas
        df_inicial = pd.DataFrame(resultados_outline)
        df_inicial.to_csv(initial_csv_path, index=False)

        lineas_pdf = self.extraer_lineas_pdf()
        print(f"Se encontraron {len(lineas_pdf)} líneas en el PDF.")
        print(resultados_outline)

        for resultado in resultados_outline:
            titulo = resultado["Titulo"]
            resultado["Linea"], resultado["PaginaLinea"] = self.buscar_linea(lineas_pdf, titulo)

        # Guardar el archivo final con todas las columnas enriquecidas
        df_final = pd.DataFrame(resultados_outline)
        df_final.to_csv(final_csv_path, index=False)
        print(f"Resultados guardados en {final_csv_path}")

    def procesar_outline_item(self, reader, pdf, item, resultados_outline):
        """
        Procesa cada item del outline y agrega información al listado de resultados.
        """
        if isinstance(item, list):  # Verificar si el item es una lista
            for subitem in item:
                if hasattr(subitem, 'title') and hasattr(subitem, 'page'):
                    page_number = subitem.page if isinstance(subitem.page, int) else reader.get_destination_page_number(subitem) + 1
                    title = self.limpiar_texto(subitem.title)  # Limpiar el título
                    resultados_outline.append({
                        "Titulo": title,
                        "Pagina": page_number + 1,
                        "Linea": None,  # Línea será asignada después
                        "PaginaLinea": None  # Línea página será asignada luego
                    })
        elif hasattr(item, 'title') and hasattr(item, 'page'):  # Asegurarse de que item no es una lista
            page_number = item.page if isinstance(item.page, int) else reader.get_destination_page_number(item) + 1
            title = self.limpiar_texto(item.title)  # Limpiar el título
            resultados_outline.append({
                "Titulo": title,
                "Pagina": page_number + 1,
                "Linea": None,  # Línea será asignada después
                "PaginaLinea": None  # Línea página será asignada luego
            })


    def buscar_linea(self, lineas_pdf, titulo):
        """
        Busca la posición (número de línea) y la página de un título en el documento.
        """
        titulo_regex = re.escape(titulo)  # Sin convertir a minúsculas
        
        for line_number, (linea, pagina) in enumerate(lineas_pdf, start=1):
            if re.fullmatch(titulo_regex, linea.strip()):
                print(f"Línea encontrada para '{titulo}': Línea {line_number}, Página {pagina}")
                print(f"Texto de la línea: {linea}")
                return (line_number, pagina)
        
        return (None, None)  # Si no se encuentra el título, retornar (None, None)



