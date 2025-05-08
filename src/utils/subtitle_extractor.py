import re
from bs4 import BeautifulSoup
import pandas as pd

class SubtitleExtractor:
    def __init__(self, exclusion_list=None):
        """
        Inicializa el extractor de subtítulos con una lista de exclusión opcional.
        """
        self.exclusion_list = exclusion_list or []
        self.regex = r'^[A-ZÁÉÍÓÚ][a-záéíóúA-ZÁÉÍÓÚ\s\W]*$'  # Validar subtítulos que comienzan con mayúscula.

    def extract_subtitles(self, input_html, output_csv):
        """
        Extrae subtítulos desde un archivo HTML y los guarda en un archivo CSV.

        Args:
            input_html (str): Ruta del archivo HTML de entrada.
            output_csv (str): Ruta del archivo CSV de salida.
        """
        # Leer y parsear el contenido HTML
        with open(input_html, "r", encoding="utf-8") as file:
            html_content = file.read()
        soup = BeautifulSoup(html_content, "html.parser")

        # Extraer subtítulos de las etiquetas <h3>
        subtitles = []
        for h3 in soup.find_all("h3"):
            subtitle = h3.get_text(strip=True)
            if re.match(self.regex, subtitle) and subtitle not in self.exclusion_list:
                subtitles.append(subtitle)

        # Crear DataFrame y guardar en CSV
        df = pd.DataFrame(subtitles, columns=["subtitulo"])
        df.to_csv(output_csv, index=False, encoding="utf-8")

        return subtitles
