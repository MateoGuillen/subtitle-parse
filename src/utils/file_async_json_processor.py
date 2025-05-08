import json
import aiofiles
import os
import csv
from io import StringIO
import os

class JSONExtractor:
    def __init__(self, json_path):
        self.json_path = json_path
        self.data = None

    async def load_json(self):
        """Carga el archivo JSON de manera asíncrona."""
        try:
            async with aiofiles.open(self.json_path, 'r', encoding='utf-8') as file:
                content = await file.read()
                self.data = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Error en el archivo {self.json_path}: JSON mal formado - {e}")
            raise
        except Exception as e:
            print(f"Error inesperado al cargar {self.json_path}: {e}")
            raise

    def extract_clauses(self):
        """Extrae las cláusulas y sus parámetros relevantes."""
        if not self.data:
            raise ValueError("Datos no cargados. Ejecute load_json primero.")
            
        clauses = []
        try:
            plantilla = self.data.get("plantilla", {})
            for section in plantilla.values():
                clausulas = section.get("clausulas", {})
                for clause in clausulas.values():
                    clauses.append({
                        "titulo": clause.get("titulo"),
                        "pre_informacion": clause.get("pre_informacion"),
                        "cuerpo": clause.get("cuerpo"),
                        "post_informacion": clause.get("post_informacion"),
                        "ayuda": clause.get("ayuda"),
                        "editable": clause.get("editable", False),
                        "no_aplica": clause.get("no_aplica", False),
                        "opcional": clause.get("opcional", False),
                    })
        except Exception as e:
            print(f"Error al extraer cláusulas de {self.json_path}: {e}")
            raise
        return clauses

    async def save_to_csv(self, clauses, output_path):
        """
        Guarda los datos extraídos en un archivo CSV de manera asíncrona.
        
        Args:
            clauses (list): Lista de cláusulas a guardar
            output_path (str): Ruta del directorio donde se guardará el CSV
        """
        if not clauses:
            raise ValueError("No hay cláusulas para guardar")
            
        try:
            # Obtener los datos necesarios para el nombre del archivo
            convocatoria_slug = self.data.get("convocatoria_slug")
            pliego_version = self.data.get("pliego_version")
            
            # Crear el nombre del archivo
            output_filename = f"{convocatoria_slug}_{pliego_version}.csv"
            
            # Combinar la ruta del directorio con el nombre del archivo
            full_path = os.path.join(output_path, output_filename)
            
            # Asegurar que el directorio existe
            os.makedirs(output_path, exist_ok=True)
            
            fieldnames = ["titulo", "pre_informacion", "cuerpo", "post_informacion",
                        "ayuda", "editable", "no_aplica", "opcional"]
            
            # Primero creamos el contenido del CSV en memoria usando StringIO y csv.DictWriter
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(clauses)
            csv_content = output.getvalue()
            output.close()
            
            # Luego escribimos el contenido al archivo de manera asíncrona
            async with aiofiles.open(full_path, mode='w', newline='', encoding='utf-8') as file:
                await file.write(csv_content)
                    
            print(f"CSV generado: {full_path}")
        except Exception as e:
            print(f"Error al guardar CSV para {self.json_path}: {e}")
            raise