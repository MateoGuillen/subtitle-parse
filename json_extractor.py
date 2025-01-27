import json
import csv

class JSONExtractor:
    def __init__(self, json_path):
        self.json_path = json_path
        self.data = None

    def load_json(self):
        """Carga el archivo JSON desde la ruta especificada."""
        with open(self.json_path, 'r', encoding='utf-8') as file:
            self.data = json.load(file)

    def extract_clauses(self):
        """Extrae las cláusulas y sus parámetros relevantes."""
        clauses = []
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
        return clauses

    def save_to_csv(self, clauses):
        """Guarda los datos extraídos en un archivo CSV."""
        convocatoria_slug = self.data.get("convocatoria_slug")
        pliego_version = self.data.get("pliego_version")
        output_filename = f"{convocatoria_slug}_{pliego_version}.csv"

        with open(output_filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=[
                "titulo", "pre_informacion", "cuerpo", "post_informacion", 
                "ayuda", "editable", "no_aplica", "opcional"
            ])
            writer.writeheader()
            writer.writerows(clauses)

        print(f"CSV generado: {output_filename}")
