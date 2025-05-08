import pandas as pd

def process_categories(input_csv, output_csv):
    categorie_id = "compiledRelease/parties/0/details/categories/0/id"
    categorie = "compiledRelease/parties/0/details/categories/0/name"
    try:
        # Leer el archivo de entrada
        df = pd.read_csv(input_csv)
        
        # Seleccionar solo las columnas necesarias
        if categorie_id in df.columns and \
           categorie in df.columns:
            df_filtered = df[[
                categorie_id,
                categorie
            ]]
        else:
            raise ValueError("El archivo no contiene las columnas esperadas.")

        # Eliminar duplicados
        df_unique = df_filtered.drop_duplicates()

        # Ordenar por id de menor a mayor
        df_sorted = df_unique.sort_values(
            by=categorie_id
        )

        # Guardar el archivo procesado
        df_sorted.to_csv(output_csv, index=False)
        print(f"Archivo procesado y guardado en: {output_csv}")
    except Exception as e:
        print(f"Error al procesar las categorías: {e}")

# Ejemplo de uso
input_csv = "./inputs/par_det_categories.csv"
output_csv = "./outputs/unique_categories_sorted.csv"
process_categories(input_csv, output_csv)
