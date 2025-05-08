"""
# llm_studio_titles_type_secuencial.py
"""
from datetime import datetime
import requests
import re
import os
import pandas as pd


# Variables para los archivos de entrada y salida
INPUT_FILE = "./test-titles-content-1.csv"
OUTPUT_DIR = "./results"  # Directorio para los resultados
# El nombre del archivo de salida se generará dinámicamente con fecha y hora

# Prompt dinámico para la clasificación
def construir_prompt_llm(title, content=None):
    """ 
    Construir el prompt dinámico para la clasificación."""
    prompt = (
        "Clasifica el tipo de variable según su título y contenido. "
        "Responde EXCLUSIVAMENTE con un JSON válido en este formato: {\"tipo\": \"<valor>\"}\n"
        "Opciones: numérico, categórico, fecha, booleano, texto libre, identificador.\n\n"
        f"Título: {title}\n"
    )
    if content:
        prompt += f"Contenido: {content}\n"
    return prompt
# Clasificar de forma secuencial
def clasificar(url, title, content=None):
    """ 
    Clasificar un título y contenido de forma secuencial."""
    prompt = construir_prompt_llm(title, content)
    print(f"Procesando: {title}")
    print(f"Longitud del prompt: {len(prompt)} caracteres, ~{len(prompt)/4} tokens")
    
    payload = {
           "model": "default",
            "prompt": prompt,
            "temperature": 0.1,  # Reducir para menos variabilidad
            # "max_tokens": 30,    # Limitar longitud de respuesta
            # "stop": ["\n"],      # Detener generación al primer salto de línea
            "stream": False
    }
    
    try:
        response = requests.post(url, json=payload, timeout=60)
        
        if response.status_code != 200:
            print(f"Error API ({response.status_code}): {response.text}")
            return "desconocido", response.text
        
        response_json = response.json()
        raw = response_json.get("choices", [{}])[0].get("text", "")
        
        # Guardar la respuesta completa para la nueva columna
        llm_response_raw = raw
        
        # Usar expresión regular para extraer el JSON de la respuesta
        json_match = re.search(r'{"tipo"\s*:\s*"([^"]+)"}', raw)
        if json_match:
            tipo = json_match.group(1).lower()
            return tipo, llm_response_raw
        else:
            print(f"No se encontró formato JSON para '{title}'. Respuesta: {raw}")
            return "formato_no_encontrado", llm_response_raw
    except Exception as e:
        print(f"Error con '{title}': {e}")
        return "error_conexion", str(e)

def procesar_dataframe_secuencial(df, url):
    """ 
    Procesar el DataFrame de forma secuencial."""
    tipos_variables = []
    respuestas_llm = []  # Lista para almacenar las respuestas crudas
    
    for index, row in df.iterrows():
        title = row['title']
        content = row.get('content')
        
        # Clasificar el registro actual y obtener la respuesta cruda
        tipo, respuesta_cruda = clasificar(url, title, content)
        tipos_variables.append(tipo)
        respuestas_llm.append(respuesta_cruda)
        
        # Imprimir progreso cada 10 registros
        if (index + 1) % 10 == 0:
            print(f"Procesados {index + 1} de {len(df)} registros...")
    
    # Añadir los resultados al DataFrame
    df['tipo_variable'] = tipos_variables
    df['llm_response'] = respuestas_llm  # Añadir la nueva columna con respuestas crudas
    return df

def get_output_filename():
    """
    Genera un nombre de archivo con la fecha y hora actual.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"resultados_clasificacion_{timestamp}.csv"

def main():
    """ 
    Función principal."""
    # URL del endpoint de completions de LLM Studio
    url = "http://localhost:1234/v1/completions"
    
    # Crear directorio de resultados si no existe
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    # Generar nombre de archivo de salida con timestamp
    output_file = os.path.join(OUTPUT_DIR, get_output_filename())
    
    # Carga del DataFrame desde el archivo especificado
    try:
        print(f"[{datetime.now()}] Cargando datos desde {INPUT_FILE}...")
        df = pd.read_csv(INPUT_FILE)
        print(f"[{datetime.now()}] Datos cargados: {len(df)} registros")
    except Exception as e:
        print(f"[{datetime.now()}] Error al cargar el archivo {INPUT_FILE}: {e}")
        return None
    
    print(f"[{datetime.now()}] Iniciando procesamiento secuencial de {len(df)} registros...")
    
    # Procesar el DataFrame de forma secuencial
    resultado_df = procesar_dataframe_secuencial(df, url)
    
    # Guardar resultados
    print(f"[{datetime.now()}] Procesamiento completado")
    
    # Mostrar una muestra de los resultados
    print("\nMuestra de resultados:")
    print(resultado_df.head(50))
    
    # Guardar a CSV
    resultado_df.to_csv(output_file, index=False)
    print(f"[{datetime.now()}] Resultados guardados en {output_file}")
    
    return resultado_df

# Ejecutar el programa si se llama directamente
if __name__ == "__main__":
    try:
        resultado = main()
        print("\n¡Proceso completado exitosamente!")
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")