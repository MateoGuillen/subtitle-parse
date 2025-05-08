""" 
# llm_studio_titles_type_secuencial.py"""
from datetime import datetime
import json
import requests
import pandas as pd
import re

# Variables para los archivos de entrada y salida
INPUT_FILE = "./test-titles-content-1.csv"
OUTPUT_FILE = "resultados_clasificacion.csv"

# Prompt dinámico para la clasificación
def construir_prompt_llm(title, content=None):
    """ 
    Construir el prompt dinámico para la clasificación."""
    prompt = (
        "Tu tarea es clasificar el tipo de variable de un campo, según su título y contenido.\n"
        "Responde solo con el tipo, en formato JSON válido: {\"tipo\": \"<valor>\"}\n\n"
        "Tipos posibles: numérico, categórico, fecha, booleano, texto libre, identificador.\n\n"
    )
    prompt += f"Título: {title}\n Contenido: {content}\n"
    return prompt

# Clasificar de forma secuencial
import re

def clasificar(url, title, content=None):
    """ 
    Clasificar un título y contenido de forma secuencial."""
    prompt = construir_prompt_llm(title, content)
    print(f"Procesando: {title}")
    print(f"Longitud del prompt: {len(prompt)} caracteres, ~{len(prompt)/4} tokens")
    
    payload = {
        "model": "default",
        "prompt": prompt,
        "temperature": 0.2,
        "stream": False
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code != 200:
            print(f"Error API ({response.status_code}): {response.text}")
            return "desconocido"

        response_json = response.json()
        raw = response_json.get("choices", [{}])[0].get("text", "")
        
        # Usar expresión regular para extraer el JSON de la respuesta
        json_match = re.search(r'{"tipo"\s*:\s*"([^"]+)"}', raw)
        if json_match:
            tipo = json_match.group(1).lower()
            return tipo
        else:
            print(f"No se encontró formato JSON para '{title}'. Respuesta: {raw}")
            return "formato_no_encontrado"
    except Exception as e:
        print(f"Error con '{title}': {e}")
        return "error_conexion"

def procesar_dataframe_secuencial(df, url):
    """ 
    Procesar el DataFrame de forma secuencial."""
    tipos_variables = []
    
    for index, row in df.iterrows():
        title = row['title']
        content = row.get('content')
        
        # Clasificar el registro actual
        tipo = clasificar(url, title, content)
        tipos_variables.append(tipo)
        
        # Imprimir progreso cada 10 registros
        if (index + 1) % 10 == 0:
            print(f"Procesados {index + 1} de {len(df)} registros...")
    
    # Añadir los resultados al DataFrame
    df['tipo_variable'] = tipos_variables
    return df

def main():
    """ 
    Función principal."""
    # URL del endpoint de completions de LLM Studio
    url = "http://localhost:1234/v1/completions"
    
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
    resultado_df.to_csv(OUTPUT_FILE, index=False)
    print(f"[{datetime.now()}] Resultados guardados en {OUTPUT_FILE}")
    
    return resultado_df

# Ejecutar el programa si se llama directamente
if __name__ == "__main__":
    try:
        resultado = main()
        print("\n¡Proceso completado exitosamente!")
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")