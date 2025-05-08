""" 
# llm_studio_titles_type_asyncio.py"""
from datetime import datetime
import json
import asyncio
import aiohttp
import pandas as pd
import re
from typing import List, Dict, Any, Optional, Tuple

# Variables para los archivos de entrada y salida
INPUT_FILE = "./test-titles-content-1.csv"
OUTPUT_FILE = "resultados_clasificacion.csv"
# Número máximo de solicitudes concurrentes
MAX_CONCURRENCY = 10

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

# Clasificar de forma asíncrona
async def clasificar_async(session, url, title, content=None, index=None):
    """ 
    Clasificar un título y contenido de forma asíncrona."""
    prompt = construir_prompt_llm(title, content)
    if index is not None:
        print(f"Procesando [{index}]: {title}")
    else:
        print(f"Procesando: {title}")
    
    print(f"Longitud del prompt: {len(prompt)} caracteres, ~{len(prompt)/4} tokens")
    
    payload = {
        "model": "default",
        "prompt": prompt,
        "temperature": 0.2,
        "stream": False
    }
    
    try:
        async with session.post(url, json=payload,timeout=120) as response:
            if response.status != 200:
                print(f"Error API ({response.status}): {await response.text()}")
                return (index, "desconocido")
            
            response_json = await response.json()
            raw = response_json.get("choices", [{}])[0].get("text", "")
            
            # Usar expresión regular para extraer el JSON de la respuesta
            json_match = re.search(r'{"tipo"\s*:\s*"([^"]+)"}', raw)
            if json_match:
                tipo = json_match.group(1).lower()
                return (index, tipo)
            else:
                print(f"No se encontró formato JSON para '{title}'. Respuesta: {raw}")
                return (index, "formato_no_encontrado")
    except Exception as e:
        print(f"Error con '{title}': {e}")
        return (index, "error_conexion")

async def procesar_lote_async(session, url, registros: List[Dict[str, Any]]):
    """
    Procesar un lote de registros de forma asíncrona.
    """
    tareas = []
    for i, registro in enumerate(registros):
        tarea = clasificar_async(
            session=session,
            url=url,
            title=registro['title'],
            content=registro.get('content'),
            index=registro['index_original']
        )
        tareas.append(tarea)
    
    return await asyncio.gather(*tareas)

async def procesar_dataframe_async(df, url):
    """ 
    Procesar el DataFrame de forma asíncrona con control de concurrencia."""
    # Preparar una lista de diccionarios con los datos y su índice original
    registros = []
    for i, (_, row) in enumerate(df.iterrows()):
        registros.append({
            'title': row['title'],
            'content': row.get('content'),
            'index_original': i
        })
    
    # Inicializar lista para almacenar resultados
    resultados = [None] * len(df)
    
    # Crear sesión HTTP asíncrona
    async with aiohttp.ClientSession() as session:
        # Procesar en lotes para controlar la concurrencia
        for i in range(0, len(registros), MAX_CONCURRENCY):
            lote_actual = registros[i:i + MAX_CONCURRENCY]
            print(f"Procesando lote {i//MAX_CONCURRENCY + 1} ({i}-{min(i+MAX_CONCURRENCY, len(registros))})")
            
            resultados_lote = await procesar_lote_async(session, url, lote_actual)
            
            # Almacenar resultados en la posición correcta
            for index, tipo in resultados_lote:
                resultados[index] = tipo
            
            # Mostrar progreso
            print(f"Completados {min(i + MAX_CONCURRENCY, len(registros))} de {len(registros)} registros...")
    
    # Añadir los resultados al DataFrame
    df['tipo_variable'] = resultados
    return df

async def main_async():
    """ 
    Función principal asíncrona."""
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
    
    print(f"[{datetime.now()}] Iniciando procesamiento asíncrono de {len(df)} registros (concurrencia: {MAX_CONCURRENCY})...")
    
    # Procesar el DataFrame de forma asíncrona
    resultado_df = await procesar_dataframe_async(df, url)
    
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
        resultado = asyncio.run(main_async())
        print("\n¡Proceso completado exitosamente!")
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")