# llm_studio_titles_type_async.py

from datetime import datetime
import re
import os
import asyncio
import aiohttp
import pandas as pd


# Variables para los archivos de entrada y salida
INPUT_FILE = "./test-titles-content-1.csv"
OUTPUT_DIR = "./results"  # Directorio para los resultados
# El nombre del archivo de salida se generará dinámicamente con fecha y hora
MAX_CONCURRENT_REQUESTS = 10  # Limitar el número de conexiones concurrentes

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

# Clasificar de forma asíncrona
async def clasificar_async(session, url, title, content=None):
    """ 
    Clasificar un título y contenido de forma asíncrona."""
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
        async with session.post(url, json=payload, timeout=60) as response:
            if response.status != 200:
                error_text = await response.text()
                print(f"Error API ({response.status}): {error_text}")
                return "desconocido", error_text
            
            response_json = await response.json()
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
    except asyncio.TimeoutError:
        print(f"Timeout al procesar '{title}'")
        return "timeout", "Timeout durante la conexión"
    except Exception as e:
        print(f"Error con '{title}': {e}")
        return "error_conexion", str(e)

async def procesar_dataframe_async(df, url):
    """ 
    Procesar el DataFrame de forma asíncrona."""
    tipos_variables = [None] * len(df)
    respuestas_llm = [None] * len(df)
    
    # Crear un semáforo para limitar el número de conexiones concurrentes
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async def procesar_fila(index, row):
        # Usar el semáforo para limitar conexiones concurrentes
        async with semaphore:
            title = row['title']
            content = row.get('content')
            
            # Clasificar el registro actual y obtener la respuesta cruda
            tipo, respuesta_cruda = await clasificar_async(session, url, title, content)
            tipos_variables[index] = tipo
            respuestas_llm[index] = respuesta_cruda
            
            # Imprimir progreso cada 10 registros
            if (index + 1) % 10 == 0:
                print(f"Procesados {index + 1} de {len(df)} registros...")
    
    # Usar TCPConnector con límite de conexiones
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    
    # Crear una sesión HTTP asíncrona
    async with aiohttp.ClientSession(connector=connector) as session:
        # Crear tareas para cada fila
        tasks = [procesar_fila(i, row) for i, row in df.iterrows()]
        
        # Ejecutar todas las tareas concurrentemente
        await asyncio.gather(*tasks)
    
    # Añadir los resultados al DataFrame
    df['tipo_variable'] = tipos_variables
    df['llm_response'] = respuestas_llm
    return df

def get_output_filename():
    """
    Genera un nombre de archivo con la fecha y hora actual.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"resultados_clasificacion_{timestamp}.csv"

async def main_async():
    """ 
    Función principal asíncrona."""
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
    
    print(f"[{datetime.now()}] Iniciando procesamiento asíncrono de {len(df)} registros...")
    
    # Procesar el DataFrame de forma asíncrona
    resultado_df = await procesar_dataframe_async(df, url)
    
    # Guardar resultados
    print(f"[{datetime.now()}] Procesamiento completado")
    
    # Mostrar una muestra de los resultados
    print("\nMuestra de resultados:")
    print(resultado_df.head(50))
    
    # Guardar a CSV
    resultado_df.to_csv(output_file, index=False)
    print(f"[{datetime.now()}] Resultados guardados en {output_file}")
    
    return resultado_df

def main():
    """
    Función principal que ejecuta el código asíncrono.
    """
    try:
        resultado = asyncio.run(main_async())
        print("\n¡Proceso completado exitosamente!")
        return resultado
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")
        return None

# Ejecutar el programa si se llama directamente
if __name__ == "__main__":
    main()