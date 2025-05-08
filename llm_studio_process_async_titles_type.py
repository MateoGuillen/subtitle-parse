""" 
# llm_studio_titles_type.py"""
from datetime import datetime
import json
import asyncio
import aiohttp
import pandas as pd

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

# Async: llamada a la API local de LLM Studio
async def clasificar_async(session, url, title, content=None):
    """ 
    Clasificar un título y contenido asíncronamente."""
    prompt = construir_prompt_llm(title, content)
    print(prompt)
    print(len(prompt))
    print(len(prompt)/4)
    payload = {
        "model": "default",  # LLM Studio normalmente usa 'default' para el modelo cargado
        "prompt": prompt,
        "temperature": 0.2,
        #"max_tokens": 100,
        "stream": False
    }
    try:
        async with session.post(url, json=payload, timeout=45) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                print(f"Error API ({resp.status}): {error_text}")
                return "desconocido"

            response = await resp.json()
            # El formato de respuesta para /v1/completions
            raw = response.get("choices", [{}])[0].get("text", "")
            try:
                tipo = json.loads(raw.strip())["tipo"].lower()
                return tipo
            except json.JSONDecodeError:
                print(f"Error al parsear JSON para '{title}': {raw}")
                return "formato_invalido"
    except Exception as e:
        print(f"Error con '{title}': {e}")
        return "error_conexion"

# Ejecutar todo en paralelo
async def procesar_dataframe_async(df, url, max_concurrent=5):
    """ 
    Procesar el DataFrame de forma asiatcrona."""
    connector = aiohttp.TCPConnector(limit=max_concurrent)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            clasificar_async(session, url, row['title'], row.get('content'))
            for _, row in df.iterrows()
        ]
        resultados = await asyncio.gather(*tasks)
        df['tipo_variable'] = resultados
        return df

async def main_async():
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
    
    # Configura el número máximo de conexiones concurrentes
    max_concurrent = 5 # Ajusta según las capacidades de tu modelo local
    
    print(f"[{datetime.now()}] Iniciando procesamiento de {len(df)} registros...")
    
    # Procesar el DataFrame de forma asíncrona
    resultado_df = await procesar_dataframe_async(df, url, max_concurrent)
    
    # Guardar resultados
    print(f"[{datetime.now()}] Procesamiento completado")
    
    # Mostrar una muestra de los resultados
    print("\nMuestra de resultados:")
    print(resultado_df.head(50))
    
    # Guardar a CSV
    resultado_df.to_csv(OUTPUT_FILE, index=False)
    print(f"[{datetime.now()}] Resultados guardados en {OUTPUT_FILE}")
    
    return resultado_df

def main():
    """ 
    Función principal."""
    # Ejecutar el código asíncrono
    return asyncio.run(main_async())

# Ejecutar el programa si se llama directamente
if __name__ == "__main__":
    try:
        resultado = main()
        print("\n¡Proceso completado exitosamente!")
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")