""" 
# llm_studio_structured_output_json_schema.py"""
from datetime import datetime
import os
import json
import asyncio
import aiohttp
import pandas as pd

# Configuración
INPUT_FILE = "./test-titles-content-1.csv"
OUTPUT_DIR = "./results"
MAX_CONCURRENT_REQUESTS = 10

# Schema JSON según especificación de LLM Studio
RESPONSE_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "variable_classification",
        "strict": "true",
        "schema": {
            "type": "object",
            "properties": {
                "tipo": {
                    "type": "string",
                    "enum": ["numérico", "categórico", "fecha", "booleano", "texto libre", "identificador"]
                },
                "confianza": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1
                },
                "explicacion": {
                    "type": "string"#,
                    #"maxLength": 100
                }
            },
            "required": ["tipo"]
        }
    }
}

async def clasificar_async(session, url, title, content=None):
    """Clasifica un título y contenido usando structured output de LLM Studio"""
    messages = [
        {
            "role": "system",
            "content": (
                "Clasifica el tipo de variable según su título y contenido. "
                "Responde EXCLUSIVAMENTE con un JSON válido que siga el schema proporcionado. "
                "Ejemplo válido: {\"tipo\": \"fecha\", \"confianza\": 0.95, \"explicacion\": \"Formato YYYY-MM-DD\"}"
            )
        },
        {
            "role": "user",
            "content": f"Título: {title}\nContenido: {content if content else 'N/A'}"
        }
    ]

    payload = {
        "model": "default",
        "messages": messages,
        "temperature": 0.1,
        "response_format": RESPONSE_SCHEMA,  # Usamos el schema completo según doc LLM Studio
        "max_tokens": 150,
        "stream": False
    }

    try:
        async with session.post(url, json=payload, timeout=60) as response:
            if response.status != 200:
                error_text = await response.text()
                print(f"Error API ({response.status}): {error_text}")
                return "error_api", error_text

            response_json = await response.json()
            raw_response = response_json["choices"][0]["message"]["content"]

            try:
                json_response = json.loads(raw_response)
                return json_response["tipo"], raw_response
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error procesando JSON para '{title}': {e}")
                return "error_json", raw_response

    except asyncio.TimeoutError:
        print(f"Timeout al procesar '{title}'")
        return "timeout", "Timeout durante la conexión"
    except Exception as e:
        print(f"Error con '{title}': {e}")
        return "error_conexion", str(e)

async def procesar_dataframe_async(df, url):
    """Procesa el DataFrame de forma asíncrona"""
    tipos_variables = [None] * len(df)
    respuestas_llm = [None] * len(df)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def procesar_fila(index, row):
        async with semaphore:
            title = row['title']
            content = row.get('content')

            tipo, respuesta_cruda = await clasificar_async(session, url, title, content)
            tipos_variables[index] = tipo
            respuestas_llm[index] = respuesta_cruda

            if (index + 1) % 10 == 0:
                print(f"Procesados {index + 1} de {len(df)} registros...")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [procesar_fila(i, row) for i, row in df.iterrows()]
        await asyncio.gather(*tasks)

    df['tipo_variable'] = tipos_variables
    df['llm_response'] = respuestas_llm
    return df

def get_output_filename():
    """Genera un nombre de archivo con la fecha y hora actual."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"resultados_clasificacion_{timestamp}.csv"

async def main_async():
    """Función principal asíncrona"""
    url = "http://localhost:1234/v1/chat/completions"
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    output_file = os.path.join(OUTPUT_DIR, get_output_filename())
    
    try:
        print(f"[{datetime.now()}] Cargando datos desde {INPUT_FILE}...")
        df = pd.read_csv(INPUT_FILE)
        print(f"[{datetime.now()}] Datos cargados: {len(df)} registros")
    except Exception as e:
        print(f"[{datetime.now()}] Error al cargar el archivo {INPUT_FILE}: {e}")
        return None
    
    print(f"[{datetime.now()}] Iniciando procesamiento asíncrono...")
    resultado_df = await procesar_dataframe_async(df, url)
    
    print(f"[{datetime.now()}] Procesamiento completado")
    print("\nMuestra de resultados:")
    print(resultado_df.head(50))
    
    resultado_df.to_csv(output_file, index=False)
    print(f"[{datetime.now()}] Resultados guardados en {output_file}")
    
    return resultado_df

def main():
    """Función principal que ejecuta el código asíncrono."""
    try:
        resultado = asyncio.run(main_async())
        print("\n¡Proceso completado exitosamente!")
        return resultado
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")
        return None

if __name__ == "__main__":
    main()