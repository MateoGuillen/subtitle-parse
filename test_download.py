import requests
from io import StringIO
import pandas as pd

def test_single_download(nro_licitacion):
    url = f"https://www.contrataciones.gov.py/buscador/licitaciones.csv?nro_nombre_licitacion={nro_licitacion}"
    print(f"Intentando descargar desde: {url}")
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            csv_data = StringIO(response.content.decode("utf-8"))
            df = pd.read_csv(csv_data)
            print(df.head())  
        else:
            print(f"Error en la descarga. Código de estado: {response.status_code}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

test_single_download("403005")
