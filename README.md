# ETL Deteccion de anomalias en Contrataciones Públicas usando los Pliegos de Bases y Condiciones del DNCP

## 📂 Estructura de Directorios

```
subtitle-parse/
├── config/         # Configuración del proyecto
├── data/           # Datos del sistema
│   ├── external/   # Fuentes externas
│   ├── processed/  # Resultados procesados
│   └── raw/        # Datos sin procesar
├── docker/         # Configuración Docker
├── scripts/        # Puntos de entrada
└── src/            # Código principal
    ├── core/       # Componentes base
    ├── etl/        # Procesamiento ETL
    ├── pipelines/  # Flujos completos
    ├── tasks/      # Scripts específicos
    └── utils/      # Utilidades
```

## 🎯 Finalidad de los Directorios

- **config/**: Parámetros globales y configuración de entorno.
- **data/**: Organización de datos:
  - **external/**: Datos descargados de terceros.
  - **processed/**: Datos finales, ya procesados.
  - **raw/**: Datos originales sin procesar.
- **docker/**: Configuración para contenedores Docker.
- **scripts/**: Scripts de entrada para ejecutar pipelines.
- **src/**:
  - **core/**: Entidades, modelos y manejo de errores.
  - **etl/**: Módulos para extraer, transformar y cargar datos.
  - **pipelines/**: Orquestación de procesos completos.
  - **tasks/**: Scripts de procesamiento específicos.
  - **utils/**: Funciones auxiliares y manejo de archivos.

## 🛠️ Cómo Ejecutar

### Requisitos

- Python 3.8+
- Docker (opcional)

### Instalación

```bash
# Crear entorno virtual (opcional pero recomendado)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate   # Windows

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
cp example.env .env
# Editar .env con los valores correspondientes
```

### Ejecución del pipeline principal

```bash
python -m scripts/run_ocds_pipeline.py
```

### Ejecutar tareas específicas

```bash
python -m src.tasks.nombre_del_script
```

### Docker (opcional)

```bash
docker build -t etl-project -f docker/Dockerfile .
docker run etl-project
```

### Project wiki

https://deepwiki.com/MateoGuillen/subtitle-parse

---
