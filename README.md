# SubtitleParserPBC

Este proyecto permite extraer subtitulos de pliegos de bases y condiciones (pbc) en formato PDF de la Dncp (Dirección Nacional de Contrataciones Públicas del Paraguay).

La entrada un .pdf (pliego de bases y condiciones)

La salida un .csv (con los subtitulos del pdf)

---

## **Requisitos**

- Python 3.12 o superior.
- Librerías incluidas en `requirements.txt`.

---

## **Instalación**

Sigue los pasos a continuación para configurar el entorno y ejecutar el proyecto:

### 1. **Clonación de Proyecto**

```bash
git clone https://github.com/mateoguillen/subtitle-parse
```

### 2. **Creación del entorno virtual**

Crea un entorno virtual para evitar conflictos con otras dependencias del sistema:

```bash
python -m venv venv
```

### 3. **Activación del entorno virtual**

Activa el entorno virtual creado:

Windows

```bash
.\venv\Scripts\activate
```

Linux

```bash
source venv/bin/activate
```

### 4. **Instalación de dependencias**

Instala las dependencias necesarias utilizando el archivo requirements.txt:

```bash
pip install -r requirements.txt
```

### 5. **Ejecución**

Sigue estos pasos para convertir archivos:

**Archivos de entrada**

Coloca los archivos PDF o HTML en la carpeta inputs/.

**Ejecutar el script principal**

Corre el archivo main.py para procesar los archivos:

```bash
python main.py
```

**Archivos de salida**

Los archivos convertidos se guardarán en la carpeta outputs/.
