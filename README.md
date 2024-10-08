
# Proyecto Elektra

Este proyecto está diseñado para integrar funcionalidades de gestión de documentos en IBM Watson Discovery y extracción de información de archivos PDF. El sistema sube archivos Excel a una colección de IBM Watson Discovery, permite eliminar documentos y extraer datos de archivos PDF.

## Estructura del Proyecto

```plaintext
proyecto_elektra/
│
├── archivos_dummy/            # Carpeta con archivos de prueba
│   ├── archivo_nuevo1.csv     # Ejemplo de archivo CSV
│   └── archivo_nuevo2.csv     # Ejemplo de archivo CSV
│
├── data/                      # Carpeta para almacenar PDFs y otros archivos
│   └── pdf.pdf                # Ejemplo de archivo PDF
│
├── funciones/                 # Módulo con funciones específicas
│   ├── __init__.py            # Archivo para definir el módulo como paquete
│   ├── pdf_extractor.py       # Funciones para extracción de datos desde PDFs
│   └── watson_discovery.py    # Funciones para interactuar con Watson Discovery
│
├── .env                       # Archivo de variables de entorno (no subir a git)
├── .gitignore                 # Archivo para ignorar archivos en el control de versiones
├── main.py                    # Archivo principal para ejecutar el proyecto
├── README.md                  # Este archivo de documentación
└── requirements.txt           # Archivo para instalar dependencias
```

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu_usuario/proyecto_elektra.git
cd proyecto_elektra
```

### 2. Crear un entorno virtual

Es recomendable usar un entorno virtual para aislar las dependencias:

```bash
python -m venv venv
source venv/bin/activate  # Para Linux/macOS
# venv\Scripts\activate   # Para Windows
```

### 3. Instalar dependencias

Instala las dependencias del proyecto utilizando `pip`:

```bash
pip install -r requirements.txt
```

## Uso

### 1. Configurar credenciales para Watson Discovery

Antes de ejecutar el proyecto, asegúrate de haber configurado las credenciales de IBM Watson Discovery en el archivo `.env`:

- **API_KEY**: Llave API de IBM Watson Discovery.
- **SERVICE_URL**: URL del servicio.
- **PROJECT_ID**: ID del proyecto en Watson Discovery.
- **COLLECTION_ID**: ID de la colección en Watson Discovery.

### 2. Ejecutar el proyecto

Para ejecutar el proyecto principal, usa el siguiente comando:

```bash
python main.py
```

Este archivo principal realiza las siguientes acciones:

1. **Eliminar documentos de una colección**: Elimina todos los documentos existentes en la colección configurada de IBM Watson Discovery.
2. **Subir archivos Excel**: Sube todos los archivos `.csv` encontrados en la carpeta especificada a la colección de Watson Discovery.
3. **Procesar archivos PDF**: Extrae información de un archivo PDF y lo procesa para obtener datos como SKUs y vigencias.

## Dependencias

Las principales dependencias de este proyecto son:

- `ibm-watson`: SDK oficial de IBM para interactuar con Watson Discovery.
- `requests`: Librería para hacer peticiones HTTP.
- `PyMuPDF (fitz)`: Librería para manipular archivos PDF.

Estas dependencias están listadas en el archivo `requirements.txt` y pueden instalarse utilizando:

```bash
pip install -r requirements.txt
```

## Estructura de los módulos

### `watson_discovery.py`

Este módulo contiene funciones para interactuar con IBM Watson Discovery. Las principales funciones son:

- `eliminar_documentos()`: Elimina todos los documentos de una colección.
- `añadir_documento()`: Añade un documento (archivo) a la colección de Watson Discovery.
- `obtener_estado_documento()`: Consulta el estado de un documento en la colección.
- `subir_archivos_de_carpeta()`: Sube todos los archivos CSV de una carpeta a la colección.

### `pdf_extractor.py`

Este módulo contiene funciones para extraer información de un archivo PDF. Las principales funciones son:

- `extraer_informacion()`: Extrae texto de un PDF y busca patrones como SKU y vigencias.
- `guardar_en_csv()`: Guarda los datos extraídos en un archivo CSV.

## .gitignore

Este proyecto incluye un archivo `.gitignore` para evitar subir al repositorio archivos innecesarios como:
- Entornos virtuales
- Archivos de entorno (`.env`)
- Archivos generados por el sistema o editores de texto

## Licencia

Este proyecto está licenciado bajo la [Licencia MIT](LICENSE).
