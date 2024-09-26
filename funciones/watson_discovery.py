import os
import mimetypes
import re
from ibm_watson import DiscoveryV2
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from dotenv import load_dotenv
import concurrent.futures

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Definición de credenciales desde variables de entorno
api_key = os.getenv('API_KEY')
service_url = os.getenv('SERVICE_URL')
project_id = os.getenv('PROJECT_ID')
collection_id = os.getenv('COLLECTION_ID')

# Autenticador para IBM Watson Discovery
authenticator = IAMAuthenticator(api_key)
discovery = DiscoveryV2(
    version='2023-06-15',
    authenticator=authenticator
)
discovery.set_service_url(service_url)

# Función para sanitizar nombre de archivo
def sanitizar_nombre(nombre_archivo):
    # Reemplazar cualquier letra seguida de '?' por esa letra entre corchetes
    nombre_archivo = re.sub(r'(\w)\?', r'[\1]', nombre_archivo)
    return nombre_archivo

# Función para eliminar un documento individual
def eliminar_documento(doc_id):
    try:
        discovery.delete_document(
            project_id=project_id,
            collection_id=collection_id,
            document_id=doc_id
        ).get_result()
        print(f"Documento con ID {doc_id} eliminado exitosamente.")
    except Exception as e:
        print(f"Error al eliminar documento con ID {doc_id}: {str(e)}")

# Función para eliminar todos los documentos de una colección en paralelo
def eliminar_documentos():
    page_limit = 100
    offset = 0
    total_documents = 1

    while offset < total_documents:
        query_response = discovery.query(
            project_id=project_id,
            collection_ids=[collection_id],
            count=page_limit,
            offset=offset
        ).get_result()

        total_documents = query_response.get('matching_results', 0)
        
        if 'results' in query_response:
            document_ids = [doc['document_id'] for doc in query_response['results']]

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(eliminar_documento, doc_id) for doc_id in document_ids]
                for future in concurrent.futures.as_completed(futures):
                    future.result()

        offset += page_limit

# Función para añadir un documento a la colección
def añadir_documento(ruta_archivo, nombre_archivo, tipo_contenido):
    nombre_archivo_sanitizado = sanitizar_nombre(nombre_archivo)

    try:
        with open(ruta_archivo, 'rb') as file:
            response = discovery.add_document(
                project_id=project_id,
                collection_id=collection_id,
                file=file,
                filename=nombre_archivo_sanitizado, 
                file_content_type=tipo_contenido
            ).get_result()
            print(f"Documento subido: {response}")
            return response['document_id']
    except Exception as e:
        print(f"Error al subir {nombre_archivo}: {str(e)}")
        return None

# Función para obtener el estado de un documento
def obtener_estado_documento(document_id):
    try:
        document_status = discovery.get_document(
            project_id=project_id,
            collection_id=collection_id,
            document_id=document_id
        ).get_result()
        print(f"Estado del documento con ID {document_id}: {document_status}")
        return document_status
    except Exception as e:
        print(f"Error al obtener estado del documento {document_id}: {str(e)}")

# Función para procesar archivos en paralelo
def subir_archivo_en_paralelo(ruta_archivo, archivo):
    tipo_contenido, _ = mimetypes.guess_type(ruta_archivo)
    if tipo_contenido is None:
        tipo_contenido = 'application/octet-stream'
    
    print(f"Subiendo {archivo} con tipo de contenido {tipo_contenido}...")
    
    document_id = añadir_documento(ruta_archivo, archivo, tipo_contenido)
    
    if document_id:
        obtener_estado_documento(document_id)

# Función para procesar todos los archivos en una carpeta
def subir_archivos_de_carpeta(carpeta):
    archivos = os.listdir(carpeta)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for archivo in archivos:
            ruta_archivo = os.path.join(carpeta, archivo)
            futures.append(executor.submit(subir_archivo_en_paralelo, ruta_archivo, archivo))
        
        for future in concurrent.futures.as_completed(futures):
            future.result()
