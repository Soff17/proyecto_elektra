import os
import mimetypes
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

# Función para eliminar un documento individual
def eliminar_documento(doc_id):
    # Consulta para eliminar un documento
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
    # Paginar la consulta para obtener todos los documentos
    page_limit = 50
    offset = 0 # Definición del punto de inicio para obtener resultados
    total_documents = 1

    while offset < total_documents:
        # Consulta para obtener documentos con paginación
        query_response = discovery.query(
            project_id=project_id,
            collection_ids=[collection_id],
            count=page_limit,
            offset=offset
        ).get_result()

        # Actualizar el total de documentos
        total_documents = query_response.get('matching_results', 0)
        
        # Verifica si hay documentos en la colección
        if 'results' in query_response:
            # Lista de IDs de documentos a eliminar
            document_ids = [doc['document_id'] for doc in query_response['results']]

            # Eliminar los documentos en paralelo
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(eliminar_documento, doc_id) for doc_id in document_ids]
                for future in concurrent.futures.as_completed(futures):
                    future.result()  # Procesa los resultados de las tareas

        offset += page_limit

# Función para añadir un documento a la colección
def añadir_documento(ruta_archivo, nombre_archivo, tipo_contenido):
    # Abrir el archivo con rb para archivos no texto.
    with open(ruta_archivo, 'rb') as file:
        # Consulta para añadir documento a la colección
        response = discovery.add_document(
            project_id=project_id,
            collection_id=collection_id,
            file=file,
            filename=nombre_archivo, 
            file_content_type=tipo_contenido
        ).get_result()
        print(f"Documento subido: {response}")
        return response['document_id']

# Función para obtener el estado de un documento
def obtener_estado_documento(document_id):
    # Consultar para obtener la información del documento
    document_status = discovery.get_document(
        project_id=project_id,
        collection_id=collection_id,
        document_id=document_id
    ).get_result()
    print(f"Estado del documento con ID {document_id}: {document_status}")
    return document_status


# Función para procesar todos los archivos en una carpeta
def subir_archivos_de_carpeta(carpeta):
    # Recorre todos los archivos en la carpeta.
    for archivo in os.listdir(carpeta):
        # Se obtiene ruta completa
        ruta_archivo = os.path.join(carpeta, archivo)
        # Se establece tipo de contenido
        tipo_contenido, _ = mimetypes.guess_type(ruta_archivo)
        if tipo_contenido is None:
            tipo_contenido = 'application/octet-stream'  # Tipo por defecto si no se reconoce
        print(f"Subiendo {archivo} con tipo de contenido {tipo_contenido}...")
        # Se sube el archivo.
        document_id = añadir_documento(ruta_archivo, archivo, tipo_contenido)
        obtener_estado_documento(document_id)
