import os
from ibm_watson import DiscoveryV2
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from dotenv import load_dotenv

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

# Función para eliminar todos los documentos de una colección
def eliminar_documentos():
    # Consulta para obtener todos los documentos en la colección
    query_response = discovery.query(
        project_id=project_id,
        collection_ids=[collection_id],
    ).get_result()
    print(query_response)

    # Verifica si hay documentos en la colección
    if 'results' in query_response:
        # Por cada documento en la colección presente en results
        for document in query_response['results']:
            # Toma su identificador único para eliminarlo.
            doc_id = document['document_id']
            print(f"Eliminando documento con ID: {doc_id}")
            # Consulta para eliminar el documento de la colección.
            discovery.delete_document(
                project_id=project_id,
                collection_id=collection_id,
                document_id=doc_id
            ).get_result()

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
        if archivo.endswith(".csv"):
            # Se obtiene ruta completa
            ruta_archivo = os.path.join(carpeta, archivo)
            # Se establece tipo de contenido
            tipo_contenido = 'text/csv'
            print(f"Subiendo {archivo}...")
            # Se sube el archivo.
            document_id = añadir_documento(ruta_archivo, archivo, tipo_contenido)
            obtener_estado_documento(document_id)
