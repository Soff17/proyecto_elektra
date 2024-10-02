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
        return document_status
    except Exception as e:
        print(f"Error al obtener estado del documento {document_id}: {str(e)}")

# Función para procesar archivos en paralelo
def subir_archivo_en_paralelo(ruta_archivo, archivo):
    tipo_contenido, _ = mimetypes.guess_type(ruta_archivo)
    if tipo_contenido is None:
        tipo_contenido = 'application/octet-stream'
    
    document_id = añadir_documento(ruta_archivo, archivo, tipo_contenido)
    
    if document_id:
        obtener_estado_documento(document_id)
    else:
        print(f"Error al subir el archivo: {archivo}")

# Función para procesar todos los archivos en una carpeta
''''
def subir_archivos_de_carpeta(carpeta):
    archivos = os.listdir(carpeta)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for archivo in archivos:
            ruta_archivo = os.path.join(carpeta, archivo)
            futures.append(executor.submit(subir_archivo_en_paralelo, ruta_archivo, archivo))
        
        for future in concurrent.futures.as_completed(futures):
            future.result()
'''
# Función para procesar todos los archivos en una carpeta y subcarpetas
def subir_archivos_de_carpeta(carpeta):
    for ruta_carpeta, subcarpetas, archivos in os.walk(carpeta):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for archivo in archivos:
                ruta_archivo = os.path.join(ruta_carpeta, archivo)
                futures.append(executor.submit(subir_archivo_en_paralelo, ruta_archivo, archivo))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

# Función para contar el número total de documentos en una colección
def contar_documentos():
    try:
        # Hacer la consulta para obtener el número total de documentos
        query_response = discovery.query(
            project_id=project_id,
            collection_ids=[collection_id],
            count=0  # No necesitamos traer ningún documento, solo contar
        ).get_result()

        # Obtener el número total de documentos
        total_documents = query_response.get('matching_results', 0)
        print(f"Total de documentos en la colección: {total_documents}")
        return total_documents
    except Exception as e:
        print(f"Error al contar los documentos: {str(e)}")
        return None

# Función para descargar un documento dado su document_id y guardar su contenido
def descargar_documento(document_id, carpeta_descarga):
    try:
        # Ejecutar consulta para obtener el documento con detalles
        document_data = discovery.query(
            project_id=project_id,
            collection_ids=[collection_id],
            filter=f"document_id::{document_id}"
        ).get_result()

        # Verificar si hay resultados y obtener el nombre del documento
        if 'results' in document_data and len(document_data['results']) > 0:
            doc = document_data['results'][0]
            texto_documento = doc.get('text', '')
            nombre_original = doc.get('extracted_metadata', {}).get('filename', f'documento_{document_id}')

            # Sanitizar el nombre del archivo
            nombre_archivo_sanitizado = sanitizar_nombre(nombre_original)

            # Si el texto es una lista, convertirla a una cadena
            if isinstance(texto_documento, list):
                texto_documento = "\n".join(texto_documento)

            if texto_documento:
                ruta_descarga = os.path.join(carpeta_descarga, f"{nombre_archivo_sanitizado}.txt")

                # Guardar el contenido del documento en un archivo .txt
                with open(ruta_descarga, 'w', encoding='utf-8') as file:
                    file.write(texto_documento)

                print(f"Documento {document_id} descargado como {ruta_descarga}")
            else:
                print(f"El documento {document_id} no tiene texto para descargar.")
        else:
            print(f"No se encontró el documento {document_id} o no tiene contenido descargable.")

    except Exception as e:
        print(f"Error al descargar el documento {document_id}: {str(e)}")

# Función para descargar todos los documentos de la colección
def descargar_todos_los_documentos(carpeta_descarga):
    if not os.path.exists(carpeta_descarga):
        os.makedirs(carpeta_descarga)

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
                futures = [executor.submit(descargar_documento, doc_id, carpeta_descarga) for doc_id in document_ids]
                for future in concurrent.futures.as_completed(futures):
                    future.result()

        offset += page_limit

# Filtrar archivos ocultos o temporales y contar archivos en subcarpetas
def contar_archivos_validos(carpeta):
    archivos_validos = []
    for ruta_carpeta, subcarpetas, archivos in os.walk(carpeta):
        for archivo in archivos:
            # Ignorar archivos ocultos o temporales
            if not archivo.startswith('.') and os.path.isfile(os.path.join(ruta_carpeta, archivo)):
                archivos_validos.append(os.path.join(ruta_carpeta, archivo))

    print(f"Archivos válidos encontrados: {archivos_validos}")
    return len(archivos_validos), archivos_validos

# Función para añadir un documento con contenido directo a la colección
def añadir_documento_desde_contenido(contenido, nombre_archivo, tipo_contenido):
    nombre_archivo_sanitizado = sanitizar_nombre(nombre_archivo)

    try:
        # Convertir el contenido a bytes para su envío a Watson Discovery
        contenido_bytes = contenido.encode('utf-8')
        from io import BytesIO
        contenido_io = BytesIO(contenido_bytes)

        response = discovery.add_document(
            project_id=project_id,
            collection_id=collection_id,
            file=contenido_io,
            filename=nombre_archivo_sanitizado, 
            file_content_type=tipo_contenido
        ).get_result()
        return response['document_id']
    except Exception as e:
        print(f"Error al subir {nombre_archivo}: {str(e)}")
        return None
