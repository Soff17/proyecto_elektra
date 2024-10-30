from flask import Flask, jsonify, request, abort
from flask_cors import CORS  # Importar CORS
from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe
from funciones import image_storage as st
from funciones import elastict_search as es
from funciones.token import verificar_token, generate_token 
from dotenv import load_dotenv
import os
import time
import io

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

bucket_name = os.getenv('bucket_name')
carpeta_imagenes_bucket = os.getenv('carpeta_imagenes_bucket')
carpeta_pdfs_bucket = os.getenv('carpeta_pdfs_bucket')
carpeta_documentos_correcciones_bucket = os.getenv('carpeta_documentos_correcciones_bucket')
carpeta_documentos_elastic_bucket = os.getenv('carpeta_documentos_elastic_bucket')
documentos_dummy = './imagenes'

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Endpoint para subir archivos de una carpeta al índice
@app.route('/subir_archivos_carpeta', methods=['POST'])
def subir_archivos():
    try:
        verificar_token()
        es.eliminar_documentos("catalogo")
        # Paso 2: Esperar a que el índice esté vacío
        while True:
            total_documentos = es.contar_documentos("catalogo")
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados del índice. Comenzando la indexación.")
                break
            else:
                print(f"Aún quedan {total_documentos} documentos en el índice. Esperando...")
                time.sleep(5)  # Espera 5 segundos antes de volver a verificar

        data = request.get_json()
        indice = data.get('indice')
        carpeta = data.get('carpeta')  # Capturamos la carpeta desde el body de la solicitud

        if not indice:
            return jsonify({"error": "Falta el parámetro 'indice'"}), 400

        if not carpeta:
            return jsonify({"error": "Falta el parámetro 'carpeta'"}), 400

        # Subir todos los archivos de la carpeta al índice especificado
        es.subir_archivos_de_carpeta(indice, carpeta)

        return jsonify({"mensaje": f"Archivos subidos correctamente al índice {indice} desde la carpeta {carpeta}"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Endpoint para eliminar documentos por categoría
@app.route('/eliminar_documentos_categoria', methods=['DELETE'])
def eliminar_documentos_categoria():
    try:
        verificar_token()
        data = request.get_json()
        categoria = data.get('categoria', " ") 

        resultado = es.eliminar_documentos_por_categoria(categoria)
        return jsonify({"mensaje": resultado}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para generar un token dinámico
@app.route('/generate_token', methods=['GET'])
def generate_token_endpoint():
    token = generate_token()
    return jsonify({"token": token}), 200

# Endpoint para procesamiento de documento e imágenes
@app.route('/ingesta_documentos', methods=['POST'])
def procesar_y_subir():
    try:
        # Verificar el token antes de proceder
        verificar_token()

        # Verificar si se recibió un archivo PDF
        if 'file' not in request.files:
            return jsonify({"error": "No se encontró el archivo PDF en la solicitud"}), 400

        pdf_file = request.files['file']

        if pdf_file.filename == '':
            return jsonify({"error": "El archivo PDF está vacío"}), 400

        # Leer el archivo PDF en memoria
        pdf_buffer = io.BytesIO(pdf_file.read())

        # Paso 1: Eliminar los pdf de Google Cloud Storage
        # st.empty_bucket_folder(bucket_name, carpeta_pdfs_bucket)

        # Paso 2: Eliminar las imágenes de Google Cloud Storage
        # st.empty_bucket_folder(bucket_name, carpeta_imagenes_bucket)

        # Paso 3: Eliminar documentos
        # wd.eliminar_documentos()
        # es.eliminar_documentos("catalogo")

        # Paso 4: Eliminar respaldos
        # st.empty_bucket_folder(bucket_name, carpeta_documentos_correcciones_bucket)
        # st.empty_bucket_folder(bucket_name, carpeta_documentos_elastic_bucket)

        pe.procesar_pdf(pdf_buffer, bucket_name, carpeta_imagenes_bucket, carpeta_pdfs_bucket, carpeta_documentos_correcciones_bucket, carpeta_documentos_elastic_bucket)
        pe.particion_pdf(pdf_buffer, bucket_name,carpeta_pdfs_bucket)

        ''''
        # Paso 4: Esperar a que se hayan eliminado todos los documentos para subir los nuevos a discovery
        while True:
            total_documentos = es.contar_documentos("catalogo")
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados.")
                pe.procesar_pdf(pdf_buffer, bucket_name, carpeta_imagenes_bucket, carpeta_pdfs_bucket, carpeta_documentos_correcciones_bucket, carpeta_documentos_elastic_bucket)
                pe.particion_pdf(pdf_buffer, bucket_name,carpeta_pdfs_bucket)
                print("PDF procesado exitosamente.")
                break
            else:
                print(f"Aún quedan {total_documentos} documentos. Esperando...")
            time.sleep(5)
        '''
        return jsonify({"message": "Proceso completo: documentos eliminados, PDF procesado, archivos e imágenes subidas"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)

'''''
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

# Función para procesar archivos en paralelo
def subir_archivo_en_paralelo(ruta_archivo, archivo):
    tipo_contenido, _ = mimetypes.guess_type(ruta_archivo)
    if tipo_contenido is None:
        tipo_contenido = 'application/octet-stream'
    
    document_id = añadir_documento(ruta_archivo, archivo, tipo_contenido)
    
    if document_id:
        print(f"Archivo subido exitosamente: {archivo}")
    else:
        print(f"Error al subir el archivo: {archivo}")

# Función para procesar todos los archivos en la carpeta documentos_dummy
def subir_archivos_de_carpeta():
    carpeta = './documentos_dummy'  # Ruta a la carpeta documentos_dummy
    for ruta_carpeta, subcarpetas, archivos in os.walk(carpeta):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for archivo in archivos:
                # Ignorar archivos ocultos o temporales
                if archivo.startswith('.'):
                    continue
                
                ruta_archivo = os.path.join(ruta_carpeta, archivo)
                futures.append(executor.submit(subir_archivo_en_paralelo, ruta_archivo, archivo))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

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
    
eliminar_documentos()

while True:
            total_documentos = contar_documentos()
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados.")
                subir_archivos_de_carpeta()
                print("PDF procesado exitosamente.")
                break
            else:
                print(f"Aún quedan {total_documentos} documentos. Esperando...")
'''
