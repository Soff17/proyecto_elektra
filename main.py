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
from elasticsearch.exceptions import NotFoundError
from concurrent.futures import ThreadPoolExecutor
from flask import send_file
import zipfile

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

bucket_name = os.getenv('bucket_name')
carpeta_imagenes_bucket = os.getenv('carpeta_imagenes_bucket')
carpeta_pdfs_bucket = os.getenv('carpeta_pdfs_bucket')
carpeta_documentos_correcciones_bucket = os.getenv('carpeta_documentos_correcciones_bucket')
carpeta_documentos_elastic_bucket = os.getenv('carpeta_documentos_elastic_bucket')
documentos_dummy = './imagenes'
INDICE = os.getenv('INDICE')
carpeta_reportes_bucket = os.getenv('carpeta_reportes_bucket')

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Endpoint para descargar arhcivos pdf almacenados en GCP
@app.route('/descargar_pdfs', methods=['POST'])
def descargar_todos_pdfs():
    try:
        # Verificar el token antes de proceder
        verificar_token()
        data = request.get_json()
        local_folder = data.get('local_folder')

        if not local_folder:
            return jsonify({"error": "Falta el parámetro 'local_folder'"}), 400

        if not os.path.exists(local_folder):
            os.makedirs(local_folder)

        client = st.initialize_storage_client()
        bucket = client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=carpeta_pdfs_bucket)

        resultados = []
        for blob in blobs:
            if blob.name.endswith('.pdf'):
                local_path = os.path.join(local_folder, os.path.basename(blob.name))
                with open(local_path, "wb") as file:
                    blob.download_to_file(file)
                resultados.append({"file_name": os.path.basename(blob.name), "status": "descargado"})
            else:
                resultados.append({"file_name": os.path.basename(blob.name), "status": "no es un PDF"})

        return jsonify({"resultados": resultados}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Endpoint para eliminar todo el contenido de una carpeta en el bucket (sin eliminar la carpeta)
@app.route('/vaciar_carpeta', methods=['DELETE'])
def vaciar_carpeta():
    try:
        # Verificar el token antes de proceder
        verificar_token()

        data = request.get_json()
        bucket_folder = data.get('bucket_folder')

        if not bucket_folder:
            return jsonify({"error": "Falta el parámetro 'bucket_folder'"}), 400

        st.empty_bucket_folder(bucket_name, bucket_folder)

        return jsonify({"message": f"Todo el contenido de la carpeta '{bucket_folder}' fue eliminado exitosamente."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para descargar una lista de archivos desde carpeta_documentos_correcciones_bucket y guardarlos en una carpeta local
@app.route('/descargar_correcciones_documentos', methods=['POST'])
def descargar_correcciones_documentos():
    try:
        # Verificar el token antes de proceder
        verificar_token()
        data = request.get_json()
        file_names = data.get('file_names')
        local_folder = data.get('local_folder')

        if not file_names:
            return jsonify({"error": "Falta el parámetro 'file_names'"}), 400
        if not local_folder:
            return jsonify({"error": "Falta el parámetro 'local_folder'"}), 400

        if not os.path.exists(local_folder):
            os.makedirs(local_folder)

        client = st.initialize_storage_client()
        bucket = client.bucket(bucket_name)

        resultados = []
        for file_name in file_names:
            blob = bucket.blob(f"{carpeta_documentos_correcciones_bucket}/{file_name}.txt")
            if not blob.exists():
                resultados.append({"file_name": file_name, "status": "no encontrado"})
                continue

            local_path = os.path.join(local_folder, f"{file_name}.txt")
            with open(local_path, "wb") as file:
                blob.download_to_file(file)
            resultados.append({"file_name": file_name, "status": "descargado"})

        return jsonify({"resultados": resultados}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para subir imágenes desde una carpeta local a Google Cloud Storage, especificando una carpeta en el bucket
@app.route('/subir_imagenes', methods=['POST'])
def subir_imagenes_carpeta():
    try:
        # Verificar el token antes de proceder
        verificar_token()
        data = request.get_json()
        folder_path = data.get('folder_path') 
        bucket_folder = data.get('bucket_folder') 

        if not folder_path:
            return jsonify({"error": "Falta el parámetro 'folder_path'"}), 400

        st.upload_images_in_folder(bucket_name, folder_path, bucket_folder)

        return jsonify({"mensaje": "Imágenes subidas correctamente"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para subir archivos pdf desde una carpeta local a Google Cloud Storage, especificando una carpeta en el bucket
@app.route('/subir_pdfs', methods=['POST'])
def subir_pdfs_carpeta():
    try:
        verificar_token()
        data = request.get_json()
        folder_path = data.get('folder_path') 
        bucket_folder = data.get('bucket_folder')

        if not folder_path:
            return jsonify({"error": "Falta el parámetro 'folder_path'"}), 400

        st.upload_pdfs_in_folder(bucket_name, folder_path, bucket_folder)

        return jsonify({"mensaje": "Pdfs subidos correctamente"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para eliminar imágenes por su nombre en una carpeta específica del bucket
@app.route('/eliminar_imagenes', methods=['DELETE'])
def eliminar_imagenes():
    try:
        # Verificar el token antes de proceder
        verificar_token()
        data = request.get_json()
        imagenes_nombres = data.get('nombres') 
        bucket_folder = data.get('bucket_folder', carpeta_imagenes_bucket)

        if not imagenes_nombres:
            return jsonify({"error": "Falta el parámetro 'nombres'"}), 400

        if isinstance(imagenes_nombres, str):
            imagenes_nombres = [imagenes_nombres]

        client = st.initialize_storage_client()
        resultados = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(lambda name: st.empty_bucket_folder(bucket_name, f"{bucket_folder}/{name}"), nombre)
                for nombre in imagenes_nombres
            ]
            for future, nombre in zip(futures, imagenes_nombres):
                try:
                    future.result()
                    resultados.append({"nombre": nombre, "status": "eliminada"})
                except Exception as e:
                    resultados.append({"nombre": nombre, "status": f"Error: {str(e)}"})

        return jsonify({"resultados": resultados}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Se elimina de elastic search y de GCP, y se descargan para su corrección.
@app.route('/eliminar_documentos', methods=['DELETE'])
def eliminar_documentos():
    try:
        # Verificar el token antes de proceder
        verificar_token()
        data = request.get_json()
        documento_ids = data.get('documento_ids')
        download_folder = data.get('download_folder')  # Carpeta de descarga especificada en la solicitud

        if not INDICE:
            return jsonify({"error": "Falta el parámetro 'indice'"}), 400
        if not documento_ids:
            return jsonify({"error": "Falta el parámetro 'documento_ids'"}), 400
        if not download_folder:
            return jsonify({"error": "Falta el parámetro 'download_folder'"}), 400

        if isinstance(documento_ids, str):
            documento_ids = [documento_ids]

        resultados = []
        client = st.initialize_storage_client()
        bucket = client.bucket(bucket_name)

        # Crear la carpeta de descarga si no existe
        if not os.path.exists(download_folder):
            os.makedirs(download_folder)

        for doc_id in documento_ids:
            try:
                # Paso 1: Eliminar documento de ElasticSearch
                es.eliminar_documento(INDICE, doc_id)
                elastic_status = "eliminado"
            except NotFoundError:
                elastic_status = "no encontrado en ElasticSearch"
            except Exception as e:
                elastic_status = f"Error en ElasticSearch: {str(e)}"

            # Paso 2: Descargar el documento desde Google Cloud Storage
            try:
                blob = bucket.blob(f"{carpeta_documentos_elastic_bucket}/{doc_id}.txt")
                if blob.exists():
                    local_path = os.path.join(download_folder, f"{doc_id}.txt")
                    with open(local_path, "wb") as file:
                        blob.download_to_file(file)
                    gcs_status = "descargado"

                    # Paso 3: Eliminar el documento de GCS
                    blob.delete()
                    gcs_delete_status = "eliminado"
                else:
                    gcs_status = "no encontrado en GCS"
                    gcs_delete_status = "no encontrado en GCS"

            except Exception as e:
                gcs_status = f"Error al descargar: {str(e)}"
                gcs_delete_status = f"Error al eliminar: {str(e)}"

            resultados.append({
                "documento_id": doc_id,
                "elastic_status": elastic_status,
                "gcs_status": gcs_status,
                "gcs_delete_status": gcs_delete_status
            })

        return jsonify({"resultados": resultados}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
# Endpoint para subir archivos de una carpeta al índice de Elastic Search
@app.route('/subir_archivos', methods=['POST'])
def subir_archivos():
    try:
        # Verificar el token antes de proceder
        verificar_token()

        data = request.get_json()
        carpeta = data.get('carpeta')

        if not INDICE:
            return jsonify({"error": "Falta el parámetro 'indice'"}), 400

        if not carpeta:
            return jsonify({"error": "Falta el parámetro 'carpeta'"}), 400

        # Paso 1: Subir archivos al índice de ElasticSearch
        es.subir_archivos_de_carpeta(INDICE, carpeta)

        # Paso 2: Subir los archivos también a GCP en la carpeta 'carpeta_documentos_elastic_bucket'
        client = st.initialize_storage_client()
        bucket = client.bucket(bucket_name)

        # Buscar todos los archivos en la carpeta indicada
        for file_name in os.listdir(carpeta):
            local_file_path = os.path.join(carpeta, file_name)
            if os.path.isfile(local_file_path):
                # Subir archivo a GCS
                blob = bucket.blob(f"{carpeta_documentos_elastic_bucket}/{file_name}")
                blob.upload_from_filename(local_file_path)
                print(f"Archivo {file_name} subido correctamente a GCS en {carpeta_documentos_elastic_bucket}")

        return jsonify({"mensaje": f"Archivos subidos correctamente al índice {INDICE} y a GCP desde la carpeta {carpeta}"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Endpoint para generar un token dinámico
@app.route('/generate_token', methods=['GET'])
def generate_token_endpoint():
    try:
        token = generate_token()
        return jsonify({"token": token}), 200
    except Exception as e:
        return jsonify({"error": "Failed to generate token", "message": str(e)}), 500

# Endpoint para procesamiento de documento e imágenes
@app.route('/ingesta_documentos', methods=['POST'])
def procesar_y_subir():
    try:
        # Verificar el token antes de proceder
        verificar_token()

        if 'file' not in request.files:
            return jsonify({"error": "No se encontró el archivo PDF en la solicitud"}), 400

        pdf_file = request.files['file']

        if pdf_file.filename == '':
            return jsonify({"error": "El archivo PDF está vacío"}), 400

        # Leer el archivo PDF en memoria
        pdf_buffer = io.BytesIO(pdf_file.read())

        # Paso 1: Eliminar los pdf de Google Cloud Storage
        st.empty_bucket_folder(bucket_name, carpeta_pdfs_bucket)

        # Paso 2: Eliminar las imágenes de Google Cloud Storage
        st.empty_bucket_folder(bucket_name, carpeta_imagenes_bucket)

        # Paso 3: Eliminar documentos
        # wd.eliminar_documentos()
        es.eliminar_documentos(INDICE)

        # Paso 4: Eliminar respaldos
        st.empty_bucket_folder(bucket_name, carpeta_documentos_correcciones_bucket)
        st.empty_bucket_folder(bucket_name, carpeta_documentos_elastic_bucket)

        #pe.procesar_pdf(pdf_buffer, bucket_name, carpeta_imagenes_bucket, carpeta_pdfs_bucket, carpeta_documentos_correcciones_bucket, carpeta_documentos_elastic_bucket, carpeta_reportes_bucket)
        #pe.particion_pdf(pdf_buffer, bucket_name,carpeta_pdfs_bucket)


        # Paso 4: Esperar a que se hayan eliminado todos los documentos para subir los nuevos a discovery
        while True:
            total_documentos = es.contar_documentos(INDICE)
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados.")
                pe.procesar_pdf(pdf_buffer, bucket_name, carpeta_imagenes_bucket, carpeta_pdfs_bucket, carpeta_documentos_correcciones_bucket, carpeta_documentos_elastic_bucket, carpeta_reportes_bucket)
                pe.particion_pdf(pdf_buffer, bucket_name,carpeta_pdfs_bucket)
                local_folder = './documentos_planes'
                if os.path.exists(local_folder):
                    for file_name in os.listdir(local_folder):
                        local_file_path = os.path.join(local_folder, file_name)
                        if os.path.isfile(local_file_path):
                            st.upload_file(bucket_name, carpeta_documentos_correcciones_bucket, local_file_path)
                print("PDF procesado exitosamente.")
                break
            else:
                print(f"Aún quedan {total_documentos} documentos. Esperando...")
            time.sleep(5)

        return jsonify({"message": "Proceso completo: documentos eliminados, PDF procesado, archivos e imágenes subidas"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
