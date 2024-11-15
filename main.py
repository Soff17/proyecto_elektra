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

# Endpoint para descargar una lista de archivos desde carpeta_documentos_elastic_bucket y guardarlos en una carpeta local
@app.route('/descargar_elastic_documentos', methods=['POST'])
def descargar_elastic_documentos():
    try:
        verificar_token()
        data = request.get_json()
        file_names = data.get('file_names')  # Lista de nombres de archivos
        local_folder = data.get('local_folder')  # Carpeta local donde guardar los archivos

        if not file_names:
            return jsonify({"error": "Falta el parámetro 'file_names'"}), 400
        if not local_folder:
            return jsonify({"error": "Falta el parámetro 'local_folder'"}), 400

        # Asegúrate de que la carpeta local existe, si no, créala
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)

        client = st.initialize_storage_client()
        bucket = client.bucket(bucket_name)

        resultados = []
        for file_name in file_names:
            blob = bucket.blob(f"{carpeta_documentos_elastic_bucket}/{file_name}.txt")
            if not blob.exists():
                resultados.append({"file_name": file_name, "status": "no encontrado"})
                continue

            # Descargar archivo y guardarlo en la carpeta local
            local_path = os.path.join(local_folder, f"{file_name}.txt")
            with open(local_path, "wb") as file:
                blob.download_to_file(file)
            resultados.append({"file_name": file_name, "status": "descargado"})

        return jsonify({"resultados": resultados}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para descargar una lista de archivos desde carpeta_documentos_correcciones_bucket y guardarlos en una carpeta local
@app.route('/descargar_correcciones_documentos', methods=['POST'])
def descargar_correcciones_documentos():
    try:
        verificar_token()
        data = request.get_json()
        file_names = data.get('file_names')  # Lista de nombres de archivos
        local_folder = data.get('local_folder')  # Carpeta local donde guardar los archivos

        if not file_names:
            return jsonify({"error": "Falta el parámetro 'file_names'"}), 400
        if not local_folder:
            return jsonify({"error": "Falta el parámetro 'local_folder'"}), 400

        # Asegúrate de que la carpeta local existe, si no, créala
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

            # Descargar archivo y guardarlo en la carpeta local
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
        verificar_token()
        data = request.get_json()
        folder_path = data.get('folder_path')  # Carpeta local de donde se suben las imágenes
        bucket_folder = data.get('bucket_folder')  # Carpeta en el bucket (opcional)

        if not folder_path:
            return jsonify({"error": "Falta el parámetro 'folder_path'"}), 400

        # Subir imágenes a la carpeta especificada en el bucket
        st.upload_images_in_folder(bucket_name, folder_path, bucket_folder)

        return jsonify({"mensaje": "Imágenes subidas correctamente"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para subir imágenes desde una carpeta local a Google Cloud Storage, especificando una carpeta en el bucket
@app.route('/subir_pdfs', methods=['POST'])
def subir_pdfs_carpeta():
    try:
        verificar_token()
        data = request.get_json()
        folder_path = data.get('folder_path')  # Carpeta local de donde se suben las imágenes
        bucket_folder = data.get('bucket_folder')  # Carpeta en el bucket (opcional)

        if not folder_path:
            return jsonify({"error": "Falta el parámetro 'folder_path'"}), 400

        # Subir imágenes a la carpeta especificada en el bucket
        st.upload_pdfs_in_folder(bucket_name, folder_path, bucket_folder)

        return jsonify({"mensaje": "Pdfs subidos correctamente"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para eliminar imágenes por su nombre en una carpeta específica del bucket
@app.route('/eliminar_imagenes', methods=['DELETE'])
def eliminar_imagenes():
    try:
        verificar_token()
        data = request.get_json()
        imagenes_nombres = data.get('nombres')  # Lista de nombres de las imágenes a eliminar
        bucket_folder = data.get('bucket_folder', carpeta_imagenes_bucket)  # Carpeta en el bucket (opcional)

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
    
@app.route('/eliminar_documentos', methods=['DELETE'])
def eliminar_documentos():
    try:
        verificar_token()
        data = request.get_json()
        documento_ids = data.get('documento_ids')

        if not INDICE:
            return jsonify({"error": "Falta el parámetro 'indice'"}), 400
        if not documento_ids:
            return jsonify({"error": "Falta el parámetro 'documento_ids'"}), 400

        # Asegurarse de que documento_ids es una lista, incluso si es solo un ID
        if isinstance(documento_ids, str):
            documento_ids = [documento_ids]

        resultados = []
        for doc_id in documento_ids:
            try:
                # Usar directamente el método delete de la instancia `es`
                es.eliminar_documento(INDICE,doc_id )
                resultados.append({"documento_id": doc_id, "status": "eliminado"})
            except NotFoundError:
                resultados.append({"documento_id": doc_id, "status": "Documento no encontrado"})
            except Exception as e:
                resultados.append({"documento_id": doc_id, "status": f"Error: {str(e)}"})

        return jsonify({"resultados": resultados}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
# Endpoint para subir archivos de una carpeta al índice
@app.route('/subir_archivos', methods=['POST'])
def subir_archivos():
    try:
        verificar_token()
        es.eliminar_documentos("elektra-docs")
        # Paso 2: Esperar a que el índice esté vacío

        while True:
            total_documentos = es.contar_documentos("elektra-docs")
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados del índice. Comenzando la indexación.")
                break
            else:
                print(f"Aún quedan {total_documentos} documentos en el índice. Esperando...")
                time.sleep(5)  # Espera 5 segundos antes de volver a verificar

        data = request.get_json()
        carpeta = data.get('carpeta')  # Capturamos la carpeta desde el body de la solicitud

        if not INDICE:
            return jsonify({"error": "Falta el parámetro 'indice'"}), 400

        if not carpeta:
            return jsonify({"error": "Falta el parámetro 'carpeta'"}), 400

        # Subir todos los archivos de la carpeta al índice especificado
        es.subir_archivos_de_carpeta(INDICE, carpeta)

        return jsonify({"mensaje": f"Archivos subidos correctamente al índice {INDICE} desde la carpeta {carpeta}"}), 200

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
