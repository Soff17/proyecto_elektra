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
        # es.eliminar_documentos("catalogo")
        # Paso 2: Esperar a que el índice esté vacío
        ''''
        while True:
            total_documentos = es.contar_documentos("catalogo")
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados del índice. Comenzando la indexación.")
                break
            else:
                print(f"Aún quedan {total_documentos} documentos en el índice. Esperando...")
                time.sleep(5)  # Espera 5 segundos antes de volver a verificar
        '''

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
