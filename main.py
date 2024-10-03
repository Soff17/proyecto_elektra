from flask import Flask, jsonify, request, abort
from flask_cors import CORS  # Importar CORS
from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe
from funciones import image_storage as st
from funciones.token import verificar_token, generate_token 
from dotenv import load_dotenv
import os
import time

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

bucket_name = os.getenv('bucket_name')
carpeta_imagenes_bucket = os.getenv('carpeta_imagenes_bucket')
carpeta_pdfs_bucket = os.getenv('carpeta_pdfs_bucket')
pdf = os.getenv('pdf')

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Endpoint para generar un token dinámico
@app.route('/generate_token', methods=['GET'])
def generate_token_endpoint():
    token = generate_token()
    return jsonify({"token": token}), 200

# Endpoint para procesamiento de documento e imágenes
@app.route('/new_documents', methods=['POST'])
def procesar_y_subir():
    try:
        # Verificar el token antes de proceder
        verificar_token()

        # Paso 1: Eliminar los pdf de Google Cloud Storage
        st.empty_bucket_folder(bucket_name,carpeta_pdfs_bucket)

        # Paso 2: Eliminar las imágenes de Google Cloud Storage
        st.empty_bucket_folder(bucket_name,carpeta_imagenes_bucket)

        # Paso 3: Eliminar documentos
        wd.eliminar_documentos()

        # Paso 4: Esperar a que se hayan eliminado todos los documentos para subir los nuevos a discovery
        while True:
            total_documentos = wd.contar_documentos()
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados.")
                pe.procesar_pdf(pdf, bucket_name, carpeta_imagenes_bucket, carpeta_pdfs_bucket)
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
