from flask import Flask, jsonify, request, abort
from flask_cors import CORS  # Importar CORS
#from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe
from funciones import image_storage as st
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Rutas fijas para archivos
pdf = './data/folleto_cambaceo_semanal_2024_W35_V1.2_movilidad.pdf'
output_arhivos = './archivos_dummy'
output_imagenes = './imagenes'

# Rutas fijas para imágenes
bucket_name = 'nds_test'
carpeta_en_bucket = 'imagenes_subidas'

# Definición de token
TOKEN_VALIDO = os.getenv('TOKEN_VALIDO')

# Middleware para verificar el token en cada solicitud
def verificar_token():
    token = request.headers.get('Authorization')
    if not token or token != TOKEN_VALIDO:
        abort(401, description="Token no válido o faltante")

@app.route('/new_documents', methods=['POST'])
def procesar_y_subir():
    try:
        # Verificar el token antes de proceder
        verificar_token()

        # Paso 1: Eliminar documentos
        # wd.eliminar_documentos()
        print("Documentos eliminados exitosamente.")

        # Paso 2: Procesar el PDF
        pe.procesar_pdf(pdf, output_arhivos, output_imagenes)
        print("PDF procesado exitosamente.")

        # Paso 3: Subir archivos a Watson Discovery
        # wd.subir_archivos_de_carpeta(output_arhivos)
        print("Archivos subidos exitosamente.")

        # Paso 4: Eliminar imágenes existentes en Google Cloud Storage
        # st.empty_bucket(bucket_name)
        print("Imágenes eliminadas")
        
        # Paso 5: Subir las imágenes a Google Cloud Storage
        # st.upload_images_in_folder(bucket_name, output_imagenes, carpeta_en_bucket)
        print("Imágenes subidas exitosamente a Google Cloud Storage.")

        return jsonify({"message": "Proceso completo: documentos eliminados, PDF procesado, archivos e imágenes subidas"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
