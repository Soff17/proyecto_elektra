from flask import Flask, jsonify, request, abort
from flask_cors import CORS  # Importar CORS
from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe
from funciones import image_storage as st
from funciones.token import verificar_token, generate_token 
from dotenv import load_dotenv
import os


# Cargar las variables de entorno desde el archivo .env
load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Rutas fijas para archivos
pdf = './data/folleto.pdf'
output_arhivos = './archivos_dummy'
output_imagenes = './imagenes'

# Rutas fijas para imágenes
bucket_name = 'nds_test'
carpeta_en_bucket = 'imagenes_subidas'

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

        # Paso 1: Eliminar documentos
        wd.eliminar_documentos()
        # wd.contar_documentos()
        # print("Documentos eliminados exitosamente.")

        # Paso 2: Esperar hasta que el conteo de documentos sea 0
        while True:
            total_documentos = wd.contar_documentos()
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados.")
                # pe.procesar_pdf(pdf, output_arhivos, output_imagenes)
                print("PDF procesado exitosamente.")
                break
            else:
                print(f"Aún quedan {total_documentos} documentos. Esperando...")

        # Paso 4: Contar los archivos en la carpeta 'output_arhivos'
        archivos_en_carpeta = len(os.listdir('./descargas'))
        print(f"Archivos en la carpeta '{output_arhivos}': {archivos_en_carpeta}")

        # Paso 5: Subir archivos a Watson Discovery
        wd.subir_archivos_de_carpeta('./descargas')
        print("Archivos subidos exitosamente a Watson Discovery.")

        # Paso 6: Esperar hasta que el conteo de documentos en Discovery coincida con los archivos subidos
        while True:
            total_documentos = wd.contar_documentos()
            if total_documentos == archivos_en_carpeta:
                print(f"Todos los documentos ({total_documentos}) han sido subidos exitosamente a Watson Discovery.")
                break
            else:
                print(f"Documentos en Discovery: {total_documentos}, esperando a que coincidan con los {archivos_en_carpeta} archivos...")

        # Paso 5: Subir las imágenes a Google Cloud Storage (comentar si no es necesario)
        # st.upload_images_in_folder(bucket_name, output_imagenes, carpeta_en_bucket)
        # print("Imágenes subidas exitosamente a Google Cloud Storage.")

        return jsonify({"message": "Proceso completo: documentos eliminados, PDF procesado, archivos e imágenes subidas"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
