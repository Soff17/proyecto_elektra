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

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Rutas fijas para archivos
pdf = './data/pdfNeuevo.pdf'
output_arhivos_pdf = './archivos_pdf'
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

        # Paso 7: Eliminar las imágenes de Google Cloud Storage
        st.empty_bucket_folder(bucket_name,'pdfs')

        # Paso 7: Eliminar las imágenes de Google Cloud Storage
        st.empty_bucket_folder(bucket_name,carpeta_en_bucket)

        # Paso 1: Eliminar documentos
        wd.eliminar_documentos()

        # Paso 2: Esperar hasta que el conteo de documentos sea 0
        while True:
            total_documentos = wd.contar_documentos()
            if total_documentos == 0:
                print("Todos los documentos han sido eliminados.")
                pe.procesar_pdf(pdf, output_imagenes, output_arhivos_pdf)
                print("PDF procesado exitosamente.")
                break
            else:
                print(f"Aún quedan {total_documentos} documentos. Esperando...")
            time.sleep(5)
        ''''
        # Paso 4: Contar los archivos en la carpeta 'output_arhivos'
        archivos_en_carpeta, lista_archivos = wd.contar_archivos_validos('./archivos_dummy')
        print(f"Archivos en la carpeta {'./archivos_dummy'}: {archivos_en_carpeta}")
        print(f"Lista de archivos: {lista_archivos}")

        # Paso 5: Subir archivos a Watson Discovery
        wd.subir_archivos_de_carpeta('./archivos_dummy')
        print("Archivos subidos exitosamente a Watson Discovery.")

        # Paso 6: Esperar hasta que el conteo de documentos en Discovery coincida con los archivos subidos
        while True:
            total_documentos = wd.contar_documentos()
            if total_documentos == archivos_en_carpeta:
                print(f"Todos los documentos ({total_documentos}) han sido subidos exitosamente a Watson Discovery.")
                break
            else:
                print(f"Documentos en Discovery: {total_documentos}, esperando a que coincidan con los {archivos_en_carpeta} archivos...")
            time.sleep(5)
        
        # Paso 7: Eliminar las imágenes de Google Cloud Storage
        st.empty_bucket_folder(bucket_name,carpeta_en_bucket)

        # Contar cuántas imágenes hay en la carpeta local
        imagenes_en_carpeta = len(os.listdir(output_imagenes))
        print(f"Imágenes en la carpeta '{output_imagenes}': {imagenes_en_carpeta}")

        # Esperar hasta que el bucket esté vacío
        while True:
            imagenes_en_bucket = st.count_images_in_bucket(bucket_name, carpeta_en_bucket)
            if imagenes_en_bucket == 0:
                print("El bucket está vacío, listo para subir nuevas imágenes.")
                # Subir imágenes al bucket
                st.upload_images_in_folder(bucket_name, output_imagenes, carpeta_en_bucket)
                break
            else:
                print(f"Aún quedan {imagenes_en_bucket} imágenes en el bucket. Esperando...")
            time.sleep(5)

        # Paso 8: Verificar si todas las imágenes han sido subidas al bucket
        while True:
            imagenes_en_bucket = st.count_images_in_bucket(bucket_name, carpeta_en_bucket)
            if imagenes_en_bucket == imagenes_en_carpeta:
                print(f"Todas las imágenes ({imagenes_en_bucket}) han sido subidas exitosamente a Google Cloud Storage.")
                break
            else:
                print(f"Imágenes en el bucket: {imagenes_en_bucket}, esperando a que coincidan con las {imagenes_en_carpeta} imágenes locales...")
            time.sleep(5)
        '''
        ''''
        # Paso 7: Eliminar las imágenes de Google Cloud Storage
        st.empty_bucket_folder(bucket_name,'pdfs')

        # Step 2: Count the number of PDFs in the local folder
        pdfs_in_local_folder = len([file for file in os.listdir('archivos_pdf') if file.endswith('.pdf')])
        print(f"PDFs in the local folder 'archivos_pdf': {pdfs_in_local_folder}")

        # Step 3: Wait until the bucket is empty before uploading new PDFs
        while True:
            pdfs_in_bucket = st.count_pdfs_in_bucket(bucket_name)
            if pdfs_in_bucket == 0:
                print("The bucket is empty, ready to upload new PDFs.")
                # Upload PDFs to the bucket
                st.upload_pdfs_in_folder(bucket_name, 'archivos_pdf', 'pdfs')
                break
            else:
                print(f"There are still {pdfs_in_bucket} PDFs in the bucket. Waiting...")
            time.sleep(5)
        '''
        return jsonify({"message": "Proceso completo: documentos eliminados, PDF procesado, archivos e imágenes subidas"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
