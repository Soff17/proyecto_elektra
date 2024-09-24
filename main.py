from flask import Flask, jsonify
from flask_cors import CORS  # Importar CORS
#from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Rutas fijas para archivos
pdf = './data/folleto_cambaceo_semanal_2024_W35_V1.2_movilidad.pdf'
output_arhivos = './archivos_dummy'
output_imagenes = './imagenes'
  
@app.route('/new_documents', methods=['POST'])
def procesar_y_subir():
    try:
        # Paso 1: Eliminar documentos
        # wd.eliminar_documentos()
        print("Documentos eliminados exitosamente.")

        # Paso 2: Procesar el PDF
        pe.procesar_pdf(pdf, output_arhivos, output_imagenes)
        print("PDF procesado exitosamente.")

        # Paso 3: Subir archivos a Watson Discovery
        # wd.subir_archivos_de_carpeta(output_arhivos)
        print("Archivos subidos exitosamente.")

        return jsonify({"message": "Proceso completo: documentos eliminados, PDF procesado, archivos subidos"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)