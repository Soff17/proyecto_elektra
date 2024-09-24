#from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe

def main():
    print("Iniciando extracción de información")

    # Eliminar documentos existentes en la colección Watson Discovery
    #wd.eliminar_documentos()

    # Procesar PDF de catálogo
    pdf = './data/folleto_cambaceo_semanal_2024_W35_V1.2_movilidad.pdf'
    output_arhivos = './archivos_dummy'
    output_imagenes = './imagenes'
    pe.procesar_pdf(pdf, output_arhivos, output_imagenes)

    # Subir archivos Excel desde una carpeta a Watson Discovery
    #carpeta_excel = './data'
    #wd.subir_archivos_de_carpeta(carpeta_excel)

if __name__ == "__main__":
    main()
