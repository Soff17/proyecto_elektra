from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe

def main():
    print("Iniciando extracción de información")

    # Procesar PDF de catálogo
    pdf = './data/pdf.pdf'
    output = './data'
    pe.procesar_pdf(pdf, output)

    # Eliminar documentos existentes en la colección Watson Discovery
    wd.eliminar_documentos()

    # Subir archivos Excel desde una carpeta a Watson Discovery
    carpeta_excel = './archivos_dummy'
    wd.subir_archivos_de_carpeta(carpeta_excel)

if __name__ == "__main__":
    main()
