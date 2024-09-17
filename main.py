#from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe

def main():
    print("Iniciando extracción de información")

    # Eliminar documentos existentes en la colección Watson Discovery
    #wd.eliminar_documentos()

    # Procesar PDF de catálogo
    pdf = './data/pdf.pdf'
    output = './archivos_dummy'
    pe.procesar_pdf(pdf, output)

    # Subir archivos Excel desde una carpeta a Watson Discovery
    #carpeta_excel = './data'
    #wd.subir_archivos_de_carpeta(carpeta_excel)

if __name__ == "__main__":
    main()
