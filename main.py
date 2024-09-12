from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe

def main():
    print("Iniciando el programa...")

    # Eliminar documentos existentes en la colección Watson Discovery
    wd.eliminar_documentos()

    # Subir archivos Excel desde una carpeta a Watson Discovery
    carpeta_excel = '/Users/sofiadonlucas/Desktop/Visual/NDS/archivos_excel'
    wd.subir_archivos_de_carpeta(carpeta_excel)

    # Procesar PDF
    pdf = './data/pdf.pdf'
    output = './data'
    pe.procesar_pdf(pdf, output)

if __name__ == "__main__":
    main()
