#from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe
import os

def main():
    print("Iniciando extracción de información")

    # Eliminar documentos existentes en la colección Watson Discovery
    #wd.eliminar_documentos()

    # Procesar PDF de catálogo
    pdf = './data/folleto.pdf'
    output_arhivos_pdf = './arhivos_pdf'
    output_arhivos = './archivos_dummy'
    output_imagenes = './imagenes'
    # pe.procesar_pdf(f"./arhivos_pdf/pagina_16.pdf", output_arhivos, output_imagenes)
    pe.particion_pdf(pdf, output_arhivos_pdf)

    archivos_pdf = [archivo for archivo in os.listdir(output_arhivos_pdf) if archivo.endswith('.pdf')]
    print(archivos_pdf)
    for arhivo in archivos_pdf:
        print(arhivo)
        pdf_path = f"./arhivos_pdf/{arhivo}"
        pe.procesar_pdf(pdf_path, output_arhivos, output_imagenes)   

    # Subir archivos Excel desde una carpeta a Watson Discovery
    #carpeta_excel = './data'
    #wd.subir_archivos_de_carpeta(carpeta_excel)

if __name__ == "__main__":
    main()
