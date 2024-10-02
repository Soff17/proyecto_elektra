#from funciones import watson_discovery as wd
from funciones import pdf_extractor as pe
import os

def main():
    print("Iniciando extracción de información")

    # Eliminar documentos existentes en la colección Watson Discovery
    #wd.eliminar_documentos()

    # Procesar PDF de catálogo
    pdf = './data/pdfNeuevo.pdf'
    output_arhivos_pdf = './archivos_pdf'
    output_imagenes = './imagenes'
    pe.procesar_pdf(f"./archivos_pdf/pagina_1.pdf", output_imagenes)
    # pe.particion_pdf(pdf, output_arhivos_pdf)
    
    # archivos_pdf = [archivo for archivo in os.listdir(output_arhivos_pdf) if archivo.endswith('.pdf')]
    
    # for arhivo in archivos_pdf:
    #     print(arhivo)
    #     pdf_path = f"./archivos_pdf/{arhivo}"
    #     pe.procesar_pdf(pdf_path, output_imagenes)   

    # Subir archivos Excel desde una carpeta a Watson Discovery
    #carpeta_excel = './data'
    #wd.subir_archivos_de_carpeta(carpeta_excel)

if __name__ == "__main__":
    main()
