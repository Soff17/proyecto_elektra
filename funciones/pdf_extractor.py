import fitz
import os
import re

sku_pattern = re.compile(r'Sku:\s*(\S+)')
vigencia_pattern = re.compile(r'Vigencia:\s*(.+)')
subtitulos = []
texto = []
vigencias = []
skus = []
imagenes = []

def extraer_informacion(pdf_path):
    print("Se está extrayendo el texto...")
    
    doc = fitz.open(pdf_path)

    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)
        text = page.get_text(sort=True)
        print(text)

        primera_linea = text.splitlines()[0] if text.splitlines() else ""
        subtitulos.append(primera_linea)

        matches = sku_pattern.findall(text)
        for match in matches:
            skus.append(match.strip())

        matches = vigencia_pattern.findall(text)
        for match in matches:
            vigencias.append(match.strip())

def guardar_en_csv(output_path):
    print("Se esta guardando la informacion en el csv...") 
    # Aquí va el código para guardar en CSV

def procesar_pdf(pdf_path, output_dir):
    extraer_informacion(pdf_path)
    guardar_en_csv(output_dir)
