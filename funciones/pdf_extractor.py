import fitz
import re
import csv
import base64

sku_pattern = re.compile(r'Sku:\s*(\S+)')
vigencia_pattern = re.compile(r'Vigencia:\s*(.+)')

titulos = []
subtitulos = []
context = [""]
vigencias = []
skus = []
imagenes = []

def nombre_de_categoria(texto, font_size, font_flags):
    if font_size == 58 and font_flags == 20:
        return True
    return False

def nombre_del_producto(texto, font_size, font_flags):
    if (font_size == 35.0 or font_size == 40.0) and (font_flags == 20 or font_flags == 4):
        return True
    return False

def extraer_informacion(page):
    print("Se está extrayendo el texto...")

    blocks = page.get_text("dict",sort=True)['blocks']
    text_buffer = ''
    fin_produto = False

    for block in blocks:
        if 'lines' in block:
            for line in block['lines']:
                for span in line['spans']:

                    text = span['text'].strip()
                    font_size = span['size']
                    font_flags = span['flags']

                    print(text)

                    #Extraer el Titulo de la categoria
                    if nombre_de_categoria(text, font_size, font_flags):
                        titulos.append(text)
                
                    #Extraer los subtitulos
                    elif nombre_del_producto(text, font_size, font_flags):
                        if text_buffer in subtitulos:
                            word = ' ' + text
                            subtitulos[len(subtitulos)-1] += word
                        else:
                            subtitulos.append(text)

                    #Extraer los Skus
                    elif sku_pattern.findall(text):
                        skus.append(text)

                    #Extraer las vigencias
                    elif vigencia_pattern.findall(text):
                        vigencias.append(text)
                        fin_produto = True

                    #Extraer el context
                    else:
                        if fin_produto:
                            context.append(text)
                            fin_produto = False
                        else:
                            word = ' ' + text
                            context[len(context)-1] += word
    
                    text_buffer = text

def guardar_en_csv(output_path, name_file):
    print("Se esta guardando la informacion en el csv...")

    csv_path = f"{output_path}/{name_file}.csv"
    with open(csv_path, mode='w',newline='',encoding='utf-8') as file:
        writer = csv.writer(file)

        writer.writerow(["Titulo", "Subtitulo", "Context", "SKU", "Vigencia"])

        for i in range(max(len(subtitulos), len(context), len(skus), len(vigencias))):
            sku = skus[i] if i < len(skus) else ""
            vigencia = vigencias[i] if i < len(vigencias) else ""
            subtitulo = subtitulos[i] if i < len(subtitulos) else ""
            content = context[i] if i < len(context) else ""
            writer.writerow([name_file, subtitulo, content, sku, vigencia])

def procesar_pdf(pdf_path, output_dir):

    doc = fitz.open(pdf_path)

    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)
        extraer_informacion(page)
        guardar_en_csv(output_dir, titulos[page_num])
