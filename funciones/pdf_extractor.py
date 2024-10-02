import fitz
import re
import os
from funciones import watson_discovery as wd

sku_pattern = re.compile(r'Sku:\s*(\S+)')
sku_pattern_2 = re.compile(r'Sku de referencia: \s*(\S+)')
vigencia_pattern = re.compile(r'Vigencia:\s*(.+)')

titulos = []
subtitulos = []
info = []
urls = []
skus = []
vigencias = []

def nombre_de_categoria(font_size, font_flags):
    if (font_size > 43) and font_flags == 20:
        return True
    return False

def nombre_del_producto(font_size, font_flags):
    if (font_size == 35.0 or font_size == 40.0) and (font_flags == 20 or font_flags == 4):
        return True
    return False

def extraer_informacion(page):
    blocks = page.get_text("dict",sort=True)["blocks"]
    text_buffer = ''
    inicio_producto = False
    fin_producto = False
    datos = ''

    for block in blocks:
        if 'lines' in block:
            for line in block['lines']:
                for span in line['spans']:

                    text = span['text'].strip()
                    text_size = span['size']
                    text_flags = span['flags']

                    # print("\n-------------")
                    # print(f"Text Size: {text_size}")
                    # print(f"Text flags: {text_flags}")
                    # print(f"Text: {text}")
                    # print(f"Text buffer: {text_buffer}")
                    # print("-------------")

                    #Get nombre de categoria
                    if nombre_de_categoria(text_size, text_flags) and inicio_producto == False:
                        if text_buffer in titulos:
                            titulos[len(titulos)-1] += " " + text
                        else:
                            titulos.append(text)

                    #Get nombre de prodcuto
                    elif nombre_del_producto(text_size, text_flags):
                        if inicio_producto:
                            subtitulos[len(subtitulos)-1] += " " + text
                        else:
                            subtitulos.append(text)
                            inicio_producto = True
                            fin_producto = False
                            datos = ''
                        #print(f"\nTITULO: {subtitulos[len(subtitulos)-1]}")
                    
                    #Get SKUs
                    elif sku_pattern.findall(text):
                        sku = sku_pattern.findall(text)[0]
                        sku = sku.replace(".","")
                        skus.append(sku)
                        fin_producto = True

                    # elif sku_pattern_2.findall(text):
                    #     sku = sku_pattern_2.findall(text)[0]
                    #     sku = sku.replace(".","")
                    #     skus.append(sku)
                    
                    #Get Vigencias
                    elif vigencia_pattern.findall(text) and fin_producto and inicio_producto:
                        vigencias.append(text)
                        #print(f"\nINSERT DATOS:\n{datos}")
                        info.append(datos)
                        datos=""
                        fin_producto = False
                        inicio_producto = False
                        
                    #Get Info prodcuto
                    else:
                        datos += " " + text
                    # elif inicio_producto:
                    #     if fin_producto or len(info) == 0:
                    #         info.append(text)
                    #         fin_producto = False
                    #     else:
                    #         word = ' ' + text
                    #         info[len(info)-1] += word


                    text_buffer = text

def extraer_imagenes_orden(output_imagenes, page, doc):
    images = page.get_image_info(hashes=True, xrefs=True)
    imagenes = []

    for img in images:
        xref = img['xref']
        if xref > 0:
            if img['width'] > 500 and img['height'] > 500:
                bbox_img = img['bbox']
                imagenes.append((xref, bbox_img))
    
    images_sorted = sorted(imagenes, key=lambda img: img[1][3], reverse=False)

    count = 0
    for xref, bbox in images_sorted:
        base_image = doc.extract_image(xref)
        image_bytes = base_image["image"]
        ext = base_image["ext"]
        
        
        if (len(skus)) > count:
            path = f"./{output_imagenes}/{skus[count]}.{ext}"
        else:
            path = f"./{output_imagenes}/producto_{count+1}.{ext}"
        with open(path, "wb") as image_file:
            image_file.write(image_bytes)
        count += 1

def get_urls(page):
    links = page.get_links()
    for link in links:
        url = link['uri']
        url = url.replace("https://www.elektra.mx/", "")
        url = url.replace(":","-")
        url = url.replace("/", "[")
        url = url.replace("?", "]")
        url = url.replace("%", "-")
        urls.append(url)

def guardar_informacion_a_discovery(titulo, name_file, data):
    # Generar el contenido del archivo como una cadena
    contenido_txt = "\n".join(data)
    
    # Subir directamente a IBM Watson Discovery
    from funciones import watson_discovery as wd  # Importar Watson Discovery
    
    # Usar una función similar a `añadir_documento` para subir el contenido
    wd.añadir_documento_desde_contenido(contenido_txt, f"{titulo} {name_file}.txt", 'text/plain')
        
def particion_pdf(pdf_path, output_archivos):
    doc = fitz.open(pdf_path)
    
    for num_page in range(doc.page_count):
        doc_pagina = fitz.open()
        doc_pagina.insert_pdf(doc, from_page=num_page, to_page=num_page)
        nombre_archivo_salida = f"{output_archivos}/pagina_{num_page + 1}.pdf"
        doc_pagina.save(nombre_archivo_salida)
        
    doc_pagina.close()


def procesar_pdf(pdf_path, output_imagenes, output_archivos):
    print(pdf_path)
    doc = fitz.open(pdf_path)
    ruta_base = os.getcwd()

    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)

        titulos.clear()
        subtitulos.clear()
        info.clear()
        urls.clear()
        skus.clear()
        vigencias.clear()

        extraer_informacion(page)
        get_urls(page)
        extraer_imagenes_orden(output_imagenes, page, doc)

        if len(titulos) == 0:
            break

        # ruta_directorio = os.path.join(ruta_base, 'archivos_dummy', f'{titulos[0]}')
        # os.makedirs(ruta_directorio, exist_ok=True)
        # print(f"\n-------------")
        # print(f"LEN DE INFO: {len(info)}")
        # print(f"LEN DE SKU: {len(skus)}")
        # print(f"LEN DE VIGENCIAS: {len(vigencias)}")
        # print("-------------")

        for i in range(max(len(subtitulos), len(info), len(skus), len(vigencias), len(urls))):
            sku = skus[i] if i < len(skus) else ""
            vigencia = vigencias[i] if i < len(vigencias) else ""
            subtitulo = subtitulos[i] if i < len(subtitulos) else ""
            content = info[i] if i < len(info) else ""
            url = urls[i] if i < len(urls) else f"{page_num}_Dummy{i}"

            if sku:
                sku_num = "Sku: " + sku
                data = [subtitulo, sku_num, content, vigencia]
                guardar_informacion_a_discovery(titulos[0], f"{sku} {url}", data)

        #Particion pdf
        doc_pagina = fitz.open()
        doc_pagina.insert_pdf(doc, from_page=page_num, to_page=page_num)
        nombre_archivo_salida = f"{output_archivos}/{titulos[0]}.pdf"
        doc_pagina.save(nombre_archivo_salida)

        doc_pagina.close()
    
        # doc.close()
        # nuevo_nombre = f"./archivos_pdf/{titulos[0]}.pdf"
        # os.rename(pdf_path, nuevo_nombre)