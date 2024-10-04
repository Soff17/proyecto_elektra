import fitz
import re
import os

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
    if (font_size > 34.0 and font_size < 41.0) and (font_flags == 20 or font_flags == 4):
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
                    
                    #Get SKUs
                    elif sku_pattern.findall(text):
                        sku = sku_pattern.findall(text)[0]
                        sku = sku.replace(".","")
                        skus.append(sku)
                        fin_producto = True
                    
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
                    
                    text_buffer = text

def extraer_imagenes_orden(output_imagenes, page, doc):
    images = page.get_image_info(hashes=True, xrefs=True)
    imagenes = []

    for img in images:
        xref = img['xref']
        if xref > 0:
            print(f"width: {img['width']} height: {img['height']}")
            if img['width'] > 495 and img['height'] > 495:
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
    urls_with_rect = []

    # Extraer URL y Rect de cada link y almacenar en lista de tuplas
    for link in links:
        if 'uri' in link and 'from' in link:
            url = link['uri']
            rect = link['from']
            coordenadas = [rect[0], rect[1], rect[2], rect[3]]
            # Limpiar el URL según tu lógica actual
            url = url.replace("https://www.elektra.mx/", "")
            url = url.replace("/","-")
            url = url.replace("?", "-")
            url = url.replace("=", "-")
            url = url.replace("#", "-")
            url = url.replace(":", "-")
            urls_with_rect.append((url, coordenadas))

    # Ordenar los URLs basados en las coordenadas rectangulares (x, y)
    # La clave de ordenación podría ser: primero en el eje Y, luego en el eje X
    urls_sorted = sorted(urls_with_rect, key=lambda x: (x[1][1], x[1][0]))

    # Extraer solo los URLs ya ordenados
    urls_sor = [url for url, _ in urls_sorted]
    for url in urls_sor:
        urls.append(url)
    

def guardar_informacion(output_arhivos, name_file, data):
    if len(name_file) > 173:
        filepath = f"{output_arhivos}/dummy{len(name_file)}.txt"
    else:
        filepath = f"{output_arhivos}/{name_file}.txt"
    # filepath = f"{output_arhivos}/{name_file}.txt"
    with open(filepath, "w", encoding="utf-8") as archivo:
        for dato in data:
            archivo.write(dato + "\n")
        
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

        #Extraccion de informacion
        extraer_informacion(page)

        #Se rompe cuando no se encuentran productos
        if len(titulos) == 0 or len(info) == 0:
            break
        
        #Extraccion de Urls
        get_urls(page)
        
        #Extraccion de imagenes
        extraer_imagenes_orden(output_imagenes, page, doc)

        ruta_directorio = os.path.join(ruta_base, 'archivos_dummy', f'{titulos[0]}')
        os.makedirs(ruta_directorio, exist_ok=True)

        #Guardado de informacion 
        for i in range(max(len(subtitulos), len(info), len(skus), len(vigencias), len(urls))):
            sku = skus[i] if i < len(skus) else ""
            vigencia = vigencias[i] if i < len(vigencias) else ""
            subtitulo = subtitulos[i] if i < len(subtitulos) else ""
            content = info[i] if i < len(info) else ""
            url = urls[i] if i < len(urls) else f"{page_num}_Dummy{i}"

            if sku:
                sku_num = "Sku: " + sku
                data = [subtitulo, sku_num, content, vigencia]
                guardar_informacion(ruta_directorio, f"{titulos[0]} {sku} {url}", data)

        #Particion pdf
        doc_pagina = fitz.open()
        doc_pagina.insert_pdf(doc, from_page=page_num, to_page=page_num)
        nombre_archivo_salida = f"{output_archivos}/{titulos[0]}.pdf"
        doc_pagina.save(nombre_archivo_salida)

        doc_pagina.close()
