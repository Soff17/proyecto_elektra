import fitz
import re

sku_pattern = re.compile(r'Sku:\s*(\S+)')
vigencia_pattern = re.compile(r'Vigencia:\s*(.+)')

titulos = []
subtitulos = []
info = []
urls = []
skus = []
vigencias = []

def nombre_de_categoria(font_size, font_flags):
    if font_size == 58 and font_flags == 20:
        return True
    return False

def nombre_del_producto(font_size, font_flags):
    if (font_size == 35.0 or font_size == 40.0) and (font_flags == 20 or font_flags == 4):
        return True
    return False

def extraer_informacion(page):
    print("Obteniendo el texto...")

    blocks = page.get_text("dict",sort=True)["blocks"]
    text_buffer = ""
    inicio_producto = False
    fin_producto = False

    for block in blocks:
        if 'lines' in block:
            for line in block['lines']:
                for span in line['spans']:

                    text = span['text'].strip()
                    text_size = span['size']
                    text_flags = span['flags']
                    
                    #Get nombre de categoria
                    if nombre_de_categoria(text_size, text_flags):
                        titulos.append(text)

                    #Get nombre de prodcuto
                    elif nombre_del_producto(text_size, text_flags):
                        if text_buffer in subtitulos:
                            subtitulos[len(subtitulos)-1] += " " + text
                        else:
                            subtitulos.append(text)
                        inicio_producto = True
                    
                    #Get SKUs
                    elif sku_pattern.findall(text):
                        sku = sku_pattern.findall(text)[0]
                        sku = sku.replace(".","")
                        skus.append(sku)
                    
                    #Get Vigencias
                    elif vigencia_pattern.findall(text):
                        vigencias.append(text)
                        fin_producto = True
                        inicio_producto = False

                    elif inicio_producto:
                        if fin_producto or len(info) == 0:
                            info.append(text)
                            fin_producto = False
                        else:
                            word = ' ' + text
                            info[len(info)-1] += word
    
                    text_buffer = text

def extraer_imagenes_orden(output_imagenes, page, doc):
    print("Obteniendo las imagenes...")
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
    print("Obteniendo las urls...")
    links = page.get_links()
    for link in links:
        url = link['uri']
        url = url.replace("https://www.elektra.mx/", "")
        url = url.replace(":","-")
        url = url.replace("/", "-")
        url = url.replace("?", "-")
        url = url.replace("%", "-")
        urls.append(url)

def guardar_informacion(output_arhivos, name_file, data):
    filepath = f"{output_arhivos}/{name_file}.txt"
    with open(filepath, "w", encoding="utf-8") as archivo:
        for dato in data:

            archivo.write(dato + "\n")
        
   
def procesar_pdf(pdf_path, output_archivos, output_imagenes):
    doc = fitz.open(pdf_path)

    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)

        subtitulos.clear()
        info.clear()
        urls.clear()
        skus.clear()
        vigencias.clear()

        extraer_informacion(page)
        get_urls(page)
        extraer_imagenes_orden(output_imagenes, page, doc)
        
        print("Guardando la informacion...")
        for i in range(max(len(subtitulos), len(info), len(skus), len(vigencias), len(urls))):
            sku = skus[i] if i < len(skus) else ""
            vigencia = vigencias[i] if i < len(vigencias) else ""
            subtitulo = subtitulos[i] if i < len(subtitulos) else ""
            content = info[i] if i < len(info) else ""
            url = urls[i] if i < len(urls) else f"{page_num}_Dummy{i}"

            if sku:
                sku = "Sku: **" + sku + "**"
                data = [subtitulo, sku, content, vigencia]
                guardar_informacion(output_archivos, url, data)
        
            
