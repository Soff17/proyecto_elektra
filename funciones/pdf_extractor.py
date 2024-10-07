import fitz
import re
import os
from funciones import watson_discovery as wd
from funciones import image_storage as st
import io

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

                    # print("\n-------------")
                    # print(f"Text Size: {text_size}")
                    # print(f"Text flags: {text_flags}")
                    # print(f"Text: {text}")
                    # print(f"Text buffer: {text_buffer}")
                    # print("-------------")

                    # Get nombre de categoria
                    if nombre_de_categoria(text_size, text_flags) and inicio_producto == False:
                        text = text.replace(" ", "_")
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

def extraer_imagenes_orden(bucket_name, bucket_folder, page, doc):
    images = page.get_image_info(hashes=True, xrefs=True)
    imagenes = []

    for img in images:
        xref = img['xref']
        if xref > 0:
            if img['width'] > 495 and img['height'] > 495:
                bbox_img = img['bbox']
                imagenes.append((xref, bbox_img))
    
    images_sorted = sorted(imagenes, key=lambda img: img[1][3], reverse=False)

    count = 0
    for xref, bbox in images_sorted:
        base_image = doc.extract_image(xref)
        image_bytes = base_image["image"]
        ext = base_image["ext"]
        
        if len(skus) > count:
            image_name = f"{skus[count]}.{ext}"
        else:
            image_name = f"producto_{count+1}.{ext}"

        # Guardar la imagen localmente en lugar de subirla al bucket
        ruta_imagen = os.path.join('./imagenes', image_name)
        with open(ruta_imagen, "wb") as f:
            f.write(image_bytes)
        
        # Subir la imagen directamente desde el buffer al bucket
        image_buffer = io.BytesIO(image_bytes)
        #st.upload_image_buffer(bucket_name, bucket_folder, image_name, image_buffer)

        print(f"Imagen {image_name} subida exitosamente al bucket.")
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
            url = url.replace("/","[")
            url = url.replace("?", "]")
            url = url.replace("=", "-")
            url = url.replace("#", "-")
            url = url.replace(":", "-")
            urls_with_rect.append((url, coordenadas))

    # Ordenar los URLs basados en las coordenadas rectangulares (x, y)
    urls_sorted = sorted(urls_with_rect, key=lambda x: (x[1][1], x[1][0]))

    # Extraer solo los URLs ya ordenados
    urls_sor = [url for url, _ in urls_sorted]
    for url in urls_sor:
        urls.append(url)

def guardar_informacion_a_discovery(titulo, name_file, data):
    # Reemplazar espacios con guiones bajos
    titulo = titulo.replace(" ", "_")
    
    # Generar el contenido del archivo como una cadena
    contenido_txt = "\n".join(data)
    
    # Subir directamente a IBM Watson Discovery
    from funciones import watson_discovery as wd  # Importar Watson Discovery
    
    # Usar una función similar a `añadir_documento` para subir el contenido
    # wd.añadir_documento_desde_contenido(contenido_txt, f"{titulo} {name_file}.txt", 'text/plain')
    print(f'se está subiendo "{titulo} {name_file}.txt"')

     # Crear el directorio si no existe
    if not os.path.exists('./output_files'):
        os.makedirs('./output_files')

    # Definir la ruta completa del archivo
    file_path = os.path.join('./output_files', f"{titulo} {name_file}.txt")

    # Guardar el contenido en un archivo local
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(contenido_txt)

    print(f'Archivo guardado localmente: "{file_path}"')
        
def particion_pdf(pdf_path, output_archivos):
    doc = fitz.open(pdf_path)
    
    for num_page in range(doc.page_count):
        doc_pagina = fitz.open()
        doc_pagina.insert_pdf(doc, from_page=num_page, to_page=num_page)
        nombre_archivo_salida = f"{output_archivos}/pagina_{num_page + 1}.pdf"
        doc_pagina.save(nombre_archivo_salida)
        
    doc_pagina.close()

def coincidir_url_con_sku(urls, skus):
    url_assignments = []
    
    for i in range(len(skus)):
        sku = skus[i] if i < len(skus) else None
        matched_url = None
        
        # Coincidencia con SKU
        for url in urls:
            if sku and sku in url:
                matched_url = url
                break

        # Sino coincide con SKU, dejar el que encontró originalmente
        if not matched_url and i < len(urls):
            matched_url = urls[i]
        elif not matched_url:
            matched_url = f"dummy-url-for-sku-{sku if sku else 'unknown'}"

        url_assignments.append(matched_url)
    
    return url_assignments

def procesar_pdf(pdf_buffer, bucket_name, carpeta_imagenes_bucket, carpeta_pdfs_bucket):
    # Abre el PDF desde el buffer en memoria
    doc = fitz.open(stream=pdf_buffer, filetype="pdf")

    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)

        titulos.clear()
        subtitulos.clear()
        info.clear()
        urls.clear()
        skus.clear()
        vigencias.clear()

        extraer_informacion(page)

        if len(titulos) == 0 or len(info) == 0:
            break

        get_urls(page)
        extraer_imagenes_orden(bucket_name, carpeta_imagenes_bucket, page, doc)

        assigned_urls = coincidir_url_con_sku(urls, skus)

        for i in range(max(len(subtitulos), len(info), len(skus), len(vigencias), len(assigned_urls))):
            sku = skus[i] if i < len(skus) else ""
            vigencia = vigencias[i] if i < len(vigencias) else ""
            subtitulo = subtitulos[i] if i < len(subtitulos) else ""
            content = info[i] if i < len(info) else ""
            url = assigned_urls[i] if i < len(assigned_urls) else f"{page_num}_Dummy{i}"

            if sku:
                sku_num = "Sku: " + sku
                data = [subtitulo, sku_num, content, vigencia]
                guardar_informacion_a_discovery(titulos[0], f"{sku} {url}", data)

        # Partición pdf, se guarda en un buffer en lugar de archivo físico
        doc_pagina = fitz.open()
        doc_pagina.insert_pdf(doc, from_page=page_num, to_page=page_num)
        nombre_archivo_pdf = f"{titulos[0].replace(' ', '_')}.pdf"

        # Crear un buffer de bytes
        pdf_buffer_output = io.BytesIO()
        doc_pagina.save(pdf_buffer_output)
        pdf_buffer_output.seek(0)  # Regresar al inicio del buffer

        # Subir el PDF directamente desde el buffer al bucket llamando a la función de tu script de storage
        # st.upload_pdf_buffer(bucket_name, carpeta_pdfs_bucket, nombre_archivo_pdf, pdf_buffer_output)

        print(f"PDF {nombre_archivo_pdf} subido exitosamente al bucket.")

        doc_pagina.close()
