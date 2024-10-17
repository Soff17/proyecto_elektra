import fitz
import re
import os
from funciones import watson_discovery as wd
from funciones import image_storage as st
import io
import pandas as pd
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as ExcelImage

sku_pattern = re.compile(r'Sku:\s*(\S+)')
sku_pattern_2 = re.compile(r'Sku de referencia:\s*(\S+)')
sku_pattern_3 = re.compile(r'Sku´s de referencia:\s*(\S+)')
vigencia_pattern = re.compile(r'Vigencia:\s*(.+)')
delete_pattern = re.compile(r'(Da clic aquí)s?')
delete_pattern2 = re.compile(r'(¡Cómpralo ya!)s?')
precio_pattern = re.compile(r'Contado\s*(\S+)')
precio_pattern2 = re.compile(r'^\$\d{1,3}(,\d{3})+(\.\d{2})?$', re.MULTILINE)
bono_pattern = re.compile(r'(¡ B O N O  D E  R E G A L O)s?')
bono_pattern2 = re.compile(r'D E\s*(.+)')

titulos = []
subtitulos = []
info = []
urls = []
skus = []
vigencias = []
precios = []
cupones = []

# Definir listas para almacenar la información que vamos a exportar
data_reporte = {
    'SKU': [],
    'Subtítulo': [],
    'Descripción': [],
    'Vigencia': [],
    'URL': []
}

def nombre_de_categoria(font_size, font_flags):
    if (font_size > 43) and font_flags == 20:
        return True
    return False

def nombre_del_producto(font_size, font_flags):
    if (font_size > 31.5 and font_size < 41.0) and (font_flags == 20 or font_flags == 4):
        return True
    return False

def extraer_informacion(page):
    blocks = page.get_text("dict", sort=True)["blocks"]

    inicio_productos = False
    inicio_producto = False
    fin_producto = False
    datos = ''
    sku_positions = []

    for block in blocks:
        if 'lines' in block:
            for line in block['lines']:
                for span in line['spans']:
                    text = span['text'].strip()
                    text_size = span['size']
                    text_flags = span['flags']
                    text_y_position = span['bbox'][1]

                    # Get nombre de categoría
                    if nombre_de_categoria(text_size, text_flags) and not inicio_producto:
                        text = text.replace(" ", "_")
                        if not inicio_productos and len(titulos) > 0:
                            titulos[-1] += " " + text
                        else:
                            titulos.append(text)

                    # Get nombre de producto
                    elif nombre_del_producto(text_size, text_flags):
                        if inicio_producto:
                            subtitulos[-1] += " " + text
                        else:
                            subtitulos.append(text)
                            inicio_producto = True
                            datos = ''
                        inicio_productos = True

                    # Get SKUs
                    elif sku_pattern.findall(text):
                        sku = sku_pattern.findall(text)[0].replace(".", "")
                        skus.append(sku)
                        sku_positions.append((sku, text_y_position))  # Registrar posición del SKU
                        fin_producto = True

                    elif sku_pattern_2.findall(text):
                            sku = sku_pattern_2.findall(text)[0].replace(".", "")
                            skus.append(sku)
                            sku_positions.append((sku, text_y_position))  # Registrar posición del SKU
                            fin_producto = True
                            inicio_producto = True
                            subtitulos.append("Producto")
                            precios.append(f"Pago de contado: SA")
                    
                    elif sku_pattern_3.findall(text):
                        text = text.replace('Sku´s de referencia: ', '')
                        skus.append(text)
                        sku = re.findall(r'\d+', text)
                        for num in sku:
                            sku_positions.append((num, text_y_position))
                        fin_producto = True
                        inicio_producto = True
                        subtitulos.append("Producto")
                        precios.append(f"Pago de contado: SA")

                    # Get Vigencias
                    elif vigencia_pattern.findall(text) and fin_producto and inicio_producto:
                        vigencias.append(text)
                        info.append(datos)
                        datos = ''
                        fin_producto = False
                        inicio_producto = False

                    # Delete info extra
                    elif delete_pattern.findall(text) or delete_pattern2.findall(text) or precio_pattern.findall(text):
                        continue
                    
                    # Get Precio precio_del_producto(text_size, text_flags)
                    elif precio_pattern2.findall(text):
                        precios.append(f"Pago de contado: {text}")

                    # Get el cupon
                    elif bono_pattern.findall(text):
                        cupones.append(text)
                        
                    elif bono_pattern2.findall(text) and len(cupones) > 0:
                        texto_original = cupones[-1] + " " + text
                        texto_sin_espacios = re.sub(r'\s(?=[A-Za-z0-9])', '', texto_original)
                        texto_intermedio = re.sub(r'\s+\$', ' $', texto_sin_espacios)
                        texto_corregido = re.sub(r'RegaloDe', 'Regalo de', texto_intermedio, flags=re.IGNORECASE)
                        texto_sin_caracteres_especiales = re.sub(r'[!¡*]', '', texto_corregido).strip()
                        texto_final = texto_sin_caracteres_especiales.lower()
                        texto_final = 'Bono' + texto_final[4:]

                        cupones[-1] = texto_final

                    # Get Info producto
                    else:
                        if datos == '':
                            datos = text
                        else:
                            datos += " " + text

    return sku_positions  # Devolver las posiciones de los SKUs para asignar imágenes

def extraer_imagenes_orden(bucket_name, bucket_folder, page, doc, sku_positions):
    images = page.get_image_info(hashes=True, xrefs=True)
    imagenes = []

    for img in images:
        
        xref = img['xref']
        if xref > 0:
            if img['width'] > 311 and img['height'] > 311:
                bbox_img = img['bbox']
                imagenes.append((xref, bbox_img))

    # Ordenamos las imágenes por su posición Y en la página
    images_sorted = sorted(imagenes, key=lambda img: img[1][3], reverse=False)

    # Asignar imágenes al SKU más cercano basado en las posiciones
    count = 0
    for xref, bbox in images_sorted:
        base_image = doc.extract_image(xref)
        image_bytes = base_image["image"]
        ext = base_image["ext"]

        # Encontrar el SKU más cercano basado en la posición Y
        closest_sku = find_closest_sku(sku_positions, bbox[3])

        if closest_sku:
            image_name = f"{closest_sku}.{ext}"
            sku_positions = [tupla for tupla in sku_positions if tupla[0] != closest_sku]
        else:
            image_name = f"dummy_{count}.{ext}"
        
        # if count < len(skus):
        #     image_name = f"{skus[count]}.{ext}"
        # else:
        #     image_name = f"dummy_{count}.{ext}"

        # Guardar la imagen localmente en lugar de subirla al bucket
        ruta_imagen = os.path.join('./imagenes', image_name)
        with open(ruta_imagen, "wb") as f:
            f.write(image_bytes)

        # Subir la imagen directamente desde el buffer al bucket
        image_buffer = io.BytesIO(image_bytes)
        # st.upload_image_buffer(bucket_name, bucket_folder, image_name, image_buffer)

        print(f"Imagen {image_name} subida exitosamente al bucket.")
        count += 1

def find_closest_sku(sku_positions, image_y_position):
    # Encuentra el SKU más cercano basado en la posición Y
    closest_sku = None
    min_distance = float('inf')

    for sku, sku_position in sku_positions:
        distance = abs(sku_position - image_y_position)
        if distance < min_distance:
            closest_sku = sku
            min_distance = distance
            sku_positions = [tupla for tupla in sku_positions if tupla[0] != closest_sku]

    return closest_sku

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

def ajustar_longitudes_listas():
    # Encontrar la longitud máxima entre todas las listas
    max_length = max(len(skus), len(subtitulos), len(info), len(vigencias), len(urls))

    # Rellenar con valores vacíos en caso de que alguna lista sea más corta
    skus.extend([""] * (max_length - len(skus)))
    subtitulos.extend([""] * (max_length - len(subtitulos)))
    info.extend([""] * (max_length - len(info)))
    vigencias.extend([""] * (max_length - len(vigencias)))
    urls.extend([""] * (max_length - len(urls)))

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
        precios.clear()

        # Llamada a extraer_informacion para obtener las posiciones de los SKUs
        sku_positions = extraer_informacion(page)

        if len(titulos) == 0 or len(info) == 0:
            break

        get_urls(page)

        # Pasar sku_positions a la función extraer_imagenes_orden
        extraer_imagenes_orden(bucket_name, carpeta_imagenes_bucket, page, doc, sku_positions)

        for i in range(max(len(subtitulos), len(info), len(skus), len(vigencias), len(urls), len(precios))):
            sku = skus[i] if i < len(skus) else ""
            vigencia = vigencias[i] if i < len(vigencias) else ""
            subtitulo = f"Producto: {subtitulos[i]}" if i < len(subtitulos) else ""
            if i < len(info):
                datos_producto = info[i]

                # Inicializar el contenido con "Pago semanal" al inicio
                content = f"Pago semanal: {datos_producto}"

                # Verificar si existe "enganche" en el texto
                if 'enganche' in datos_producto:
                    content = content.replace('enganche', 'enganche\nDescuento:')
                
                # Verificar si existe "pago inicial {cantidad}" en el texto
                elif re.search(r'pago inicial \d+', datos_producto):
                    # Insertar el salto de línea antes del descuento después del "pago inicial"
                    content = re.sub(r'(pago inicial \d+)', r'\1\nDescuento:', content)

                # Quitar cualquier texto adicional como "es la mejor opción para pagar menos"
                content = content.replace('es la mejor opción para pagar menos', '')

            else:
                content = ""
                
            precio = precios[i] if i < len(precios) else ""
            url = urls[i] if i < len(urls) else f"{page_num}_Dummy{i}"
            categoria = f"Categoria: {titulos[0]}"
            cupon = cupones[i] if i < len(cupones) else "Sin Cupones"

            if sku:
                sku_num = "Sku: " + sku
                data = [sku_num, categoria, subtitulo, cupon, content, precio, vigencia]

        # Generar un reporte Excel para cada categoría
        if len(titulos) > 0:
            generar_reporte_excel(titulos[0])

        # Partición pdf, se guarda en un buffer en lugar de archivo físico
        doc_pagina = fitz.open()
        doc_pagina.insert_pdf(doc, from_page=page_num, to_page=page_num)
        nombre_archivo_pdf = f"{titulos[0].replace(' ', '_')}.pdf"

        # Crear un buffer de bytes
        pdf_buffer_output = io.BytesIO()
        doc_pagina.save(pdf_buffer_output)
        pdf_buffer_output.seek(0)  # Regresar al inicio del buffer

        # Guardar el PDF localmente antes de subirlo al bucket
        ruta_local_pdf = os.path.join('./pdfs_locales', nombre_archivo_pdf)
        if not os.path.exists('./pdfs_locales'):
            os.makedirs('./pdfs_locales')
        with open(ruta_local_pdf, 'wb') as f:
            f.write(pdf_buffer_output.getvalue())

        print(f"PDF {nombre_archivo_pdf} guardado localmente en: {ruta_local_pdf}")

        # Subir el PDF directamente desde el buffer al bucket llamando a la función de tu script de storage
        # st.upload_pdf_buffer(bucket_name, carpeta_pdfs_bucket, nombre_archivo_pdf, pdf_buffer_output)

        print(f"PDF {nombre_archivo_pdf} subido exitosamente al bucket.")

        doc_pagina.close()

def generar_reporte_excel(titulo_categoria):
    # Ajustar las longitudes de las listas antes de generar el reporte
    ajustar_longitudes_listas()

    # Convertir los datos almacenados en un DataFrame de pandas
    df = pd.DataFrame({
        'SKU': skus,
        'Subtítulo': subtitulos,
        'Descripción': info,
        'Vigencia': vigencias,
        'URL': urls
    })

    # Añadir la columna "URL Coincide con SKU"
    df['URL Coincide con SKU'] = df.apply(lambda row: row['SKU'] in row['URL'], axis=1)

    # Añadir una columna que indique si el SKU tiene una imagen asociada
    def tiene_imagen(sku):
        extensiones_imagen = ['jpeg']
        for ext in extensiones_imagen:
            if os.path.exists(f"./imagenes/{sku}.{ext}"):
                return True
        return False

    df['Tiene Imagen'] = df['SKU'].apply(tiene_imagen)

    # Crear un nombre de archivo único para cada categoría
    nombre_archivo_excel = f"./reportes/reporte_{titulo_categoria.replace(' ', '_')}.xlsx"

    # Guardar el DataFrame como archivo Excel sin imágenes aún
    df.to_excel(nombre_archivo_excel, index=False)

    # Cargar el archivo Excel generado con openpyxl
    workbook = load_workbook(nombre_archivo_excel)
    sheet = workbook.active

    # Ajustar el ancho de las columnas para "SKU", "URL Coincide con SKU" y "Tiene Imagen"
    col_sku = 1
    col_url_coincide = sheet.max_column - 1
    col_tiene_imagen = sheet.max_column 

    # Ajustar los anchos de las columnas
    sheet.column_dimensions[sheet.cell(row=1, column=col_sku).column_letter].width = 30 
    sheet.column_dimensions[sheet.cell(row=1, column=col_url_coincide).column_letter].width = 30
    sheet.column_dimensions[sheet.cell(row=1, column=col_tiene_imagen).column_letter].width = 30

    # Añadir imágenes a una nueva columna al final
    col_img = sheet.max_column + 1
    sheet.cell(row=1, column=col_img).value = "Imagen"

    # Añadir imágenes en la nueva columna para cada SKU
    for index, sku in enumerate(skus, start=2):
        imagen_path = None
        extensiones_imagen = ['jpeg']
        for ext in extensiones_imagen:
            imagen_path = f"./imagenes/{sku}.{ext}"
            if os.path.exists(imagen_path):
                break

        if imagen_path and os.path.exists(imagen_path):
            # Insertar la imagen en la última columna para cada fila
            img = ExcelImage(imagen_path)
            img.width = 100
            img.height = 100

            # Calcular la altura de la fila según el alto de la imagen
            fila = index
            altura_fila = img.height * 0.75  
            sheet.row_dimensions[fila].height = altura_fila 

            img_anchor = sheet.cell(row=fila, column=col_img).coordinate 
            sheet.add_image(img, img_anchor)

    # Guardar el archivo Excel con las imágenes insertadas
    workbook.save(nombre_archivo_excel)

    print(f'Reporte generado y guardado en: {nombre_archivo_excel}')

