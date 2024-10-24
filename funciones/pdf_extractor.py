import fitz
import re
import os
from funciones import watson_discovery as wd
from funciones import image_storage as st
from funciones import elastict_search as es
import io
import pandas as pd
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as ExcelImage
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from openpyxl.styles import PatternFill
from unidecode import unidecode

sku_pattern = re.compile(r'Sku:\s*(\S+)')
sku_pattern_2 = re.compile(r'Sku de referencia:\s*(\S+)')
sku_pattern_3 = re.compile(r'Sku[´\'s]{0,4} de referencia:\s*(\S+)|Sku:\s*(\S+)')
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
nueva_data_productos = []

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

def nombre_del_producto(font_size, font_flags, categoria):
    if categoria == "Planes":
        return (font_size > 28 and font_size < 34) and (font_flags == 20 or font_flags == 4)
    else:
        return (font_size > 31.5 and font_size < 41.0) and (font_flags == 20 or font_flags == 4)


def extraer_informacion(page):
    blocks = page.get_text("dict", sort=True)["blocks"]

    inicio_productos = False
    inicio_producto = False
    fin_producto = False
    datos = ''
    sku_positions = []

    # Nuevo patrón para capturar precios precedidos por "Contado"
    precio_pattern3 = re.compile(r'Contado\s*\$\d{1,3}(,\d{3})*(\.\d{2})?')

    # Determinar la categoría antes de procesar el resto de la información
    categoria = extraer_categoria(page)

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
                        if not inicio_productos and len(titulos) > 0:
                            titulos[-1] += " " + text
                        else:
                            titulos.append(text)

                    # Get nombre de producto
                    # Get nombre de producto usando la nueva condición de la categoría
                    elif nombre_del_producto(text_size, text_flags, categoria):
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
                        precios.append(f"Pago de contado: NA")

                    elif sku_pattern_3.findall(text):
                        text = text.replace('Sku´s de referencia: ', '')
                        skus_encontrados = re.findall(r'\d+', text)

                        if skus_encontrados:
                            primer_sku = skus_encontrados[0]

                            # Verificar si el primer SKU ya está en la lista de SKUs
                            if primer_sku not in skus:
                                skus.append(primer_sku)
                                sku_positions.append((primer_sku, text_y_position))  # Registrar posición del primer SKU
                            elif len(skus_encontrados) > 1:
                                # Si el primer SKU ya existe, usar el segundo
                                segundo_sku = skus_encontrados[1]
                                skus.append(segundo_sku)
                                sku_positions.append((segundo_sku, text_y_position))  # Registrar posición del segundo SKU

                        fin_producto = True
                        inicio_producto = True
                        subtitulos.append("Producto")
                        precios.append(f"Pago de contado: NA")

                    # Get Vigencias
                    elif vigencia_pattern.findall(text) and fin_producto and inicio_producto:
                        vigencias.append(text)
                        info.append(datos)
                        datos = ''
                        fin_producto = False
                        inicio_producto = False

                    # Delete info extra
                    elif delete_pattern.findall(text) or delete_pattern2.findall(text):
                        continue
                    
                    # Get Precio precio_del_producto(text_size, text_flags)
                    elif precio_pattern3.findall(text):
                        precio = precio_pattern3.findall(text)[0]  # Capturar el precio completo
                        precios.append(f"Pago de contado: {precio}")

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
            if img['width'] > 269.5 and img['height'] > 269.5:
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
            image_name = f"{closest_sku}.jpeg"
        else:
            image_name = f"producto_{count+1}.{ext}"

        # Obtener la carpeta de descargas del usuario
        downloads_folder = get_downloads_folder()

        # Crear el directorio 'imagenes' dentro de la carpeta Descargas si no existe
        ruta_imagenes = os.path.join(downloads_folder, 'imagenes')
        if not os.path.exists(ruta_imagenes):
            os.makedirs(ruta_imagenes)

        # Guardar la imagen en la nueva carpeta de Descargas
        ruta_imagen = os.path.join(ruta_imagenes, image_name)
        with open(ruta_imagen, "wb") as f:
            f.write(image_bytes)
            
        # Subir la imagen directamente desde el buffer al bucket
        image_buffer = io.BytesIO(image_bytes)
        #st.upload_image_buffer(bucket_name, bucket_folder, image_name, image_buffer)

        print(f"Imagen {image_name} subida exitosamente al bucket.")
        count += 1

    # Si no se encontraron imágenes para algún SKU, usa 'default.jpeg' desde Descargas/imagenes
    for sku, _ in sku_positions:
        ruta_imagen_sku = os.path.join(ruta_imagenes, f"{sku}.jpeg")
        if not os.path.exists(ruta_imagen_sku):
            ruta_default = './default.jpeg'
            if os.path.exists(ruta_default):
                ruta_imagen = os.path.join(ruta_imagenes, f"{sku}.jpeg")
                
                # Copiar localmente la imagen predeterminada
                with open(ruta_default, "rb") as f_default, open(ruta_imagen, "wb") as f_sku:
                    f_sku.write(f_default.read())
                print(f"Imagen predeterminada asignada al SKU {sku} localmente.")

            else:
                print(f"Imagen predeterminada no encontrada.")
    ''''
    # Si no se encontraron imágenes para algún SKU, usa 'default.jpeg' desde el bucket
    for sku, _ in sku_positions:
        # Verificar si la imagen del SKU ya fue subida
        #image_exists = st.check_image_in_bucket(bucket_name, bucket_folder, f"{sku}.jpeg")
        if not image_exists:
            # Subir 'default.jpeg' desde el almacenamiento predeterminado
            ruta_default = './default.jpeg'
            default_image_buffer = get_default_image_buffer_from_local("./default.jpeg")
            if default_image_buffer:
                # st.upload_image_buffer(bucket_name, bucket_folder, f"{sku}.jpeg", default_image_buffer)
                print(f"Imagen predeterminada asignada al SKU {sku} subida al bucket.")
                ruta_imagen = os.path.join(ruta_imagenes, f"{sku}.jpeg")
                # Copiar localmente la imagen predeterminada
                with open(ruta_default, "rb") as f_default, open(ruta_imagen, "wb") as f_sku:
                    f_sku.write(f_default.read())
            else:
                print(f"Imagen predeterminada no encontrada.")
    '''
def get_default_image_buffer_from_local(default_image_path="./default.jpeg"):
    try:
        # Abrir la imagen en modo binario
        with open(default_image_path, "rb") as default_image_file:
            # Cargar la imagen en un buffer
            image_buffer = io.BytesIO(default_image_file.read())
            image_buffer.seek(0)  # Reiniciar el puntero del buffer al inicio
            print(f"Imagen predeterminada '{default_image_path}' cargada exitosamente desde la raíz del proyecto.")
            return image_buffer
    except FileNotFoundError:
        print(f"Imagen predeterminada '{default_image_path}' no encontrada en la raíz del proyecto.")
        return None

def find_closest_sku(sku_positions, image_y_position):
    closest_sku = None
    min_distance = float('inf')
    # Set a threshold to avoid matching distant images (e.g., set to 200)
    threshold = 200  

    for sku, sku_position in sku_positions:
        distance = abs(sku_position - image_y_position)
        if distance < min_distance and distance < threshold:
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
            urls_with_rect.append((url, coordenadas))

    # Ordenar los URLs basados en las coordenadas rectangulares (x, y)
    urls_sorted = sorted(urls_with_rect, key=lambda x: (x[1][1], x[1][0]))

    # Extraer solo los URLs ya ordenados
    urls_sor = [url for url, _ in urls_sorted]
    for url in urls_sor:
        urls.append(url)

def guardar_informacion_a_elasticsearch(name_file, data):   
    # Generar el contenido del archivo como una cadena
    contenido_txt = "\n".join(data)

    # Subir directamente a Elasticsearch
    documento = {
        "titulo": f"{name_file}",
        "contenido": contenido_txt
    }
    
    # Subir el documento a Elasticsearch
    #es.indexar_documento("catalogo", f"{titulo} {name_file}", documento) 
    print(f'Se está subiendo "{name_file}.txt" a Elasticsearch.')

    # Guardar el archivo localmente
    downloads_folder = get_downloads_folder()
    ruta_output_files = os.path.join(downloads_folder, 'output_files')
    
    if not os.path.exists(ruta_output_files):
        os.makedirs(ruta_output_files)

    # Definir la ruta completa del archivo
    file_path = os.path.join(ruta_output_files, f"{name_file}.txt")

    # Guardar el contenido en un archivo local
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(contenido_txt)

def guardar_informacion_a_discovery(titulo, name_file, data):
    # Reemplazar espacios con guiones bajos
    titulo = re.sub(r'[^A-Za-zÁÉÍÓÚáéíóúÑñ\s]', '', titulo).strip()
    titulo = unidecode(titulo).replace(" ", "_")  # Quitar acentos con unidecode
    
    # Generar el contenido del archivo como una cadena
    contenido_txt = "\n".join(data)
    
    # Subir directamente a IBM Watson Discovery
    from funciones import watson_discovery as wd  # Importar Watson Discovery
    
    # Usar una función similar a `añadir_documento` para subir el contenido
    # wd.añadir_documento_desde_contenido(contenido_txt, f"{titulo} {name_file}.txt", 'text/plain')
    print(f'se está subiendo "{titulo} {name_file}.txt"')

    # Obtener la carpeta de descargas del usuario
    downloads_folder = get_downloads_folder()

    # Crear el directorio 'output_files' dentro de la carpeta Descargas si no existe
    ruta_output_files = os.path.join(downloads_folder, 'output_files')
    if not os.path.exists(ruta_output_files):
        os.makedirs(ruta_output_files)

    # Definir la ruta completa del archivo
    file_path = os.path.join(ruta_output_files, f"{titulo} {name_file}.txt")

    # Guardar el contenido en un archivo local
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(contenido_txt)
        
def particion_pdf(pdf_buffer, bucket_name, bucket_folder):
    # Abre el PDF desde el buffer en memoria
    doc = fitz.open(stream=pdf_buffer, filetype="pdf")
    
    # Obtener la carpeta de descargas del usuario
    downloads_folder = get_downloads_folder()

    # Crear la carpeta 'pdfs_finales' dentro de la carpeta Descargas si no existe
    ruta_pdfs_finales = os.path.join(downloads_folder, 'pdfs_finales')
    if not os.path.exists(ruta_pdfs_finales):
        os.makedirs(ruta_pdfs_finales)

    for num_page in range(doc.page_count):
        # Crear un nuevo PDF con solo una página
        doc_pagina = fitz.open()
        doc_pagina.insert_pdf(doc, from_page=num_page, to_page=num_page)
        
        # Extraer el nombre de la categoría de la página
        page = doc.load_page(num_page)
        categoria = extraer_categoria(page)

        # Sanitizar el nombre de la categoría para ser usado en el nombre de archivo
        nombre_categoria_sanitizado = sanitizar_nombre_categoria(categoria)

        # Nombre del archivo para la página basado en la categoría
        nombre_archivo_pdf = f"{nombre_categoria_sanitizado}.pdf"
        
        # Guardar localmente en la carpeta 'pdfs_finales'
        ruta_local_pdf = os.path.join(ruta_pdfs_finales, nombre_archivo_pdf)
        doc_pagina.save(ruta_local_pdf)
        print(f"Página {num_page + 1} guardada como {ruta_local_pdf}")
        
        # Crear un buffer de bytes para almacenar el PDF en memoria
        pdf_buffer_output = io.BytesIO()
        doc_pagina.save(pdf_buffer_output)
        pdf_buffer_output.seek(0)  # Regresar al inicio del buffer

        # Subir el PDF al bucket directamente desde el buffer
        # st.upload_pdf_buffer(bucket_name, bucket_folder, nombre_archivo_pdf, pdf_buffer_output)
        print(f"PDF {nombre_archivo_pdf} subido exitosamente al bucket.")

        doc_pagina.close()

    doc.close()

def extraer_categoria(page):
    # Lógica para extraer el nombre de la categoría desde la página
    blocks = page.get_text("dict", sort=True)["blocks"]
    
    for block in blocks:
        if 'lines' in block:
            for line in block['lines']:
                for span in line['spans']:
                    text = span['text'].strip()
                    text_size = span['size']
                    text_flags = span['flags']

                    # Verificar si el texto coincide con el nombre de la categoría
                    if nombre_de_categoria(text_size, text_flags):
                        return text
    return "Categoria_Desconocida"

def sanitizar_nombre_categoria(categoria):
    # Reemplazar solo los caracteres no válidos, mantener los espacios y convertirlos a guiones bajos
    categoria_sanitizada = re.sub(r'[^A-Za-zÁÉÍÓÚáéíóúÑñ\s]', '', categoria).strip()
    nombre_con_guiones_bajos = unidecode(categoria_sanitizada).replace(" ", "_")
    
    # Si hay varios guiones bajos, cortar en el primer guión bajo
    nombre_final = nombre_con_guiones_bajos.split('_')[0]
    
    # Excepciones para nombres específicos
    if nombre_final == "Home":
        nombre_final = "Home_Audio"
    elif nombre_final == "Linea":
        nombre_final = "Linea_Blanca"
    
    return nombre_final

def ajustar_longitudes_listas():
    # Encontrar la longitud máxima entre todas las listas
    max_length = max(len(skus), len(subtitulos), len(info), len(vigencias), len(urls), len(nueva_data_productos))

    # Rellenar con valores vacíos en caso de que alguna lista sea más corta
    skus.extend([""] * (max_length - len(skus)))
    subtitulos.extend([""] * (max_length - len(subtitulos)))
    info.extend([""] * (max_length - len(info)))
    vigencias.extend([""] * (max_length - len(vigencias)))
    urls.extend([""] * (max_length - len(urls)))
    
    # Ajustar también la lista 'nueva_data_productos' para que coincida
    nueva_data_productos.extend([["", "", "", "", "", ""]] * (max_length - len(nueva_data_productos)))

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
        cupones.clear()  # También limpia la lista de cupones si es necesario

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
            subtitulo = subtitulos[i] if i < len(subtitulos) else ""
            datos_producto = info[i] if i < len(info) else ""

            if titulos[0] == "Planes":
                subtitulo = re.sub(r'^.*?Llévate un', '', subtitulo).strip()
                subtitulo = re.sub(r'con\s.*', '', subtitulo).strip()

                cupon = "Sin Cupones"

                # Corregir el formato de "95 $ Desde x 128 semanas" a "95 $ x 128 semanas"
                datos_producto = re.sub(r'(\d+\s*\$)\s*Desde\s*x\s*(\d+\s*semanas)', r'\1 x \2', datos_producto)

                # Corregir el formato de "85 $ $ semanales Desde" a "85 $ semanales" con salto de línea
                datos_producto = re.sub(r'(\d+\s*\$)\s*\$\s*semanales\s*Desde', r'\1 semanales\n', datos_producto)

                # Corregir el formato de "110 $ semanales Desde" a "110 $ semanales" con salto de línea
                datos_producto = re.sub(r'(\d+\s*\$)\s*semanales\s*Desde', r'\1 semanales\n', datos_producto)

                if 'Incluye:' in datos_producto:
                    partes = datos_producto.split('Incluye:', 1)
                    primera_parte = partes[0].strip()
                    resto = partes[1].strip() if len(partes) > 1 else ''
                    datos_producto = f"{primera_parte}\nIncluye: {resto}"

                if 'Incluye:' not in datos_producto:
                    partes = datos_producto.split('\n', 1)
                    primera_parte = partes[0]
                    resto = partes[1] if len(partes) > 1 else ''
                    resto = resto.replace('Incluye:', '').strip()
                    datos_producto = f"{primera_parte}\nIncluye: {resto}"
                
                vigencia = vigencia.replace('al comprar solo con', '').strip()

            # Aplicar las transformaciones comunes independientemente del subtítulo
            datos_producto = datos_producto.replace('es la mejor opción para pagar menos', '')  # Eliminar esta frase
            datos_producto = datos_producto.replace('con tu', 'con tu préstamo Elektra')
            datos_producto = datos_producto.replace('Total a pagar con Préstamo Elektra', '\nTotal a pagar con Préstamo Elektra')

            # Eliminar frases no deseadas
            datos_producto = re.sub(r'Recuerda que el uso de casco.*', '', datos_producto)
            datos_producto = re.sub(r'Sku´s participantes:.*', '', datos_producto)
            datos_producto = re.sub(r'Sku´s\s+que no participan:.*', '', datos_producto)
            datos_producto = re.sub(r'Modelos seleccionados:.*', '', datos_producto)
            datos_producto = datos_producto.replace('Precio total a pagar a crédito:', '\nPrecio total a pagar a crédito:')
            
            # Verificar si el subtítulo es 'Producto'
            if subtitulo == 'Producto':
                subtitulo = "Producto: Producto"
                content = f"Pago semanal: NA\nDescuento: {datos_producto}\nPago de contado: NA"
            else:
                # Si no es 'Producto', aplicar las transformaciones habituales
                match = re.search(r'^(.*?)(\d+ ?\$|\$\d+)', datos_producto)
                if match:
                    subtitulo = f"Producto: {subtitulo} {match.group(1).strip()}"
                    datos_producto = datos_producto.replace(match.group(1), "").strip()
                else:
                    subtitulo = f"Producto: {subtitulo}"
                
                # Revisar y ajustar las frases que preceden "abono semanal", "descuento" o "descuento en abono semanal"
                if 'descuento en abono semanal' in datos_producto:
                    datos_producto = datos_producto.replace('descuento en abono semanal', 'descuento en abono semanal')
                elif 'Descuento' in datos_producto or 'descuento' in datos_producto:
                    datos_producto = datos_producto.replace('descuento', 'descuento\nPago de contado:')
                if 'abono semanal' in datos_producto or 'Abono semanal' in datos_producto:
                    datos_producto = datos_producto.replace('abono semanal', 'abono semanal\nPago de contado:')

                # Asignar el contenido modificado
                content = f"{datos_producto}"

                # Patrón para el formato de pago semanal
                pago_semanal_pattern = re.compile(r'(\$?\d+)\s*x\s*(\d+)\s*semanas\s*(\$\d{1,3}(?:,\d{3})*)\s*de\s*pago\s*inicial\s*(\d+)(.*)')
            
                # Buscar y reemplazar el formato de pago semanal
                match = pago_semanal_pattern.search(datos_producto)
                if match:
                    cantidad4 = match.group(4)  # Se usa cantidad4 en lugar de cantidad1
                    semanas = match.group(2)
                    pago_inicial = match.group(3)
                    texto_adicional = match.group(5).strip()

                    # Verificar si el texto adicional contiene "Descuento"
                    if 'Descuento' in texto_adicional or 'descuento' in texto_adicional:
                        texto_adicional = f"\nDescuento: {texto_adicional}"

                    nuevo_texto = f"${cantidad4} x {semanas} semanas {pago_inicial} de pago inicial"
                    datos_producto = datos_producto.replace(match.group(0), nuevo_texto + texto_adicional)
                    content = f"Pago semanal: {datos_producto}"

                # Revisar si 'enganche' aparece en datos_producto
                if 'enganche' in datos_producto:
                    datos_producto = datos_producto.replace('enganche', 'enganche\nDescuento:')
                    content = f"Pago semanal: {datos_producto}"

                # Revisar si hay 'pago inicial' en datos_producto
                elif re.search(r'pago inicial \d+', datos_producto):
                    datos_producto = re.sub(r'(pago inicial \d+)', r'\1\nDescuento:', datos_producto)
                    content = f"Pago semanal: {datos_producto}"
            
            # Restante de las asignaciones
            url = urls[i] if i < len(urls) else f"{page_num}_Dummy{i}"
            categoria = f"Categoria: {re.sub(r'[^A-Za-zÁÉÍÓÚáéíóúÑñ\s]', '', titulos[0]).strip()}" if titulos else ""
            cupon = cupones[i] if i < len(cupones) else "Sin Cupones"

            if sku:
                sku_num = "Sku: " + sku
                url_line = f"Url: {url}"
                data = [sku_num, categoria, subtitulo, cupon, content, vigencia, url_line]
                nueva_data_productos.append([sku_num, categoria, subtitulo, cupon, content, vigencia])
                #guardar_informacion_a_discovery(titulos[0], f"{sku} {url}", data)
                guardar_informacion_a_elasticsearch(f"{sku}", data)

        # Generar un reporte Excel para cada categoría
        if len(titulos) > 0:
            generar_reporte_excel(titulos[0])

def get_downloads_folder():
    # Obtener la carpeta de descargas según el sistema operativo
    if os.name == 'nt':  # Windows
        return str(Path.home() / 'Downloads')
    else:  # macOS y Linux
        return str(Path.home() / 'Downloads')

def generar_reporte_excel(titulo_categoria):
    # Ajustar las longitudes de las listas antes de generar el reporte
    ajustar_longitudes_listas()

    # Filtrar la nueva data solo para la categoría actual
    categoria_filtrada = [data for data in nueva_data_productos if data[1] == f"Categoria: {titulo_categoria.strip()}"]

    # Formatear la nueva data con saltos de línea en lugar de comas
    nueva_data_formateada = ['\n'.join(data) for data in categoria_filtrada]

    # Convertir los datos almacenados en un DataFrame de pandas
    df = pd.DataFrame({
        'SKU': skus[:len(categoria_filtrada)],  # Usa los SKUs correspondientes a la longitud de los datos filtrados
        'URL': urls[:len(categoria_filtrada)],  
        'Nueva Data Producto': nueva_data_formateada  # Nueva data con saltos de línea
    })

    # Asegurarse de que tanto la columna 'SKU' como 'URL' sean de tipo string
    df['SKU'] = df['SKU'].astype(str).str.strip()
    df['URL'] = df['URL'].astype(str).str.strip()

    # Crear una nueva columna para verificar si el SKU está contenido en la URL
    df['URL Coincide con SKU'] = [sku in url for sku, url in zip(df['SKU'], df['URL'])]





    # Añadir una columna que indique si el SKU tiene una imagen asociada
    def tiene_imagen(sku):
        extensiones_imagen = ['jpeg']

        # Obtener la carpeta de descargas del usuario
        downloads_folder = get_downloads_folder()

        # Ruta de la carpeta 'imagenes' en Descargas
        ruta_imagenes = os.path.join(downloads_folder, 'imagenes')

        for ext in extensiones_imagen:
            if os.path.exists(f"{ruta_imagenes}/{sku}.{ext}"):
                return True
        return False

    df['Tiene Imagen'] = df['SKU'].apply(tiene_imagen)

    # Obtener la carpeta de descargas del usuario
    downloads_folder = get_downloads_folder()

    # Crear un nombre de archivo único para cada categoría y guardarlo en la carpeta de descargas
    nombre_archivo_excel = os.path.join(downloads_folder, f"reporte_{titulo_categoria.replace(' ', '_')}.xlsx")

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

    # Ajustar el ancho de la columna de "Nueva Data Producto" para que sea 3 veces más ancho
    col_nueva_data = 3  # Asumiendo que la columna 3 es la que contiene "Nueva Data Producto"
    sheet.column_dimensions[sheet.cell(row=1, column=col_nueva_data).column_letter].width = 90  # Triplicar el ancho normal

    # Añadir imágenes a una nueva columna al final
    col_img = sheet.max_column + 1
    sheet.cell(row=1, column=col_img).value = "Imagen"

    # Añadir imágenes en la nueva columna para cada SKU
    for index, sku in enumerate(skus, start=2):
        imagen_path = None
        extensiones_imagen = ['jpeg']

        # Ruta de la carpeta 'imagenes' en Descargas
        ruta_imagenes = os.path.join(downloads_folder, 'imagenes')

        for ext in extensiones_imagen:
            imagen_path = f"{ruta_imagenes}/{sku}.{ext}"
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
    # Aplicar formato condicional para resaltar en rojo las celdas con "False" en las columnas "URL Coincide con SKU" y "Tiene Imagen"
    red_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")

    # Columna "URL Coincide con SKU"
    for row in sheet.iter_rows(min_row=2, min_col=col_url_coincide, max_col=col_url_coincide, max_row=sheet.max_row):
        for cell in row:
            if cell.value == False:
                cell.fill = red_fill


    # Columna "Tiene Imagen"
    for row in sheet.iter_rows(min_row=2, min_col=col_tiene_imagen, max_col=col_tiene_imagen, max_row=sheet.max_row):
        for cell in row:
            if cell.value == False:
                cell.fill = red_fill

    # Guardar el archivo Excel con las imágenes insertadas
    workbook.save(nombre_archivo_excel)