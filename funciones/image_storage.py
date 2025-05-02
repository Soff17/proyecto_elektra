import io
import os
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor

def initialize_storage_client():
    # Obtener la ruta del JSON desde el .env
    json_path = os.getenv('STORAGE_SERVICE_ACCOUNT_JSON_PATH')
    if not json_path:
        raise ValueError("La ruta del archivo JSON no está definida en el archivo .env")
    
    client = storage.Client.from_service_account_json(json_path)
    return client

# Función para vaciar la carpeta de imagenes_subidas en lugar de todo el bucket
def empty_bucket_folder(bucket_name, folder_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=folder_name))
    
    if blobs:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(bucket.delete_blob, blob.name) for blob in blobs]
            for future in futures:
                future.result()
        print(f"Todas las imágenes en la carpeta '{folder_name}' han sido eliminadas del bucket '{bucket_name}'.")
    else:
        print(f"No hay imágenes en la carpeta '{folder_name}' dentro del bucket '{bucket_name}'.")

# Función para subir una imagen
def upload_image(client, bucket_name, file_path, blob_name):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

def upload_images_in_folder(bucket_name, folder_path, bucket_folder_name):
    client = initialize_storage_client()
    file_paths = [
        os.path.join(folder_path, filename)
        for filename in os.listdir(folder_path)
        if filename.lower().endswith('.jpeg') and os.path.isfile(os.path.join(folder_path, filename))
    ]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(upload_image, client, bucket_name, file_path, f'{bucket_folder_name}/{os.path.basename(file_path)}')
            for file_path in file_paths
        ]
        for future in futures:
            future.result()

# Función para subir múltiples imágenes concurrentemente
def upload_images_in_folder(bucket_name, folder_path, bucket_folder_name):
    client = initialize_storage_client()
    file_paths = [
        os.path.join(folder_path, filename)
        for filename in os.listdir(folder_path)
        if filename.lower().endswith('.jpeg') and os.path.isfile(os.path.join(folder_path, filename))
    ]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(upload_image, client, bucket_name, file_path, f'{bucket_folder_name}/{os.path.basename(file_path)}')
            for file_path in file_paths
        ]
        for future in futures:
            future.result()


# Función para contar las imágenes en una carpeta específica del bucket
def count_images_in_bucket(bucket_name, folder_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    
    # Listar los blobs que están dentro de la carpeta especificada
    blobs = list(bucket.list_blobs(prefix=folder_name))
    
    # Filtrar solo imágenes por su tipo MIME
    image_count = sum(1 for blob in blobs if blob.name.endswith(('.jpeg')))
    
    print(f"La carpeta '{folder_name}' en el bucket '{bucket_name}' contiene {image_count} imágenes.")
    return image_count

# Function to upload a PDF
def upload_pdf(client, bucket_name, file_path, blob_name):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

# Function to upload multiple PDFs concurrently
def upload_pdfs_in_folder(bucket_name, folder_path, bucket_folder_name):
    client = initialize_storage_client()
    file_paths = [
        os.path.join(folder_path, filename)
        for filename in os.listdir(folder_path)
        if filename.endswith('.pdf') and os.path.isfile(os.path.join(folder_path, filename))
    ]

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(upload_pdf, client, bucket_name, file_path, f'{bucket_folder_name}/{os.path.basename(file_path)}')
            for file_path in file_paths
        ]
        for future in futures:
            future.result()

# Function to count the number of PDFs in the storage bucket
def count_pdfs_in_bucket(bucket_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    
    # Get all the blobs (objects) in the bucket
    blobs = list(bucket.list_blobs())
    
    # Filter out only PDFs
    pdf_count = sum(1 for blob in blobs if blob.name.endswith('.pdf'))
    
    print(f"The bucket '{bucket_name}' contains {pdf_count} PDFs.")
    return pdf_count

def upload_pdf_buffer(bucket_name, folder_name, blob_name, pdf_buffer):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{blob_name}')
    blob.upload_from_file(pdf_buffer, content_type='application/pdf')
    print(f"El PDF {blob_name} fue subido exitosamente al bucket {bucket_name}/{folder_name}.")

def upload_image_buffer(bucket_name, folder_name, image_name, image_buffer):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{image_name}')
    blob.upload_from_file(image_buffer, content_type='image/jpeg')
    print(f"La imagen {image_name} fue subida exitosamente al bucket {bucket_name}/{folder_name}.")

def check_image_in_bucket(bucket_name, folder_name, image_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    
    # Crear el nombre completo del blob (ruta en el bucket)
    blob_name = f"{folder_name}/{image_name}"
    
    # Verificar si el blob existe en el bucket
    blob = bucket.blob(blob_name)
    return blob.exists()

# Esta función optimizada reemplaza la llamada a st.check_image_in_bucket para múltiples imágenes
def check_images_in_bucket(bucket_name, folder_name, image_names):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    all_blobs = list(bucket.list_blobs(prefix=folder_name))  # Obtener todos los blobs una vez

    # Crear un set con los nombres de los blobs para facilitar la búsqueda
    existing_images = {blob.name.split('/')[-1] for blob in all_blobs if blob.name.endswith('.jpeg')}

    # Crear un diccionario de resultados basado en la existencia en el set de imágenes
    results = {image_name: image_name in existing_images for image_name in image_names}

    # Mostrar el estado de cada imagen
    for image_name, exists in results.items():
        status = "existe" if exists else "no se encontró"
        print(f"La imagen {image_name} {status} en el bucket {bucket_name}/{folder_name}.")

    return results


def get_default_image_buffer(bucket_name, default_image_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    
    # Crear el nombre completo del blob (ruta en el bucket)
    blob = bucket.blob(default_image_name)
    
    # Verificar si el blob existe antes de intentar descargarlo
    if blob.exists():
        # Descargar el contenido del blob en un buffer
        image_buffer = io.BytesIO()
        blob.download_to_file(image_buffer)
        image_buffer.seek(0)  # Reiniciar el puntero del buffer al inicio
        print(f"Imagen predeterminada '{default_image_name}' descargada exitosamente.")
        return image_buffer
    else:
        print(f"Imagen predeterminada '{default_image_name}' no se encontró en el bucket '{bucket_name}'.")
        return None

def upload_text_buffer(bucket_name, folder_name, file_name, text_buffer):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    blob.upload_from_file(text_buffer, content_type='text/plain')
    print(f"El archivo de texto '{file_name}' fue subido exitosamente al bucket '{bucket_name}/{folder_name}'.")

def upload_file(bucket_name, folder_name, file_path):
    """
    Subir un archivo genérico al bucket de Google Cloud Storage.
    
    :param bucket_name: Nombre del bucket en GCP.
    :param folder_name: Carpeta dentro del bucket donde se subirá el archivo.
    :param file_path: Ruta del archivo local a subir.
    """
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    blob_name = f"{folder_name}/{os.path.basename(file_path)}"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    print(f"Archivo '{os.path.basename(file_path)}' subido exitosamente a '{bucket_name}/{folder_name}'.")

def download_image_from_bucket(bucket_name, folder_name, image_name):
    """
    Descarga una imagen desde el bucket de Google Cloud Storage como un buffer.
    """
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/{image_name}")

    if blob.exists():
        image_buffer = io.BytesIO()
        blob.download_to_file(image_buffer)
        image_buffer.seek(0)
        print(f"Imagen '{image_name}' descargada exitosamente del bucket '{bucket_name}/{folder_name}'.")
        return image_buffer
    else:
        print(f"Imagen '{image_name}' no encontrada en el bucket '{bucket_name}/{folder_name}'.")
        return None

def image_exists_in_bucket(bucket_name, folder_name, image_name):
    """
    Verifica si una imagen existe en el bucket de Google Cloud Storage.
    """
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/{image_name}")
    return blob.exists()

def download_pdf_buffer(bucket_name, folder, file_name):
    """
    Descarga un archivo PDF desde el bucket y lo devuelve como un buffer en memoria.
    """
    try:
        client = initialize_storage_client()
        bucket = client.bucket(bucket_name)
        blob_path = f"{folder}/{file_name}"
        blob = bucket.blob(blob_path)

        if not blob.exists():
            raise FileNotFoundError(f"El archivo {blob_path} no existe en el bucket {bucket_name}.")

        # Descargar el contenido del archivo al buffer
        pdf_buffer = io.BytesIO()
        blob.download_to_file(pdf_buffer)
        pdf_buffer.seek(0)  # Asegurarse de que el buffer esté al inicio

        print(f"Archivo {blob_path} descargado exitosamente desde el bucket {bucket_name}.")
        return pdf_buffer

    except Exception as e:
        print(f"Error al descargar el archivo {file_name}: {e}")
        raise

def delete_file(bucket_name, file_path):
    """
    Elimina un archivo del bucket especificado.
    
    Args:
        bucket_name (str): Nombre del bucket.
        file_path (str): Ruta completa del archivo dentro del bucket.
    """
    try:
        client = initialize_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)

        if blob.exists():
            blob.delete()
            print(f"Archivo '{file_path}' eliminado del bucket '{bucket_name}'.")
        else:
            print(f"Archivo '{file_path}' no encontrado en el bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error al eliminar el archivo '{file_path}' del bucket: {e}")
        raise
