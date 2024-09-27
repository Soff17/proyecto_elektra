import os
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor

def initialize_storage_client():
    return storage.Client.from_service_account_json('')

# Función para vaciar el bucket eliminando todos los blobs a la vez
def empty_bucket(bucket_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())

    if blobs:
        bucket.delete_blobs(blobs)
        print(f"Todas las imágenes han sido eliminadas del bucket '{bucket_name}'.")
    else:
        print(f"No hay imágenes en el bucket '{bucket_name}'.")

# Función para subir una imagen
def upload_image(client, bucket_name, file_path, blob_name):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

# Función para subir múltiples imágenes a una carpeta en el bucket de forma concurrente
def upload_images_in_folder(bucket_name, folder_path, bucket_folder_name):
    client = initialize_storage_client()
    
    # Crear una lista de archivos en la carpeta local
    file_paths = [
        os.path.join(folder_path, filename)
        for filename in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, filename))
    ]

    # Usar ThreadPoolExecutor para subir las imágenes en paralelo
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                upload_image, client, bucket_name, file_path, f'{bucket_folder_name}/{os.path.basename(file_path)}'
            )
            for file_path in file_paths
        ]
        
        # Asegurarse de que todas las subidas hayan terminado
        for future in futures:
            future.result()

# Función para contar las imagenes en el storage. 
def count_images_in_bucket(bucket_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    
    # Obtener todos los blobs (objetos) en el bucket
    blobs = list(bucket.list_blobs())
    
    # Contar los blobs
    image_count = len(blobs)
    
    print(f"El bucket '{bucket_name}' contiene {image_count} imágenes.")
    return image_count
