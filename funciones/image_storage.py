import os
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor

def initialize_storage_client():
    return storage.Client.from_service_account_json('')

# Función para vaciar la carpeta de imagenes_subidas en lugar de todo el bucket
def empty_bucket_folder(bucket_name, folder_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    
    # Listar solo los blobs que están dentro de la carpeta especificada
    blobs = list(bucket.list_blobs(prefix=folder_name))
    
    if blobs:
        bucket.delete_blobs(blobs)
        print(f"Todas las imágenes en la carpeta '{folder_name}' han sido eliminadas del bucket '{bucket_name}'.")
    else:
        print(f"No hay imágenes en la carpeta '{folder_name}' dentro del bucket '{bucket_name}'.")

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

# Función para contar las imágenes en una carpeta específica del bucket
def count_images_in_bucket(bucket_name, folder_name):
    client = initialize_storage_client()
    bucket = client.bucket(bucket_name)
    
    # Listar los blobs que están dentro de la carpeta especificada
    blobs = list(bucket.list_blobs(prefix=folder_name))
    
    # Filtrar solo imágenes por su tipo MIME (esto depende del formato de imágenes que estés utilizando, por ejemplo, PNG o JPG)
    image_count = sum(1 for blob in blobs if blob.name.endswith(('.png', '.jpg', '.jpeg', '.gif')))
    
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
    
    # Create a list of files in the local folder (only PDFs)
    file_paths = [
        os.path.join(folder_path, filename)
        for filename in os.listdir(folder_path)
        if filename.endswith('.pdf') and os.path.isfile(os.path.join(folder_path, filename))
    ]

    # Use ThreadPoolExecutor to upload the PDFs in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                upload_pdf, client, bucket_name, file_path, f'{bucket_folder_name}/{os.path.basename(file_path)}'
            )
            for file_path in file_paths
        ]
        
        # Ensure all uploads are complete
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
    blob.upload_from_file(image_buffer, content_type='image/jpeg')  # Ajustar el tipo MIME si es PNG o GIF
    print(f"La imagen {image_name} fue subida exitosamente al bucket {bucket_name}/{folder_name}.")
