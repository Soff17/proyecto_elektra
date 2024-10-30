from elasticsearch import Elasticsearch, exceptions
from langchain_elasticsearch import ElasticsearchEmbeddings
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv
import concurrent.futures
import datetime

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

URL_PROJECT = os.getenv('URL_PROJECT')
API_KEY_PROJECT = os.getenv('API_KEY_PROJECT')
INDICE = os.getenv('INDICE')
model_id = ".multilingual-e5-small_linux-x86_64"

# Conectar a Elasticsearch con API Key
es = Elasticsearch(
    URL_PROJECT,
    api_key=API_KEY_PROJECT
)

def split_text(text, max_length=512):
    """Dividir el texto en partes de longitud máxima especificada."""
    return [text[i:i+max_length] for i in range(0, len(text), max_length)]

def get_documents(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    title = os.path.splitext(os.path.basename(file_path))[0]
    upload_date = datetime.datetime.now().isoformat()
    metadata = {'title': title, 'upload-date': upload_date}

    text_parts = split_text(content)
    return [{'text': part, 'metadata': metadata} for part in text_parts]

def embed_and_index_documents(es, model_id, index_name, documents, batch_size=10):
    embeddings = ElasticsearchEmbeddings.from_es_connection(model_id, es)

    existing_docs = set()  # Conjunto para almacenar IDs de documentos existentes
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        try:
            document_embeddings = embeddings.embed_documents([doc['text'] for doc in batch])
            for doc, emb in zip(batch, document_embeddings):
                doc['vector'] = emb
                doc.update(doc['metadata'])
                
                # Verificar duplicados
                doc_id = doc.get('metadata', {}).get('title')  # O usar otro campo que identifique el documento
                if doc_id in existing_docs:
                    print(f"Duplicado encontrado: {doc_id}")
                    continue  # Saltar la indexación del documento duplicado
                existing_docs.add(doc_id)

                res = es.index(index=index_name, body=doc, request_timeout=120)
                print(f"Document indexed: {res}")
        except Exception as e:
            print(f"Error in indexing batch: {e}")

def subir_archivos_de_carpeta(indice, carpeta):
    processed_files = set()
    with ThreadPoolExecutor() as executor:
        futures = []
        for ruta_carpeta, _, archivos in os.walk(carpeta):
            for archivo in archivos:
                if archivo.endswith('.txt') and archivo not in processed_files:
                    ruta_archivo = os.path.join(ruta_carpeta, archivo)
                    documents = get_documents(ruta_archivo)
                    future = executor.submit(embed_and_index_documents, es, model_id, indice, documents)
                    futures.append(future)
                    processed_files.add(archivo)

        # Monitorea si hay duplicación en resultados de futures
        results_seen = set()
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    doc_id = result.get('_id', 'No ID')
                    if doc_id not in results_seen:
                        results_seen.add(doc_id)
                        print(f"Indexing complete for document: {doc_id}")
                    else:
                        print(f"Duplicate document ID detected: {doc_id}")
            except Exception as e:
                print(f"Error in indexing: {e}")

    print(f"Total documentos únicos procesados: {len(processed_files)}")
    print(f"Total documentos únicos indexados: {len(results_seen)}")

def verificar_y_limpiar_indice(es, index_name):
    """Verifica si el índice existe y contiene documentos, y los borra si existen."""
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)['count']
        if count > 0:
            print(f"El índice {index_name} tiene {count} documentos, procediendo a limpiar.")
            es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})
            print(f"Documentos borrados del índice {index_name}.")
        else:
            print(f"El índice {index_name} está vacío.")
    else:
        print(f"El índice {index_name} no existe, procediendo a la carga de documentos.")

def indexar_documento(indice, id_documento, documento):
    try:
        # Crear el objeto de embeddings
        embeddings = ElasticsearchEmbeddings.from_es_connection(model_id, es)
        
        # Embeder el contenido del documento
        document_embedding = embeddings.embed_documents([documento["contenido"]])
        
        # Preparar el documento con la estructura adecuada
        doc_to_index = {
            "text": documento["contenido"],
            "vector": document_embedding[0],  # Suponiendo que se embebe un solo documento
            "metadata": {
                "title": documento.get("titulo", id_documento),  # Usa el título proporcionado o el ID como título
                "upload-date": datetime.datetime.now().isoformat()  # O agrega la fecha de carga si es necesario
            }
        }

        # Indexar el documento
        res = es.index(index=indice, id=id_documento, body=doc_to_index)
        print(f"Documento {id_documento} indexado correctamente: {res['result']}")
        return res
    except Exception as e:
        print(f"Error al indexar el documento {id_documento}: {str(e)}")
        return None

# Función para eliminar un documento de Elasticsearch por ID
def eliminar_documento(indice, id_documento):
    try:
        res = es.delete(index=indice, id=id_documento)
        print(f"Documento {id_documento} eliminado: {res['result']}")
    except Exception as e:
        print(f"Error al eliminar el documento {id_documento}: {str(e)}")

# Función para eliminar todos los documentos de un índice
def eliminar_documentos(indice):
    try:
        # Obtener todos los documentos en el índice
        res = es.search(index=indice, body={"query": {"match_all": {}}}, size=1000)
        document_ids = [doc['_id'] for doc in res['hits']['hits']]

        # Usar ThreadPoolExecutor para eliminar documentos en paralelo
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(eliminar_documento, indice, doc_id): doc_id for doc_id in document_ids}
            for future in as_completed(futures):
                doc_id = futures[future]
                try:
                    future.result()  # Para manejar excepciones si ocurren en el hilo
                except Exception as e:
                    print(f"Error al eliminar el documento {doc_id}: {str(e)}")
    except Exception as e:
        print(f"Error al eliminar los documentos del índice {indice}: {str(e)}")

# Función para contar documentos en Elasticsearch
def contar_documentos(indice):
    try:
        res = es.count(index=indice)
        total_documentos = res['count']
        print(f"Total de documentos en el índice {indice}: {total_documentos}")
        return total_documentos
    except Exception as e:
        print(f"Error al contar los documentos en el índice {indice}: {str(e)}")
        return None

# Función para eliminar un documento de Elasticsearch por ID
def eliminar_documento(indice, id_documento):
    try:
        res = es.delete(index=indice, id=id_documento)
        print(f"Documento {id_documento} eliminado: {res['result']}")
        return True
    except Exception as e:
        print(f"Error al eliminar el documento {id_documento}: {str(e)}")
        return False

# Función para eliminar documentos por categoria
DEFAULT_INDICE = INDICE

def eliminar_documentos_por_categoria(categoria):
    try:
        query = {
            "query": {
                "match": {
                    "contenido": categoria
                }
            }
        }
        res = es.search(index=DEFAULT_INDICE, body=query, size=1000)
        document_ids = [doc['_id'] for doc in res['hits']['hits']]

        if not document_ids:
            return f"No se encontraron documentos con la categoría '{categoria}'"

        # Usar ThreadPoolExecutor para eliminar documentos en paralelo
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(eliminar_documento, DEFAULT_INDICE, doc_id): doc_id for doc_id in document_ids}
            for future in as_completed(futures):
                doc_id = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error al eliminar el documento {doc_id}: {str(e)}")

        return f"Eliminados {len(document_ids)} documentos con la categoría '{categoria}'"
    
    except exceptions.NotFoundError:
        return f"El índice '{DEFAULT_INDICE}' no existe."
    except Exception as e:
        return f"Error al eliminar documentos: {str(e)}"

