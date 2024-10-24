from elasticsearch import Elasticsearch
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv
from langchain_elasticsearch import ElasticsearchEmbeddings

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

URL_PROJECT = os.getenv('https://ee94da5de3504cef84748a8e38dc54dc.us-central1.gcp.cloud.es.io:443')
API_KEY_PROJECT = os.getenv('VVpDWHVwSUJSQ3ZmQVYyWUd2bFY6SXNiMWRnTXNUcEdycVJQZmNvWlctQQ==')

# Conectar a Elasticsearch con API Key
es = Elasticsearch(
    URL_PROJECT,
    api_key=API_KEY_PROJECT
)

def embed_and_index_documents(documents):
    '''
    # Crear los metadatos del documento
    metadata = {
        'title': title,
    }
    
    return {
        'text': content,
        'metadata': metadata
    }

    Esto es lo que necesita tener el documents para poder ser leido por el embed y ser subido a elasitc
    '''

    model_id = ".multilingual-e5-small_linux-x86_64"
    index_name = "langchaintest"

    embeddings = ElasticsearchEmbeddings.from_es_connection(model_id, es)
    document_embeddings = embeddings.embed_documents(documents['text'])

    for doc, emb in zip([documents], document_embeddings):
        doc['embedding'] = emb
        res = es.index(index=index_name, body=doc)
        print(f"Document indexed: {res}")

# Función para subir un documento a Elasticsearch
def indexar_documento(indice, id_documento, documento):
    try:
        res = es.index(index=indice, id=id_documento, document=documento)
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
