import time
import logging
import os
import json
from whoosh.fields import Schema, TEXT, ID
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser
from sqs_utils import send_message, receive_messages, delete_message, INDEXER_QUEUE_NAME, RESULT_QUEUE_NAME

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

# Paths
INDEX_DIR = "index_dir"

# Create schema
schema = Schema(
    url=ID(stored=True, unique=True),
    content=TEXT
)

def init_index():
    if not os.path.exists(INDEX_DIR):
        os.mkdir(INDEX_DIR)
        ix = create_in(INDEX_DIR, schema)
        logging.info("Created new index.")
    else:
        ix = open_dir(INDEX_DIR)
        logging.info("Opened existing index.")
    return ix

def index_content(ix, url, content):
    try:
        writer = ix.writer()
        writer.update_document(url=url, content=content)
        writer.commit()
        logging.info(f"Indexed: {url}")
    except Exception as e:
        logging.error(f"Error indexing {url}: {e}")

def search_index(ix, query_str):
    try:
        with ix.searcher() as searcher:
            query = QueryParser("content", ix.schema).parse(query_str)
            results = searcher.search(query, limit=10)
            return [(r['url'], r.score) for r in results]
    except Exception as e:
        logging.error(f"Error searching: {e}")
        return []

def indexer_process():
    logging.info("Indexer started")
    ix = init_index()

    while True:
        try:
            messages = receive_messages(INDEXER_QUEUE_NAME)
            if not messages:
                time.sleep(1)
                continue
                
            message = messages[0]
            try:
                body = json.loads(message['Body'])
                logging.info(f"Received message: {json.dumps(body)}")
            except json.JSONDecodeError:
                logging.error("Failed to decode message body")
                delete_message(INDEXER_QUEUE_NAME, message['ReceiptHandle'])
                continue

            if body.get("type") == "search":
                query = body.get("query")
                if query:
                    results = search_index(ix, query)
                    send_message(RESULT_QUEUE_NAME, {
                        "type": "search_result",
                        "results": results
                    })
                delete_message(INDEXER_QUEUE_NAME, message['ReceiptHandle'])
                continue

            url = body.get("url")
            content = body.get("content")

            if url and content:
                index_content(ix, url, content)
            else:
                logging.warning(f"Missing data in message: {body}")

            # Delete processed message
            delete_message(INDEXER_QUEUE_NAME, message['ReceiptHandle'])

        except Exception as e:
            logging.error(f"Indexer encountered an error: {e}")
            time.sleep(1)  # Add delay before retrying

if __name__ == '__main__':
    indexer_process()
