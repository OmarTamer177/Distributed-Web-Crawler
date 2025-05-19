import time
import logging
import json
from collections import deque
from flask import Flask, request, jsonify
import threading
from urllib.parse import urlparse
from sqs_utils import send_message, receive_messages, delete_message, CRAWLER_QUEUE_NAME, INDEXER_QUEUE_NAME, RESULT_QUEUE_NAME
from clear_queues import get_queue_url, purge_queue, get_sqs_client

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# Configuration
CRAWL_TIMEOUT = 10  # seconds before assuming crawler is unresponsive
NUM_CRAWLERS = 2  # number of crawler processes
MAX_CRAWL_DEPTH = 3  # maximum depth for crawling
MAX_RETRIES = 3  # maximum retries for a URL
RETRY_DELAY = 5  # seconds before retrying a failed URL

ALLOWED_DOMAINS = [
    # Python-related domains
    'python.org',
    'docs.python.org',
    'wiki.python.org',
    'pypi.org',
    
    # Documentation and learning
    'tutorial.python.org',
    'python-guide.org',
    'python-course.eu',
    
    # Popular programming websites
    'github.com',
    'stackoverflow.com',
    'stackexchange.com',
    
    # Tech documentation
    'developer.mozilla.org',
    'docs.microsoft.com',
    'cloud.google.com',
    
    # Tech learning platforms
    'coursera.org',
    'udemy.com',
    'edx.org',
    
    # Tech forums
    'forums.python.org',
    'forum.djangoproject.com',
    'forum.flask.pocoo.org'
]

# Initialize Flask app
app = Flask(__name__)

# Initialize stats dictionary
stats = {
    "active_crawlers": 0,
    "urls_in_queue": 0,
    "urls_crawled": 0,
    "urls_indexed": 0,
    "failed_urls": 0,
    "filtered_urls": 0,
    "total_links_found": 0,
    "average_links_per_page": 0,
    "domains_crawled": set(),
    "crawl_depth": {0: 0, 1: 0, 2: 0, 3: 0},
    "urls_in_progress": set()
}

# Initialize crawl queue
crawl_queue = deque()
tasks_in_progress = {}

def is_allowed_domain(url):
    """Check if the URL belongs to an allowed domain"""
    try:
        domain = urlparse(url).netloc
        return any(allowed in domain for allowed in ALLOWED_DOMAINS)
    except:
        return False

def is_html_url(url):
    """Check if the URL is likely to be an HTML page"""
    try:
        path = urlparse(url).path
        return not any(ext in path.lower() for ext in ['.pdf', '.jpg', '.png', '.gif', '.zip', '.tar', '.gz'])
    except:
        return False

def process_results():
    """Process results from crawlers"""
    while True:
        messages = receive_messages(RESULT_QUEUE_NAME)
        for message in messages:
            try:
                result = json.loads(message['Body'])
                
                if not isinstance(result, dict):
                    logging.error(f"Invalid result format: {result}")
                    delete_message(RESULT_QUEUE_NAME, message['ReceiptHandle'])
                    continue
                
                # Heartbeat processing
                if result.get("type") == "heartbeat":
                    crawler_id = result["crawler_id"]
                    if crawler_id in tasks_in_progress:
                        tasks_in_progress[crawler_id]["last_heartbeat"] = time.time()
                        logging.info(f"Heart beat from crawler {crawler_id} recieved!")
                    continue
                
                if "error" in result:
                    crawler_id = result.get("crawler_id")
                    if crawler_id:
                        logging.warning(f"Error from crawler {crawler_id}: {result['error']}")
                        tasks_in_progress.pop(crawler_id, None)
                        stats["failed_urls"] += 1
                    else:
                        logging.error("Error message without crawler_id")
                    continue

                if "status" in result:
                    crawler_id = result.get("crawler_id")
                    if crawler_id:
                        logging.info(f"Status from crawler {crawler_id}: {result['status']}")
                    else:
                        logging.error("Status message without crawler_id")
                    continue

                # Process crawl result
                url = result.get("url")
                if not url:
                    logging.error("Missing URL in result")
                    delete_message(RESULT_QUEUE_NAME, message['ReceiptHandle'])
                    continue

                extracted_urls = result.get("extracted_urls", [])
                if not isinstance(extracted_urls, list):
                    extracted_urls = []
                    
                content = result.get("content", "")
                crawler_id = result.get("crawler_id")
                depth = result.get("depth", 0)

                logging.info(f"Received result from crawler {crawler_id} for {url} - {len(extracted_urls)} new URLs")
                stats["urls_crawled"] += 1
                stats["total_links_found"] += len(extracted_urls)
                if stats["urls_crawled"] > 0:
                    stats["average_links_per_page"] = stats["total_links_found"] / stats["urls_crawled"]
                
                # Track domain statistics
                try:
                    domain = urlparse(url).netloc
                    stats["domains_crawled"].add(domain)
                except Exception as e:
                    logging.error(f"Error parsing domain from URL {url}: {str(e)}")
                
                # Track depth statistics
                if depth not in stats["crawl_depth"]:
                    stats["crawl_depth"][depth] = 0
                stats["crawl_depth"][depth] += 1

                # Remove URL from in-progress set
                stats["urls_in_progress"].discard(url)

                # Add new URLs to queue (with filtering)
                for new_url in extracted_urls:
                    if (is_allowed_domain(new_url) and is_html_url(new_url) and 
                        new_url not in crawl_queue and new_url not in stats["urls_in_progress"] and
                        depth < MAX_CRAWL_DEPTH):
                        crawl_queue.append(new_url)
                        stats["urls_in_queue"] += 1
                    else:
                        stats["filtered_urls"] += 1

                # Send content to indexer
                if content:
                    try:
                        send_message(INDEXER_QUEUE_NAME, {"url": url, "content": content})
                        stats["urls_indexed"] += 1
                    except Exception as e:
                        logging.error(f"Failed to send content to indexer: {str(e)}")

                # Mark task as done
                if crawler_id:
                    tasks_in_progress.pop(crawler_id, None)

                # Delete processed message
                delete_message(RESULT_QUEUE_NAME, message['ReceiptHandle'])

            except Exception as e:
                logging.error(f"Error processing result: {str(e)}")
                try:
                    delete_message(RESULT_QUEUE_NAME, message['ReceiptHandle'])
                except:
                    pass
                continue

        time.sleep(0.5)

def assign_tasks():
    """Assign URLs to crawlers"""
    while True:
        # Update active crawlers count
        stats["active_crawlers"] = len(tasks_in_progress)
        
        # Reassign unresponsive crawlers
        current_time = time.time()
        for crawler_id, task in list(tasks_in_progress.items()):
            # Unresponsive crawlers check
            last_active = task.get('last_heartbeat', task['start_time'])
            if current_time - last_active > CRAWL_TIMEOUT:
                logging.warning(f"Crawler {crawler_id} unresponsive. Last active: {last_active}. Reassigning URL: {task['url']}")
                stats["active_crawlers"] -= 1

            # Handle timeouts and failures
            if current_time - task['start_time'] > CRAWL_TIMEOUT:
                logging.warning(f"Crawler {crawler_id} timed out. Reassigning URL: {task['url']}")
                crawl_queue.appendleft(task['url'])
                tasks_in_progress.pop(crawler_id)
                stats["urls_in_progress"].discard(task['url'])
                stats["active_crawlers"] -= 1

        # Assign new URLs to idle crawlers
        for crawler_id in range(1, NUM_CRAWLERS + 1):
            if crawler_id not in tasks_in_progress and crawl_queue:
                url = crawl_queue.popleft()
                if url not in stats["urls_in_progress"]:
                    send_message(CRAWLER_QUEUE_NAME, {"url": url, "depth": 0, "crawler_id": crawler_id})
                    tasks_in_progress[crawler_id] = {
                        "url": url,
                        "start_time": time.time()
                    }
                    stats["urls_in_progress"].add(url)
                    logging.info(f"Assigned URL {url} to crawler {crawler_id}")
                    stats["urls_in_queue"] -= 1

        time.sleep(0.5)

@app.route('/add_urls', methods=['POST'])
def add_urls():
    data = request.get_json()
    if not data or 'urls' not in data:
        return jsonify({"error": "No URLs provided"}), 400
    
    urls = data['urls']
    added_count = 0
    for url in urls:
        if is_allowed_domain(url) and is_html_url(url) and url not in crawl_queue:
            crawl_queue.append(url)
            stats["urls_in_queue"] += 1
            added_count += 1
    
    return jsonify({"message": f"Added {added_count} URLs", "filtered": len(urls) - added_count}), 200

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q')
    logging.info(f"Received search request for query: {query}")
    
    if not query:
        logging.warning("Empty query received")
        return jsonify({"error": "No query provided"}), 400
    
    try:
        # Clear any old search results first
        messages = receive_messages(RESULT_QUEUE_NAME, max_messages=10)
        for msg in messages:
            try:
                result = json.loads(msg['Body'])
                if result.get("type") == "search_result":
                    delete_message(RESULT_QUEUE_NAME, msg['ReceiptHandle'])
            except:
                delete_message(RESULT_QUEUE_NAME, msg['ReceiptHandle'])
        
        # Forward search request to indexer
        logging.info(f"Forwarding search request to indexer for query: {query}")
        send_message(INDEXER_QUEUE_NAME, {
            "type": "search",
            "query": query,
            "timestamp": time.time()
        })
        
        # Wait for result with timeout
        start_time = time.time()
        timeout = 30
        attempts = 0
        max_attempts = 20
        
        while time.time() - start_time < timeout:
            attempts += 1
            logging.info(f"Checking for search results (attempt {attempts}/{max_attempts})")
            
            messages = receive_messages(RESULT_QUEUE_NAME, max_messages=10, wait_time=1)
            if messages:
                logging.info(f"Received {len(messages)} messages from result queue")
            
            for message in messages:
                try:
                    result = json.loads(message['Body'])
                    logging.info(f"Processing message: {json.dumps(result)}")
                    
                    if result.get("type") == "search_result":
                        results = result.get("results", [])
                        logging.info(f"Found search results: {len(results)} matches")
                        delete_message(RESULT_QUEUE_NAME, message['ReceiptHandle'])
                        return jsonify(results), 200
                except json.JSONDecodeError:
                    logging.error("Failed to decode search result message")
                    delete_message(RESULT_QUEUE_NAME, message['ReceiptHandle'])
                    continue
                except Exception as e:
                    logging.error(f"Error processing message: {e}", exc_info=True)
                    delete_message(RESULT_QUEUE_NAME, message['ReceiptHandle'])
                    continue
            
            if attempts >= max_attempts:
                logging.warning("Maximum attempts reached while waiting for search results")
                break
                
            time.sleep(0.5)  # Shorter sleep between checks
        
        logging.warning("Search request timed out")
        return jsonify({"error": "Search timed out"}), 504  # Gateway Timeout
        
    except Exception as e:
        logging.error(f"Search error: {e}", exc_info=True)
        return jsonify({"error": "Search failed"}), 500

@app.route('/status', methods=['GET'])
def get_status():
    logging.info("Received status request")
    status = dict(stats)
    # Convert all sets to lists for JSON serialization
    for k, v in status.items():
        if isinstance(v, set):
            status[k] = list(v)
    logging.info(f"Returning status: {json.dumps(status, default=str)}")
    return jsonify(status), 200


if __name__ == '__main__':
    # Add initial seed URLs
    seed_urls = [
        "https://www.python.org"
    ]
    
    # Add seed URLs to crawl queue
    for url in seed_urls:
        if is_allowed_domain(url) and is_html_url(url):
            crawl_queue.append(url)
            stats["urls_in_queue"] += 1
            logging.info(f"Added seed URL to queue: {url}")

    # Start result processing thread
    result_thread = threading.Thread(target=process_results)
    result_thread.daemon = True
    result_thread.start()

    # Start task assignment thread
    task_thread = threading.Thread(target=assign_tasks)
    task_thread.daemon = True
    task_thread.start()

    # Start Flask server
    app.run(host='0.0.0.0', port=5001, debug=False)
