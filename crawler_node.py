import time
import logging
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag
import urllib.robotparser
from sqs_utils import send_message, receive_messages, delete_message, CRAWLER_QUEUE_NAME, RESULT_QUEUE_NAME
import threading

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

# Global Config
CRAWL_DELAY = 1  # polite delay between requests (seconds)
USER_AGENT = "DistributedCrawlerBot/1.0"

# Robots.txt Cache
robots_cache = {}

def is_allowed_by_robots(url):
    parsed = urlparse(url)
    domain = parsed.scheme + "://" + parsed.netloc

    if domain not in robots_cache:
        robots_url = urljoin(domain, "/robots.txt")
        rp = urllib.robotparser.RobotFileParser()
        try:
            rp.set_url(robots_url)
            rp.read()
            robots_cache[domain] = rp
            logging.info(f"Fetched robots.txt for domain: {domain}")
        except Exception as e:
            logging.warning(f"Failed to fetch robots.txt for {domain}, assuming allowed. Error: {e}")
            return True  # Assume allowed if robots.txt fails

    return robots_cache[domain].can_fetch(USER_AGENT, url)

def normalize_url(url, base_url):
    try:
        url = urljoin(base_url, url)         # Convert relative URLs to absolute
        url, _ = urldefrag(url)              # Remove fragments like #section
        return url
    except:
        return None

def extract_links_and_text(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    links = set()

    for a in soup.find_all('a', href=True):
        href = normalize_url(a['href'], base_url)
        if href and urlparse(href).scheme in ['http', 'https']:
            links.add(href)

    return list(links), text

def crawl_url(url, crawler_id):
    headers = {"User-Agent": USER_AGENT}

    try:
        logging.info(f"Crawler {crawler_id} starting to fetch URL: {url}")
        time.sleep(CRAWL_DELAY)  # politeness
        
        response = requests.get(url, headers=headers, timeout=10)
        logging.info(f"Crawler {crawler_id} got response: {response.status_code}")

        if response.status_code != 200:
            logging.warning(f"Invalid response for {url}: {response.status_code}")
            return [], None

        content_type = response.headers.get('Content-Type', '')
        if 'text/html' not in content_type:
            logging.warning(f"Non-HTML content for {url}: {content_type}")
            return [], None

        links, text = extract_links_and_text(response.text, url)
        logging.info(f"Crawler {crawler_id} crawled {url}: Found {len(links)} links")
        return links, text

    except requests.RequestException as e:
        logging.error(f"Crawler {crawler_id} failed to fetch {url}: {str(e)}")
        return [], None
    except Exception as e:
        logging.error(f"Crawler {crawler_id} unexpected error while crawling {url}: {str(e)}")
        return [], None

def crawler_process(crawler_id):
    logging.info(f"Crawler {crawler_id} started")
    
    while True:
        try:
            messages = receive_messages(CRAWLER_QUEUE_NAME)
            if not messages:
                time.sleep(1)
                continue
                
            message = messages[0]
            try:
                task = json.loads(message['Body'])
            except json.JSONDecodeError as e:
                logging.error(f"Crawler {crawler_id} received invalid JSON: {str(e)}")
                delete_message(CRAWLER_QUEUE_NAME, message['ReceiptHandle'])
                continue
            
            if not isinstance(task, dict):
                logging.error(f"Crawler {crawler_id} received invalid task format")
                delete_message(CRAWLER_QUEUE_NAME, message['ReceiptHandle'])
                continue
            
            if task.get('crawler_id') != crawler_id:
                # This message is for another crawler
                time.sleep(0.1)
                continue
                
            url = task.get("url")
            if not url:
                logging.error(f"Crawler {crawler_id} received task without URL")
                delete_message(CRAWLER_QUEUE_NAME, message['ReceiptHandle'])
                continue
                
            depth = task.get("depth", 0)
            
            logging.info(f"Crawler {crawler_id} received URL: {url}")
            
            # Send status update
            send_message(RESULT_QUEUE_NAME, {
                "status": f"Starting to crawl {url}",
                "crawler_id": crawler_id
            })
            
            links, content = crawl_url(url, crawler_id)
            
            # Ensure links is a list
            if not isinstance(links, list):
                links = []
            
            # Send result
            result = {
                "url": url,
                "extracted_urls": links,
                "content": content,
                "crawler_id": crawler_id,
                "depth": depth
            }
            
            try:
                send_message(RESULT_QUEUE_NAME, result)
            except Exception as e:
                logging.error(f"Crawler {crawler_id} failed to send result: {str(e)}")
                continue
            
            # Send completion status
            send_message(RESULT_QUEUE_NAME, {
                "status": f"Completed {url}",
                "crawler_id": crawler_id
            })
            
            # Delete processed message
            delete_message(CRAWLER_QUEUE_NAME, message['ReceiptHandle'])
            
        except Exception as e:
            logging.error(f"Error in crawler {crawler_id}: {str(e)}")
            send_message(RESULT_QUEUE_NAME, {
                "error": str(e),
                "crawler_id": crawler_id
            })
            time.sleep(1)  # Prevent tight error loop

def send_heartbeat(crawler_id):
    while True:
        send_message(RESULT_QUEUE_NAME, {
            "type": "heartbeat",
            "crawler_id": crawler_id,
            "timestamp": time.time()
        })
        time.sleep(3)  # Send every 3 seconds


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python crawler_node.py <crawler_id>")
        sys.exit(1)
    
    crawler_id = int(sys.argv[1])

    # Heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, args=(crawler_id,))
    heartbeat_thread.daemon = True
    heartbeat_thread.start()

    crawler_process(crawler_id)
