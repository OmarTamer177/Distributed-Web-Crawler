import boto3
import json
import logging
from botocore.config import Config
from botocore.exceptions import ClientError
from utils import AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - SQS - %(levelname)s - %(message)s')

# Queue Names
CRAWLER_QUEUE_NAME = 'crawler-queue'
INDEXER_QUEUE_NAME = 'indexer-queue'
RESULT_QUEUE_NAME = 'result-queue'

try:
    # Initialize SQS client
    sqs = boto3.client('sqs',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(
            retries=dict(
                max_attempts=3
            )
        )
    )
    # Test the connection
    sqs.list_queues()
    logging.info("Successfully connected to AWS SQS")
except Exception as e:
    logging.error(f"Failed to initialize AWS SQS client: {e}")
    raise

def get_queue_url(queue_name):
    """Get the URL for a queue, creating it if it doesn't exist"""
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        return response['QueueUrl']
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            logging.info(f"Queue {queue_name} does not exist, creating it...")
            response = sqs.create_queue(QueueName=queue_name)
            return response['QueueUrl']
        else:
            logging.error(f"Error getting queue URL for {queue_name}: {e}")
            raise

def send_message(queue_name, message_body):
    """Send a message to the specified queue"""
    try:
        queue_url = get_queue_url(queue_name)
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )
        logging.debug(f"Message sent to {queue_name}: {message_body}")
        return response['MessageId']
    except Exception as e:
        logging.error(f"Error sending message to {queue_name}: {e}")
        return None

def receive_messages(queue_name, max_messages=1, wait_time=20):
    """Receive messages from the specified queue"""
    try:
        queue_url = get_queue_url(queue_name)
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time
        )
        messages = response.get('Messages', [])
        if messages:
            logging.debug(f"Received {len(messages)} messages from {queue_name}")
        return messages
    except Exception as e:
        logging.error(f"Error receiving messages from {queue_name}: {e}")
        return []

def delete_message(queue_name, receipt_handle):
    """Delete a message from the queue"""
    try:
        queue_url = get_queue_url(queue_name)
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        logging.debug(f"Message deleted from {queue_name}")
        return True
    except Exception as e:
        logging.error(f"Error deleting message from {queue_name}: {e}")
        return False 