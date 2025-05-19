import boto3
import json
from sqs_utils import CRAWLER_QUEUE_NAME, INDEXER_QUEUE_NAME, RESULT_QUEUE_NAME
from utils import AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY

def get_sqs_client():
    return boto3.client('sqs',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

def get_queue_url(sqs, queue_name):
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        return response['QueueUrl']
    except Exception as e:
        print(f"Error getting queue URL for {queue_name}: {e}")
        return None

def list_messages(queue_url, sqs):
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=10,
            WaitTimeSeconds=1
        )
        messages = response.get('Messages', [])
        print(f"Found {len(messages)} messages")
        for msg in messages:
            print(f"Message ID: {msg['MessageId']}")
            print(f"Content: {msg['Body']}")
            print("-" * 80)
        return messages
    except Exception as e:
        print(f"Error listing messages: {e}")
        return []

def purge_queue(queue_url, sqs):
    try:
        sqs.purge_queue(QueueUrl=queue_url)
        print(f"Successfully purged queue: {queue_url}")
    except Exception as e:
        print(f"Error purging queue: {e}")

def main():
    sqs = get_sqs_client()
    queues = [CRAWLER_QUEUE_NAME, INDEXER_QUEUE_NAME, RESULT_QUEUE_NAME]
    
    print("Available queues:")
    for queue_name in queues:
        print(f"- {queue_name}")
    
    while True:
        print("\nOptions:")
        print("1. List messages in a queue")
        print("2. Purge a queue")
        print("3. Purge all queues")
        print("4. Exit")
        
        choice = input("Enter your choice (1-4): ")
        
        if choice == "1":
            queue_name = input("Enter queue name: ")
            queue_url = get_queue_url(sqs, queue_name)
            if queue_url:
                list_messages(queue_url, sqs)
        
        elif choice == "2":
            queue_name = input("Enter queue name: ")
            queue_url = get_queue_url(sqs, queue_name)
            if queue_url:
                confirm = input(f"Are you sure you want to purge {queue_name}? (yes/no): ")
                if confirm.lower() == "yes":
                    purge_queue(queue_url, sqs)
        
        elif choice == "3":
            confirm = input("Are you sure you want to purge ALL queues? (yes/no): ")
            if confirm.lower() == "yes":
                for queue_name in queues:
                    queue_url = get_queue_url(sqs, queue_name)
                    if queue_url:
                        purge_queue(queue_url, sqs)
        
        elif choice == "4":
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main() 