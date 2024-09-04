import boto3
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Initialize clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
table_name = os.environ['DYNAMODB_TABLE']
source_bucket = os.environ['SOURCE_BUCKET']
destination_bucket = os.environ['DESTINATION_BUCKET']
batch_size = 1000  # Adjust batch size based on your needs
max_threads = 10   # Number of threads to use

# Initialize a lock for thread-safe updates
lock = Lock()
last_key = None

def lambda_handler(event, context):
    global last_key
    # Read prefix from the event or environment
    prefix = event.get('prefix', None)
    if not prefix:
        return {'statusCode': 400, 'body': 'Prefix is required'}

    # Retrieve the next token from DynamoDB
    next_token = get_next_token(prefix)
    
    # Copy objects
    copied_count, last_key = copy_objects(prefix, next_token, context)
    
    # Save last_key to DynamoDB if necessary
    if copied_count == batch_size:
        # Ensure last_key is updated by all threads before saving
        if last_key:
            save_next_token(prefix, last_key)

    return {'statusCode': 200, 'body': f'Copied {copied_count} objects'}

def get_next_token(prefix):
    """Retrieve the next token from DynamoDB."""
    response = dynamodb.get_item(
        TableName=table_name,
        Key={'prefix': {'S': prefix}}
    )
    return response.get('Item', {}).get('next_token', {}).get('S', '')

def save_next_token(prefix, next_token):
    """Save the next token to DynamoDB."""
    dynamodb.put_item(
        TableName=table_name,
        Item={
            'prefix': {'S': prefix},
            'next_token': {'S': next_token}
        }
    )

def copy_objects(prefix, start_after, context):
    """Copy objects in batches using threads."""
    paginator = s3.get_paginator('list_objects_v2')
    copied_count = 0
    last_key = start_after

    while True:
        # Collect objects to copy
        page = paginator.paginate(Bucket=source_bucket, Prefix=prefix, StartAfter=start_after).build_full_result()
        objects_to_copy = [obj['Key'] for obj in page.get('Contents', [])]
        next_token = page.get('NextContinuationToken', None)

        if not objects_to_copy:
            break

        # Copy objects using threads
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {executor.submit(copy_object, obj_key): obj_key for obj_key in objects_to_copy}
            
            for future in as_completed(futures):
                try:
                    future.result()
                    copied_count += 1
                    # Update the last key processed in a thread-safe manner
                    with lock:
                        last_key = futures[future]
                    
                    # Check remaining Lambda execution time
                    if get_remaining_time(context) < 60:
                        # Save the last token and return before timeout
                        save_next_token(prefix, next_token or last_key)
                        return copied_count, last_key
                except Exception as e:
                    print(f"Error copying object: {e}")

        # Continue to the next page
        if not next_token:
            break
        start_after = last_key

    # If all objects are copied and Lambda has not timed out
    if objects_to_copy:
        with lock:
            last_key = objects_to_copy[-1]
        save_next_token(prefix, last_key)

    return copied_count, last_key

def copy_object(obj_key):
    """Copy a single object."""
    copy_source = {'Bucket': source_bucket, 'Key': obj_key}
    destination_key = obj_key.replace(prefix, prefix, 1)
    s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)

def get_remaining_time(context):
    """Get the remaining time before Lambda times out."""
    return context.get_remaining_time_in_millis() / 1000
