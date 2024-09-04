import boto3
import os
import time

# Initialize clients
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
table_name = os.environ['DYNAMODB_TABLE']
source_bucket = os.environ['SOURCE_BUCKET']
destination_bucket = os.environ['DESTINATION_BUCKET']
batch_size = 1000  # Adjust batch size based on your needs

def lambda_handler(event, context):
    # Read prefix from the event or environment
    prefix = event.get('prefix', None)
    if not prefix:
        return {'statusCode': 400, 'body': 'Prefix is required'}

    # Retrieve the next token from DynamoDB
    next_token = get_next_token(prefix)
    
    # Copy objects
    copied_count = copy_objects(prefix, next_token)
    
    # If there are more objects to copy, update DynamoDB and trigger the next Lambda
    if copied_count == batch_size:
        # Save next token to DynamoDB
        save_next_token(prefix, next_token)
        # You may need to invoke the next Lambda function here if needed
        
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

def copy_objects(prefix, start_after):
    """Copy objects in batches."""
    paginator = s3.get_paginator('list_objects_v2')
    copied_count = 0

    for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix, StartAfter=start_after):
        for obj in page.get('Contents', []):
            copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
            destination_key = obj['Key'].replace(prefix, prefix, 1)
            s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
            copied_count += 1
            
            # Check remaining Lambda execution time
            if get_remaining_time(context) < 60:
                # Save the next token and return before timeout
                return copied_count
                
        # If we've processed a batch, update the next token
        last_key = page.get('Contents', [-1])['Key'] if page.get('Contents') else ''
        if copied_count >= batch_size:
            save_next_token(prefix, last_key)
            break

    return copied_count

def get_remaining_time(context):
    """Get the remaining time before Lambda times out."""
    return context.get_remaining_time_in_millis() / 1000
