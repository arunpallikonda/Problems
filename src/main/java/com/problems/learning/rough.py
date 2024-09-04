import boto3
from concurrent.futures import ThreadPoolExecutor

# Initialize the S3 client
s3 = boto3.client('s3')

def delete_objects(bucket_name, objects):
    """Delete a batch of objects."""
    if objects:
        response = s3.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [{'Key': obj} for obj in objects]
            }
        )
        print(f"Deleted {len(response.get('Deleted', []))} objects.")
    else:
        print("No objects to delete.")

def list_objects(bucket_name, prefix, max_keys=1000):
    """List objects in a bucket with a specific prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, PaginationConfig={'MaxItems': max_keys}):
        yield from (obj['Key'] for obj in page.get('Contents', []))

def main(bucket_name, prefix):
    """Main function to delete objects from the S3 bucket."""
    batch_size = 1000
    objects_to_delete = []
    
    # Process objects in batches
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for obj_key in list_objects(bucket_name, prefix):
            objects_to_delete.append(obj_key)
            if len(objects_to_delete) >= batch_size:
                # Submit batch delete task
                futures.append(executor.submit(delete_objects, bucket_name, objects_to_delete))
                objects_to_delete = []
        
        # Submit the last batch if any
        if objects_to_delete:
            futures.append(executor.submit(delete_objects, bucket_name, objects_to_delete))
        
        # Wait for all tasks to complete
        for future in futures:
            future.result()

if __name__ == "__main__":
    bucket_name = 'your-bucket-name'
    prefix = 'your/folder/prefix/'  # Make sure to include the trailing slash
    main(bucket_name, prefix)
