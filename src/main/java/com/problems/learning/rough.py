import os
import random
import string
import boto3

def generate_random_string(size_in_bytes):
    """Generate a random string of a specific size."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size_in_bytes))

def upload_random_files_to_s3(prefix, min_size, max_size, total_size, bucket_name):
    s3 = boto3.client('s3')
    current_total_size = 0
    file_counter = 1

    while current_total_size < total_size:
        file_size = random.randint(min_size, max_size)
        if current_total_size + file_size > total_size:
            file_size = total_size - current_total_size

        file_content = generate_random_string(file_size)
        file_name = f"{prefix}/file_{file_counter}.txt"
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=file_content)

        current_total_size += file_size
        file_counter += 1
        print(f"Uploaded {file_name} with size {file_size} bytes. Total uploaded: {current_total_size} bytes.")

    print(f"Total size of all files uploaded: {current_total_size} bytes.")

# Example usage
prefixOfS3 = 'your/prefix'
minimumSizeOfFile = 150 * 1024 * 1024  # 150 MB
maximumSizeOfFile = 500 * 1024 * 1024  # 500 MB
totalSizeOfAllFilesCombined = 100 * 1024 * 1024 * 1024  # 100 GB
bucket_name = 'your-bucket-name'

upload_random_files_to_s3(prefixOfS3, minimumSizeOfFile, maximumSizeOfFile, totalSizeOfAllFilesCombined, bucket_name)
