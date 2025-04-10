import boto3
import gzip
import io
import json
from boto3.dynamodb.types import TypeDeserializer

s3 = boto3.client('s3')
bucket = 'your-bucket'
gz_key = 'input-dynamo-lines.json.gz'
output_key = 'output-normal-json.json'

deserializer = TypeDeserializer()

mpu = s3.create_multipart_upload(Bucket=bucket, Key=output_key)
upload_id = mpu['UploadId']
parts = []
part_number = 1
buffer = io.StringIO()
part_size = 5 * 1024 * 1024  # 5MB
current_size = 0

s3_object = s3.get_object(Bucket=bucket, Key=gz_key)
gz_stream = gzip.GzipFile(fileobj=s3_object['Body'])

def dynamo_to_regular_json(dynamo_json):
    return {k: deserializer.deserialize(v) for k, v in dynamo_json.items()}

def upload_part(force=False):
    global part_number, current_size, buffer
    if current_size >= part_size or force:
        data = buffer.getvalue()
        response = s3.upload_part(
            Body=data.encode('utf-8'),
            Bucket=bucket,
            Key=output_key,
            UploadId=upload_id,
            PartNumber=part_number
        )
        parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
        part_number += 1
        buffer = io.StringIO()
        current_size = 0

# Begin JSON array
buffer.write("[\n")
current_size += 2
first_line = True

try:
    for raw_line in gz_stream:
        line = raw_line.decode('utf-8').strip()
        if not line:
            continue
        dynamo_item = json.loads(line)
        normal_item = dynamo_to_regular_json(dynamo_item)
        if not first_line:
            buffer.write(",\n")
            current_size += 2
        else:
            first_line = False

        json_str = json.dumps(normal_item)
        buffer.write(json_str)
        current_size += len(json_str.encode('utf-8'))

        upload_part()

    # Close JSON array
    buffer.write("\n]")
    current_size += 2
    upload_part(force=True)

    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=output_key,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts}
    )
except Exception as e:
    print("Error:", e)
    s3.abort_multipart_upload(Bucket=bucket, Key=output_key, UploadId=upload_id)
    raise
