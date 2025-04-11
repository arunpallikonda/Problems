import boto3
import json
import io
from boto3.dynamodb.types import TypeSerializer

# AWS setup
s3 = boto3.client('s3')
bucket = 'your-bucket'
input_key = 'output-normal-json.json'
output_key = 'input-dynamo-lines.json'  # Uncompressed JSON

serializer = TypeSerializer()

def to_dynamo_format(item):
    return {k: serializer.serialize(v) for k, v in item.items()}

# Multipart upload setup
part_size = 5 * 1024 * 1024  # 5MB
mpu = s3.create_multipart_upload(Bucket=bucket, Key=output_key)
upload_id = mpu['UploadId']
parts = []
part_number = 1
buffer = io.StringIO()
current_size = 0

def upload_part(force=False):
    global part_number, current_size, buffer
    if current_size >= part_size or force:
        data = buffer.getvalue().encode('utf-8')
        response = s3.upload_part(
            Body=data,
            Bucket=bucket,
            Key=output_key,
            UploadId=upload_id,
            PartNumber=part_number
        )
        parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
        part_number += 1
        buffer = io.StringIO()
        current_size = 0

try:
    # Stream input file from S3
    obj = s3.get_object(Bucket=bucket, Key=input_key)
    stream = obj['Body']

    buf = ''
    depth = 0
    inside_object = False

    for line_bytes in stream.iter_lines():
        line = line_bytes.decode('utf-8').strip()
        if not line or line in ('[', ']'):
            continue  # Skip array brackets or empty lines

        buf += line

        # Track brackets to detect full JSON object
        for char in line:
            if char == '{':
                depth += 1
                inside_object = True
            elif char == '}':
                depth -= 1
                if depth == 0 and inside_object:
                    inside_object = False
                    json_str = buf.rstrip(',')  # Remove trailing comma
                    buf = ''
                    item = json.loads(json_str)
                    dynamo_item = json.dumps(to_dynamo_format(item))
                    buffer.write(dynamo_item + '\n')
                    current_size += len(dynamo_item.encode('utf-8')) + 1
                    upload_part()

    upload_part(force=True)

    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=output_key,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts}
    )
    print("✅ Successfully converted JSON and uploaded to S3.")
except Exception as e:
    print("❌ Error:", str(e))
    s3.abort_multipart_upload(Bucket=bucket, Key=output_key, UploadId=upload_id)
    raise
