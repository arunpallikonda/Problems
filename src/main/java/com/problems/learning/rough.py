import boto3
import json
import io

from boto3.dynamodb.types import TypeSerializer

s3 = boto3.client('s3')
bucket = 'your-bucket'
input_key = 'output-normal-json.json'
output_key = 'input-dynamo-lines.json.gz'  # Replace file
part_size = 5 * 1024 * 1024  # 5 MB

serializer = TypeSerializer()

def to_dynamo_format(item):
    return {k: serializer.serialize(v) for k, v in item.items()}

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
    obj = s3.get_object(Bucket=bucket, Key=input_key)
    stream = obj['Body']

    # You must parse the file as a JSON array, but stream it safely
    buf = ''
    depth = 0
    inside = False

    for chunk in stream.iter_lines():
        if chunk:
            line = chunk.decode('utf-8')
            buf += line.strip()

            # Primitive token parsing to extract full JSON objects
            for i, ch in enumerate(line):
                if ch == '{':
                    depth += 1
                    inside = True
                elif ch == '}':
                    depth -= 1
                    if depth == 0 and inside:
                        inside = False
                        # We found a full JSON object
                        json_str = buf.strip().rstrip(',').lstrip('[').rstrip(']')
                        buf = ''

                        if not json_str:
                            continue
                        item = json.loads(json_str)
                        dynamo_json = json.dumps(to_dynamo_format(item))
                        buffer.write(dynamo_json + "\n")
                        current_size += len(dynamo_json.encode('utf-8')) + 1

                        upload_part()

    upload_part(force=True)

    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=output_key,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts}
    )
    print("✅ Multipart upload completed.")
except Exception as e:
    print("❌ Error during upload:", str(e))
    s3.abort_multipart_upload(Bucket=bucket, Key=output_key, UploadId=upload_id)
    raise
