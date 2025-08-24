import json
import boto3
from PIL import Image
import io

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    image_data = response['Body'].read()
    
    image = Image.open(io.BytesIO(image_data))
    image = image.resize((256, 256))
    
    output_buffer = io.BytesIO()
    image.save(output_buffer, format='JPEG')
    output_key = f"processed/{key}"
    
    s3_client.put_object(Bucket=bucket, Key=output_key, Body=output_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {key} and saved to {output_key}")
    }
