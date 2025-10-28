import json
import os
import io
import logging
from urllib.parse import unquote_plus

import boto3
from PIL import Image, ImageOps

# log
s3_client = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Config
PROCESSED_PREFIX = os.getenv('PROCESSED_PREFIX', 'processed/')
MAX_INPUT_SIZE_BYTES = int(os.getenv('MAX_INPUT_SIZE_BYTES', 50 * 1024 * 1024))  # 50 MB default
OUTPUT_SIZE = (256, 256)
DEFAULT_OUTPUT_FORMAT = 'JPEG'  # fallback

CONTENT_TYPE_MAP = {
    'JPEG': 'image/jpeg',
    'JPG': 'image/jpeg',
    'PNG': 'image/png',
    'WEBP': 'image/webp',
}


def _safe_get_record(event):
    try:
        return event['Records'][0]
    except Exception:
        raise ValueError("Invalid event structure: expected event['Records'][0]")


def _determine_output_format(original_format, image):
    
    if original_format:
        fmt = original_format.upper()
        if fmt in CONTENT_TYPE_MAP:
            return fmt
    # If original not known, but imagealpha, use PNG
    if 'A' in getattr(image, 'mode', ''):
        return 'PNG'
    return DEFAULT_OUTPUT_FORMAT


def _composite_for_jpeg(image):
    # Convert images with alpha to RGB by compositing over white background
    if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):
        background = Image.new('RGB', image.size, (255, 255, 255))
        background.paste(image.convert('RGBA'), mask=image.convert('RGBA').split()[3])
        return background
    return image.convert('RGB')


def lambda_handler(event, context):
    """
    Lambda S3 handler: fetches image, resizes it, and writes to processed/{key}.
    Improvements:
    - URL-decodes S3 key
    - Avoids recursion by skipping objects already under processed prefix
    - Validates size and type
    - Handles transparency when saving to JPEG
    - Preserves content-type where possible and copies original metadata
    - Adds error handling and logging
    """
    try:
        record = _safe_get_record(event)
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        logger.info("Processing s3://%s/%s", bucket, key)
    except Exception as e:
        logger.exception("Failed to parse event")
        return {"statusCode": 400, "body": json.dumps(f"Bad event: {e}")}

    # Avoid processing already-processed objects (recursion)
    if key.startswith(PROCESSED_PREFIX):
        msg = f"Skipping {key} because it is under the processed prefix."
        logger.info(msg)
        return {"statusCode": 200, "body": json.dumps(msg)}

    # Fetch object
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        logger.exception("Failed to get object from S3")
        return {"statusCode": 500, "body": json.dumps(f"Failed to get object: {e}")}

    content_length = response.get('ContentLength')
    if content_length and content_length > MAX_INPUT_SIZE_BYTES:
        msg = f"Object too large ({content_length} bytes), skipping."
        logger.warning(msg)
        return {"statusCode": 413, "body": json.dumps(msg)}

    body = response['Body'].read()
    input_content_type = response.get('ContentType')
    original_metadata = response.get('Metadata', {})

    # Open image with Pillow
    try:
        image = Image.open(io.BytesIO(body))
        image.load()  # make sure image is fully loaded
    except Exception as e:
        logger.exception("Failed to open image with Pillow")
        return {"statusCode": 415, "body": json.dumps(f"File is not a valid image: {e}")}

    # Determine output format
    output_format = _determine_output_format(image.format, image)
    logger.info("Original format=%s, chosen output format=%s", image.format, output_format)

    # Resize - use high-quality filter
    try:
        image = ImageOps.exif_transpose(image)  # honor EXIF orientation
        image = image.convert('RGBA') if 'A' in getattr(image, 'mode', '') else image.convert('RGB')
        image = image.resize(OUTPUT_SIZE, Image.LANCZOS)
    except Exception as e:
        logger.exception("Failed to resize image")
        return {"statusCode": 500, "body": json.dumps(f"Failed to resize image: {e}")}

    # Handle format-specific conversions (e.g., transparency -> JPEG)
    if output_format in ('JPEG', 'JPG'):
        image_to_save = _composite_for_jpeg(image)
    else:
        # For PNG/WEBP we can keep RGBA if present
        image_to_save = image

    # Prepare output buffer
    output_buffer = io.BytesIO()
    save_params = {}
    if output_format in ('JPEG', 'JPG'):
        save_params['quality'] = 85
        save_params['optimize'] = True
    if output_format == 'PNG':
        save_params['optimize'] = True

    try:
        image_to_save.save(output_buffer, format=output_format, **save_params)
        output_buffer.seek(0)
    except Exception as e:
        logger.exception("Failed to save processed image to buffer")
        return {"statusCode": 500, "body": json.dumps(f"Failed to encode image: {e}")}

    # Build output key and content type
    filename = os.path.basename(key)
    name_root, _ = os.path.splitext(filename)
    ext = output_format.lower()
    # Keep original folder structure under processed/
    key_dir = os.path.dirname(key)
    processed_subpath = f"{key_dir}/{name_root}.{ext}" if key_dir else f"{name_root}.{ext}"
    output_key = f"{PROCESSED_PREFIX.rstrip('/')}/{processed_subpath}"

    content_type = CONTENT_TYPE_MAP.get(output_format, input_content_type or 'application/octet-stream')

    # Upload
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=output_buffer.getvalue(),
            ContentType=content_type,
            Metadata=original_metadata
        )
    except Exception as e:
        logger.exception("Failed to put object to S3")
        return {"statusCode": 500, "body": json.dumps(f"Failed to write processed image: {e}")}

    msg = f"Processed {key} and saved to {output_key}"
    logger.info(msg)
    return {"statusCode": 200, "body": json.dumps(msg)}
