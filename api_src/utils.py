# import json
import os
import tempfile
from datetime import datetime

from dotenv import load_dotenv
from flask import jsonify, request
from google.cloud import storage

# --- Configuration Loading ---
load_dotenv()

# Get the project root directory
PROJECT_ROOT = os.getcwd()

# Load other environment variables
SOURCE_TYPE = os.getenv('SOURCE_TYPE')
BUCKET_NAME = os.getenv('BUCKET_NAME')
PREFIX = os.getenv('PREFIX')
PREFIX_PDF_UNLOCKED = os.getenv('PREFIX_PDF_UNLOCKED')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
PASSWORD_PDF = os.getenv('PASSWORD_PDF')
SILVER_BUCKET_NAME = os.getenv('SILVER_BUCKET_NAME')
API_KEY = os.getenv('API_KEY')

# --- Dynamic Path Construction ---
LOCAL_PATH_RELATIVE = os.getenv('LOCAL_PATH', 'cuentas_pdf')
OUTPUT_LOCAL_PATH_RELATIVE = os.getenv('OUTPUT_LOCAL_PATH', 'unlocked_pdf')

LOCAL_PATH = os.path.join(PROJECT_ROOT, LOCAL_PATH_RELATIVE)
OUTPUT_LOCAL_PATH = os.path.join(PROJECT_ROOT, OUTPUT_LOCAL_PATH_RELATIVE)

# Ensure local directories exist
os.makedirs(LOCAL_PATH, exist_ok=True)
os.makedirs(OUTPUT_LOCAL_PATH, exist_ok=True)


# --- Robust Service Account Credential Handling ---
def get_service_account_info():
    """
    Build and return Google Cloud service account credentials from
    discrete environment variables (field by field).

    Background:
    - The project migrated from using a single JSON variable
        (SERVICE_ACCOUNT_JSON or SERVICE_ACCOUNT_JSON_PATH) to per-field
        variables defined in .env.
    - This function reads environment variables and constructs the object
        expected by google.cloud.storage.Client.from_service_account_info.

    Reads these variables:
    - GCP_SERVICE_ACCOUNT_TYPE
    - GCP_SERVICE_ACCOUNT_PROJECT_ID
    - GCP_SERVICE_ACCOUNT_PRIVATE_KEY_ID
    - GCP_SERVICE_ACCOUNT_PRIVATE_KEY
    - GCP_SERVICE_ACCOUNT_CLIENT_EMAIL
    - GCP_SERVICE_ACCOUNT_CLIENT_ID
    - GCP_SERVICE_ACCOUNT_AUTH_URI
    - GCP_SERVICE_ACCOUNT_TOKEN_URI
    - GCP_SERVICE_ACCOUNT_AUTH_PROVIDER_X509_CERT_URL
    - GCP_SERVICE_ACCOUNT_CLIENT_X509_CERT_URL
    - GCP_SERVICE_ACCOUNT_UNIVERSE_DOMAIN

    Returns:
    - dict: a dictionary with the service account fields, usable by
        google.cloud.storage.Client.from_service_account_info.

    Notes:
    - Missing variables will result in None values. The Google client may
        fail if required fields are absent. Ensure all variables are set.
    - In Docker, inject variables via `--env-file .env` or `-e NAME=value`.
    """

    sa_type = os.getenv('GCP_SERVICE_ACCOUNT_TYPE')
    project_id = os.getenv('GCP_SERVICE_ACCOUNT_PROJECT_ID')
    private_key_id = os.getenv('GCP_SERVICE_ACCOUNT_PRIVATE_KEY_ID')
    private_key = os.getenv('GCP_SERVICE_ACCOUNT_PRIVATE_KEY')
    client_email = os.getenv('GCP_SERVICE_ACCOUNT_CLIENT_EMAIL')
    client_id = os.getenv('GCP_SERVICE_ACCOUNT_CLIENT_ID')
    auth_uri = os.getenv('GCP_SERVICE_ACCOUNT_AUTH_URI')
    token_uri = os.getenv('GCP_SERVICE_ACCOUNT_TOKEN_URI')
    auth_provider_x509_cert_url = os.getenv(
        'GCP_SERVICE_ACCOUNT_AUTH_PROVIDER_X509_CERT_URL'
    )
    client_x509_cert_url = os.getenv('GCP_SERVICE_ACCOUNT_CLIENT_X509_CERT_URL')
    universe_domain = os.getenv('GCP_SERVICE_ACCOUNT_UNIVERSE_DOMAIN')

    service_account_data = {
        'type': sa_type,
        'project_id': project_id,
        'private_key_id': private_key_id,
        'private_key': private_key,
        'client_email': client_email,
        'client_id': client_id,
        'auth_uri': auth_uri,
        'token_uri': token_uri,
        'auth_provider_x509_cert_url': auth_provider_x509_cert_url,
        'client_x509_cert_url': client_x509_cert_url,
        'universe_domain': universe_domain,
    }

    return service_account_data

    # if sa_content_str:
    #     try:
    #         return json.loads(sa_content_str)
    #     except json.JSONDecodeError:
    #         # Fall through to try the path if content is not valid JSON
    #         pass

    # sa_path_str = os.getenv('SERVICE_ACCOUNT_JSON_PATH')
    # if sa_path_str:
    #     full_path = os.path.join(PROJECT_ROOT, sa_path_str)
    #     try:
    #         with open(full_path, 'r', encoding='utf-8') as f:
    #             return json.load(f)
    #     except FileNotFoundError:
    #         raise ValueError(f'Credential file not found at path: {full_path}')

    # raise ValueError(
    #     'Could not load Google Cloud credentials. '
    #     "Set either 'SERVICE_ACCOUNT_JSON' (with JSON content) or "
    #     "'SERVICE_ACCOUNT_JSON_PATH' (with a file path)."
    # )


# Load credentials once on startup
SERVICE_ACCOUNT_INFO = get_service_account_info()


def parse_date(s):
    """
    Parse a date string in 'YYYY-MM-DD' format to a datetime.date object.
    Args:
        s (str): Date string in 'YYYY-MM-DD' format.
    Returns:
        datetime.date: Parsed date object.
    Raises:
        ValueError: If the input string does not match the 'YYYY-MM-DD' format.
    """
    return datetime.strptime(s, '%Y-%m-%d').date()


def get_dates():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return (
            None,
            None,
            jsonify({'error': 'Ingresa las fechas correspondientes'}),
            400,
        )
    start = parse_date(start_date)
    end = parse_date(end_date)
    if start > end:
        return None, None, jsonify({'error': 'start_date debe ser <= end_date'}), 400
    return start, end, None, None


def build_prefixes():
    prefix_parts = PREFIX_PDF_UNLOCKED.strip('/').split('/')
    try:
        pdf_index = prefix_parts.index('pdf')
        prefix_parts[pdf_index] = 'json'
    except ValueError:
        pass
    json_prefix = '/'.join(prefix_parts)

    parquet_prefix_parts = PREFIX_PDF_UNLOCKED.strip('/').split('/')
    try:
        pdf_index = parquet_prefix_parts.index('pdf')
        parquet_prefix_parts[pdf_index] = 'parquet'
    except ValueError:
        pass
    parquet_prefix = '/'.join(parquet_prefix_parts)

    return json_prefix, parquet_prefix


def list_json_paths(gcs, path, start, end):
    return gcs.list_files_by_date(
        path=path,
        start=start,
        end=end,
        extension='.json',
    )


def download_json_files(bucket, json_paths):
    client = storage.Client.from_service_account_info(SERVICE_ACCOUNT_INFO)
    bucket = client.bucket(BUCKET_NAME)
    json_files = []
    for json_blob in json_paths:
        blob_path = json_blob[len(f'gs://{BUCKET_NAME}/') :]
        tmp_fd, tmp_path = tempfile.mkstemp(suffix='.json')
        os.close(tmp_fd)
        bucket.blob(blob_path).download_to_filename(tmp_path)
        with open(tmp_path, 'rb') as f:
            json_files.append((f.read(), json_blob))
        os.remove(tmp_path)
    return json_files


def upload_results(bucket, parquet_prefix, results, suffix, files_processed: list):
    client = storage.Client.from_service_account_info(SERVICE_ACCOUNT_INFO)
    bucket = client.bucket(BUCKET_NAME)
    for year, file_path in results:
        blob_name = f'{parquet_prefix}/{year}-{suffix}.parquet'
        bucket.blob(blob_name).upload_from_filename(file_path)
        files_processed.append(f'gs://{BUCKET_NAME}/{blob_name}')
        os.remove(file_path)
