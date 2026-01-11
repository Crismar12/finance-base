# api.py
import json
import os
import tempfile
from functools import wraps

import pandas as pd
from flask import Flask, jsonify, request
from google.cloud import storage

from api_src.data_lake.connector import DataLakeConnector, GCSConnector
from api_src.endpoints.parse_document.openai_agent import OpenAIAssistant
from api_src.endpoints.process_data.data_processing import (
    unify_items_and_statements_gcs,
)
from api_src.endpoints.remove_password.pdf_processor import PDFProcessor
from api_src.endpoints.structure_data.data_structuring import (
    generate_statement_id,
    transform_credit_card_json,
    transform_statement_items_json,
    transform_statement_upcoming_dues_json,
)

# Import centralized configuration from utils
from .utils import (
    API_KEY,
    BUCKET_NAME,
    OPENAI_API_KEY,
    OUTPUT_LOCAL_PATH,
    PREFIX,
    PREFIX_PDF_UNLOCKED,
    SERVICE_ACCOUNT_INFO,
    build_prefixes,
    download_json_files,
    get_dates,
    list_json_paths,
    parse_date,
    upload_results,
)

app = Flask(__name__)


def api_key_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if (
            request.headers.get('X-API-Key')
            and request.headers.get('X-API-Key') == API_KEY
        ):
            return f(*args, **kwargs)
        else:
            return jsonify({'message': 'Unauthorized: Invalid or missing API Key'}), 401

    return decorated_function


@app.route('/remove_password', methods=['GET'])
@api_key_required
def unlock_and_save_pdfs():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not start_date or not end_date:
        return jsonify({'error': 'Ingresa las fechas correspondientes'}), 400

    # Use the centralized SERVICE_ACCOUNT_INFO
    gcs = GCSConnector(
        service_account_info=SERVICE_ACCOUNT_INFO, bucket_name=BUCKET_NAME
    )
    start = parse_date(start_date)
    end = parse_date(end_date)

    pdf_paths = gcs.list_pdfs_by_date(
        path=f'gs://{BUCKET_NAME}/{PREFIX}',
        start=start,
        end=end,
    )

    processed_files = []

    if not pdf_paths:
        return jsonify({'source': 'GCS', 'files_processed': [], 'total_files': 0})

    # Use the centralized SERVICE_ACCOUNT_INFO
    client = storage.Client.from_service_account_info(SERVICE_ACCOUNT_INFO)
    bucket = client.bucket(BUCKET_NAME)

    for pdf_blob in pdf_paths:
        try:
            gs_uri_in = pdf_blob
            blob_path = pdf_blob.replace(f'gs://{BUCKET_NAME}/', '')

            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.pdf')
            os.close(tmp_fd)
            bucket.blob(blob_path).download_to_filename(tmp_path)

            # Use the centralized, absolute OUTPUT_LOCAL_PATH
            final_path = os.path.join(OUTPUT_LOCAL_PATH, os.path.basename(pdf_blob))

            processor = PDFProcessor(tmp_path, original_path=gs_uri_in)
            # Pass SERVICE_ACCOUNT_INFO to the processor
            processor.remove_password_if_needed(
                output_path=final_path,
                upload_to_gcs_prefix=PREFIX_PDF_UNLOCKED,
                keep_local_copy=False,
            )

            processed_files.append(processor.last_uploaded_gs_uri)

            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception as e:
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(f'ERROR: No se pudo procesar el archivo {pdf_blob}')
            print(f'Excepción: {e}')
            import traceback

            traceback.print_exc()
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
            continue

    return jsonify(
        {
            'source': 'GCS',
            'files_processed': processed_files,
            'total_files': len(processed_files),
        }
    )


@app.route('/parse_document', methods=['GET'])
@api_key_required
def extract_and_save_json():
    print('Endpoint /parse_document: Iniciando procesamiento.')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not start_date or not end_date:
        print('Endpoint /parse_document: Error - Fechas no proporcionadas.')
        return jsonify({'error': 'Ingresa las fechas correspondientes'}), 400

    print(f'Endpoint /parse_document: Rango de fechas: {start_date} a {end_date}')
    gcs = GCSConnector(
        service_account_info=SERVICE_ACCOUNT_INFO, bucket_name=BUCKET_NAME
    )

    start = parse_date(start_date)
    end = parse_date(end_date)

    if start > end:
        print('Endpoint /parse_document: Error - start_date debe ser <= end_date.')
        return jsonify({'error': 'start_date debe ser <= end_date'}), 400

    assistant = OpenAIAssistant(api_key=OPENAI_API_KEY)
    processed_files = []

    print('Endpoint /parse_document: Listando PDFs en GCS.')
    pdf_paths = gcs.list_pdfs_by_date(
        path=f'gs://{BUCKET_NAME}/{PREFIX_PDF_UNLOCKED}',
        start=start,
        end=end,
    )
    print(f'Endpoint /parse_document: Se encontraron {len(pdf_paths)} PDFs.')

    if not pdf_paths:
        return jsonify({'source': 'GCS', 'files_processed': [], 'total_files': 0})

    bucket = gcs.client.bucket(BUCKET_NAME)

    for i, pdf_blob in enumerate(pdf_paths):
        print(f'--- Procesando archivo {i+1}/{len(pdf_paths)}: {pdf_blob} ---')
        try:
            blob_path = pdf_blob.replace(f'gs://{BUCKET_NAME}/', '')

            print('  Paso 1: Descargando PDF a archivo temporal...')
            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.pdf')
            os.close(tmp_fd)
            bucket.blob(blob_path).download_to_filename(tmp_path)
            print(f'  Paso 1: PDF descargado a {tmp_path}')

            print('  Paso 2: Leyendo contenido del PDF.')
            processor = PDFProcessor(tmp_path, original_path=pdf_blob)
            raw_text = processor.read_content()
            print('  Paso 2: Contenido leído exitosamente.')

            print('  Paso 3: Extrayendo datos con OpenAI...')
            json_data = assistant.extract_data(raw_text)
            print('  Paso 3: Datos extraídos exitosamente.')

            date_obj = DataLakeConnector._try_parse_date_any(pdf_blob)
            year_folder = str(date_obj.year) if date_obj else 'unclassified'
            json_filename = os.path.basename(blob_path).replace('.pdf', '.json')

            print('  Paso 4: Guardando JSON en archivo temporal...')
            tmp_json_path = os.path.join(tempfile.gettempdir(), json_filename)
            with open(tmp_json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            print(f'  Paso 4: JSON guardado en {tmp_json_path}')

            prefix_parts = PREFIX_PDF_UNLOCKED.strip('/').split('/')
            try:
                pdf_index = prefix_parts.index('pdf')
                prefix_parts[pdf_index] = 'json'
            except ValueError:
                pass
            json_prefix = '/'.join(prefix_parts)

            json_blob_path = f"{json_prefix.rstrip('/')}/{year_folder}/{json_filename}"

            print(f"  Paso 5: Subiendo JSON a GCS en '{json_blob_path}'...")
            bucket.blob(json_blob_path).upload_from_filename(tmp_json_path)
            print('  Paso 5: JSON subido exitosamente.')

            os.remove(tmp_path)
            os.remove(tmp_json_path)

            processed_files.append(f'gs://{BUCKET_NAME}/{json_blob_path}')
            print(f'--- Archivo {pdf_blob} procesado con éxito. ---')

        except Exception as e:
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(f'ERROR: No se pudo procesar el archivo {pdf_blob}')
            print(f'Excepción: {e}')
            import traceback

            traceback.print_exc()
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
            continue

    print('Endpoint /parse_document: Procesamiento completado.')

    return jsonify(
        {
            'source': 'GCS',
            'files_processed': processed_files,
            'total_files': len(processed_files),
        }
    )


@app.route('/structure_data', methods=['GET'])
@api_key_required
def structure_data():
    start, end, error_response, status = get_dates()
    if error_response:
        return error_response, status

    # Use the centralized SERVICE_ACCOUNT_INFO
    gcs = GCSConnector(
        service_account_info=SERVICE_ACCOUNT_INFO, bucket_name=BUCKET_NAME
    )
    json_prefix, parquet_prefix = build_prefixes()
    json_paths = list_json_paths(gcs, f'gs://{BUCKET_NAME}/{json_prefix}', start, end)

    if not json_paths:
        return jsonify({'source': 'GCS', 'files_processed': [], 'total_files': 0})

    bucket = gcs.client.bucket(BUCKET_NAME)
    json_files = download_json_files(bucket, json_paths)

    all_statements = []
    all_items = []
    all_dues = []

    for raw_json, raw_bucket_path in json_files:
        try:
            statement_id = generate_statement_id()

            statement_rec = transform_credit_card_json(
                raw_json, raw_bucket_path, statement_id
            )
            if statement_rec:
                all_statements.append(statement_rec)

            item_recs = transform_statement_items_json(
                raw_json, raw_bucket_path, statement_id
            )
            all_items.extend(item_recs)

            due_recs = transform_statement_upcoming_dues_json(
                raw_json, raw_bucket_path, statement_id
            )
            all_dues.extend(due_recs)
        except Exception as e:
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(f'ERROR: No se pudo procesar el archivo {raw_bucket_path}')
            print(f'Excepción: {e}')
            import traceback

            traceback.print_exc()
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            continue

    files_processed = []

    if all_statements:
        df_statements = pd.DataFrame(all_statements)
        df_statements['_year'] = pd.to_datetime(df_statements['statement_date']).dt.year
        for year, gdf in df_statements.groupby('_year'):
            fd, tmp_path = tempfile.mkstemp(suffix='.parquet')
            os.close(fd)
            gdf.drop(columns=['_year']).to_parquet(tmp_path, index=False)
            upload_results(
                bucket,
                parquet_prefix,
                [(int(year), tmp_path)],
                'statements',
                files_processed,
            )

    if all_items:
        df_items = pd.DataFrame(all_items)
        if 'year' in df_items.columns:
            for year, gdf in df_items.groupby('year'):
                fd, tmp_path = tempfile.mkstemp(suffix='.parquet')
                os.close(fd)
                gdf.to_parquet(tmp_path, index=False)
                upload_results(
                    bucket,
                    parquet_prefix,
                    [(int(year), tmp_path)],
                    'statement_items',
                    files_processed,
                )

    if all_dues:
        df_dues = pd.DataFrame(all_dues)
        if 'year' in df_dues.columns:
            for year, gdf in df_dues.groupby('year'):
                fd, tmp_path = tempfile.mkstemp(suffix='.parquet')
                os.close(fd)
                gdf.to_parquet(tmp_path, index=False)
                upload_results(
                    bucket,
                    parquet_prefix,
                    [(int(year), tmp_path)],
                    'statement_upcoming_dues',
                    files_processed,
                )

    return jsonify(
        {
            'source': 'GCS',
            'files_processed': files_processed,
            'total_files': len(files_processed),
        }
    )


@app.route('/process_data', methods=['GET'])
@api_key_required
def process_data():
    try:
        start, end, error_response, status = get_dates()
        if error_response:
            return error_response, status

        gcs = GCSConnector(
            service_account_info=SERVICE_ACCOUNT_INFO, bucket_name=BUCKET_NAME
        )

        _, parquet_prefix = build_prefixes()

        results = unify_items_and_statements_gcs(
            gcs=gcs,
            bucket_name=BUCKET_NAME,
            parquet_prefix=parquet_prefix,
            start_date=start,
            end_date=end,
        )

        return jsonify(
            {
                'source': 'GCS',
                'files_processed': results,
                'total_files': len(results),
            }
        )
    except Exception as e:
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        print('ERROR: Falló el endpoint /process_data')
        print(f'Excepción: {e}')
        import traceback

        traceback.print_exc()
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500


if __name__ == '__main__':
    app.run(debug=True)
