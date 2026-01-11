import os
import tempfile
from typing import List, Set

import pandas as pd


def list_blobs_by_suffix(gcs, bucket_name: str, prefix: str, suffix: str) -> List[str]:
    bucket = gcs.client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [b.name for b in blobs if b.name.endswith(suffix)]


def extract_years_from_filenames(files: List[str]) -> Set[str]:
    years = set()
    for name in files:
        base = os.path.basename(name)
        parts = base.split('-')
        if parts and parts[0].isdigit():
            years.add(parts[0])
    return years


def download_parquet_from_gcs(gcs, bucket_name: str, blob_name: str) -> pd.DataFrame:
    bucket = gcs.client.bucket(bucket_name)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.parquet')
    os.close(tmp_fd)
    bucket.blob(blob_name).download_to_filename(tmp_path)
    df = pd.read_parquet(tmp_path)
    os.remove(tmp_path)
    return df


def upload_parquet_to_gcs(
    gcs, bucket_name: str, blob_name: str, df: pd.DataFrame
) -> str:
    bucket = gcs.client.bucket(bucket_name)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.parquet')
    os.close(tmp_fd)
    df.to_parquet(tmp_path, index=False)
    bucket.blob(blob_name).upload_from_filename(tmp_path)
    os.remove(tmp_path)
    return f'gs://{bucket_name}/{blob_name}'


def _coalesce_overlaps(df: pd.DataFrame, overlaps: Set[str]) -> pd.DataFrame:
    """
    Consolida columnas duplicadas (presentes en ambas tablas) en una sola:
    - Si existen col_st y col_it: col = col_it.combine_first(col_st)
    - Si existe solo uno de los sufijos, se renombra a col.
    """
    for col in sorted(overlaps):
        st = f'{col}_st'
        it = f'{col}_it'
        st_exists = st in df.columns
        it_exists = it in df.columns

        if st_exists and it_exists:
            df[col] = df[it].combine_first(df[st])
            df.drop(columns=[st, it], inplace=True)
        elif st_exists and not it_exists:
            # Si ya existe df[col], evitamos duplicar: preferimos no sobrescribir.
            if col in df.columns:
                # Si col ya existe, combinar y luego borrar st.
                df[col] = df[col].combine_first(df[st])
                df.drop(columns=[st], inplace=True)
            else:
                df.rename(columns={st: col}, inplace=True)
        elif it_exists and not st_exists:
            if col in df.columns:
                df[col] = df[it].combine_first(df[col])
                df.drop(columns=[it], inplace=True)
            else:
                df.rename(columns={it: col}, inplace=True)
        # Si ninguno existe, no hacemos nada (raro, pero defensivo).
    return df


def _deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina columnas duplicadas conservando la primera aparición.
    Llamar SIEMPRE como último paso antes de guardar.
    """
    return df.loc[:, ~df.columns.duplicated()]


def unify_items_and_statements_gcs(
    gcs, bucket_name: str, parquet_prefix: str, start_date, end_date
) -> List[str]:
    stmt_files = list_blobs_by_suffix(
        gcs, bucket_name, parquet_prefix, 'statements.parquet'
    )
    items_files = list_blobs_by_suffix(
        gcs, bucket_name, parquet_prefix, 'statement_items.parquet'
    )

    stmt_years = extract_years_from_filenames(stmt_files)
    items_years = extract_years_from_filenames(items_files)
    years = stmt_years.intersection(items_years)

    start_year = start_date.year
    end_year = end_date.year
    years = {year for year in years if start_year <= int(year) <= end_year}

    results = []
    for year in sorted(years):
        stmt_blob = f'{parquet_prefix}/{year}-statements.parquet'
        items_blob = f'{parquet_prefix}/{year}-statement_items.parquet'

        df_statements = download_parquet_from_gcs(gcs, bucket_name, stmt_blob)
        df_items = download_parquet_from_gcs(gcs, bucket_name, items_blob)

        unified = df_statements.merge(
            df_items,
            how='left',
            on='statement_id',
            suffixes=('_st', '_it'),
        )

        # Columnas solapadas (idénticos nombres en ambas) excepto la llave.
        overlap_cols = set(df_statements.columns).intersection(df_items.columns)
        overlap_cols.discard('statement_id')

        # Consolidar columnas duplicadas en una sola, removiendo sufijos.
        unified = _coalesce_overlaps(unified, overlap_cols)

        # Reordenar columnas: primero las del statement, luego las comunes, luego las del ítem.
        stmt_cols = [c for c in df_statements.columns if c != 'statement_id']
        item_cols = [
            c
            for c in df_items.columns
            if c not in ('statement_id',) and c not in overlap_cols
        ]
        final_cols = (
            ['statement_id'] + stmt_cols + sorted(list(overlap_cols)) + item_cols
        )
        # Reindex manteniendo las existentes; columnas faltantes se ignoran automáticamente.
        unified = unified[[c for c in final_cols if c in unified.columns]]

        # Deduplicación FINAL (paso crítico para Parquet/PyArrow).
        unified = _deduplicate_columns(unified)

        out_blob = f'{parquet_prefix}/{year}-unified-base.parquet'
        silver_bucket_name = os.getenv(
            'SILVER_BUCKET_NAME', bucket_name
        )  # Fallback to original bucket_name if env var not set
        uploaded = upload_parquet_to_gcs(gcs, silver_bucket_name, out_blob, unified)
        results.append(uploaded)

    return results
