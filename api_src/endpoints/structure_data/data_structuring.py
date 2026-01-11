# -*- coding: utf-8 -*-
"""
Transformación de JSONs de estados de cuenta de tarjeta de crédito a Parquet,
con mapeo embebido a columnas canónicas, generación de statement_id (UUIDv4),
data_source_hash (SHA-256 del JSON fuente) y created_at (UTC).
"""

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import pandas as pd
from api_src.endpoints.structure_data import utils_data_structuring

# ==========================
# Utilidades de parsing
# ==========================

DATE_INPUT_FORMATS = [
    '%d/%m/%Y',  # 24/01/2025
    '%d-%m-%Y',
    '%Y-%m-%d',
    '%Y/%m/%d',
]


def now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def parse_date_loose(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    for fmt in DATE_INPUT_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def collapse_spaces(s: str) -> str:
    # Trimea y colapsa espacios
    return re.sub(r'\s+', ' ', s.strip())


def normalize_string(v: Any) -> Optional[str]:
    if v is None:
        return None
    return collapse_spaces(str(v))


def extract_last4_digits(s: str) -> Optional[str]:
    m = re.search(r'(\d{4})\D*$', s)
    return m.group(1) if m else None


def normalize_card_mask(v: Any) -> Optional[str]:
    """
    Retorna en formato XXXX-XXXX-XXXX-1234.
    Si solo viene el último 4, generamos la máscara estándar.
    Si ya viene masked, extraemos los 4 últimos dígitos y rearmamos.
    """
    if v is None:
        return None
    s = str(v)
    last4 = extract_last4_digits(s)
    if not last4:
        return None
    return f'XXXX-XXXX-XXXX-{last4}'


def parse_number_cl(v: Any) -> Optional[float]:
    """
    Convierte strings numéricos chilenos a float. Acepta:
    - 1.234.567
    - 1.234.567,89
    - -352.000
    - " 38,52 "
    - "5.000.000"
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip()
    if not s or s in {'-', 'null', 'None'}:
        return None

    # Elimina símbolos no numéricos excepto . , -
    s = re.sub(r'[^\d.,\-]', '', s)

    # Si tiene punto y coma → punto es miles, coma es decimal
    if '.' in s and ',' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s and '.' not in s:
        s = s.replace(',', '.')  # coma como decimal
    # Si solo tiene punto, ya está

    try:
        return float(s)
    except Exception:
        return None


def parse_percent(v: Any) -> Optional[float]:
    """
    Convierte porcentajes a float (por ejemplo, '1,23%' -> 1.23).
    """
    if v is None or v == '':
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace('%', '')
    n = parse_number_cl(s)
    return n


def get_by_path(obj: Any, path: str) -> Any:
    """
    Navega diccionarios con dot-notation.
    """
    cur = obj
    for part in path.split('.'):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def pick_first_by_aliases(
    root: Dict[str, Any],
    aliases: List[str],
    node_hint: Optional[str] = 'estado_de_cuenta',
) -> Any:
    """
    Intenta primero bajo el nodo 'estado_de_cuenta', si existe; luego en root.
    Además respeta las rutas absolutas ya incluidas en los alias.
    """
    # Si alias ya incluye 'estado_de_cuenta.' es ruta absoluta
    for path in aliases:
        val = get_by_path(root, path)
        if val is not None:
            return val

    # Si los alias son claves simples, intenta bajo el nodo y luego en root
    node = root.get(node_hint) if isinstance(root, dict) else None
    for path in aliases:
        if '.' not in path:
            if isinstance(node, dict) and path in node:
                return node.get(path)
            if isinstance(root, dict) and path in root:
                return root.get(path)
    return None


def coerce_value(key: str, val: Any) -> Any:
    t = utils_data_structuring.CANONICAL_TYPES.get(key)
    if t == 'str':
        return normalize_string(val) if val is not None else None
    if t == 'card_mask':
        s = normalize_card_mask(val)
        return s
    if t == 'date':
        return parse_date_loose(val)
    if t == 'money':
        n = parse_number_cl(val)
        return n if n is not None else None
    if t == 'pct':
        p = parse_percent(val)
        return p if p is not None else None
    return val


def compute_derived_fields(rec: Dict[str, Any]) -> None:
    """
    - currency_code: default 'CLP' si viene vacío
    - available_credit: si falta pero total y used existen, calcula total - used (min 0)
    """
    if not rec.get('currency_code'):
        rec['currency_code'] = 'CLP'

    if (
        rec.get('available_credit') is None
        and rec.get('total_credit_limit') is not None
        and rec.get('used_credit') is not None
    ):
        try:
            diff = float(rec['total_credit_limit']) - float(rec['used_credit'])
            # No forzamos >=0 estrictamente, pero puedes clamp si lo deseas:
            rec['available_credit'] = diff
        except Exception:
            pass


def derive_year(rec: Dict[str, Any]) -> Optional[int]:
    """
    Año para particionar Parquet, derivado de statement_date.
    """
    iso = rec.get('statement_date')
    if not iso:
        return None
    try:
        return int(str(iso)[:4])
    except Exception:
        return None


def generate_statement_id() -> str:
    return uuid.uuid4().hex


def hash_data_source(raw_json_bytes: bytes) -> str:
    return hashlib.sha256(raw_json_bytes).hexdigest()


def build_item_record(
    item: dict,
    category_name: str,
    idx: int,
    statement_id: str,
    data_hash: str,
    created_at: str,
    ingestion_ts: str,
    raw_bucket_path: str,
    statement_year: int,
) -> dict:
    """
    Construye un registro canónico de la tabla items a partir de un item del JSON.
    """
    rec = {
        'item_id': str(uuid.uuid4()),
        'statement_id': statement_id,
        'data_source_hash': data_hash,
        'created_at': created_at,
        'ingestion_ts': ingestion_ts,
        'raw_bucket_path': raw_bucket_path,
        'category': category_name,
        'line_order': idx,
        'year': statement_year,
    }

    # Mapear campos según COLUMN_MAP_ITEMS
    for canonical, aliases in utils_data_structuring.COLUMN_MAP_ITEMS.items():
        if canonical in rec:
            continue
        value = pick_first_by_aliases(item, aliases)
        rec[canonical] = coerce_value(canonical, value)

    # Derivar signo
    amount = rec.get('operation_amount')
    if isinstance(amount, (int, float)) and pd.notnull(amount):
        rec['sign'] = 1 if amount >= 0 else -1
    else:
        rec['sign'] = None

    # Moneda por defecto
    rec['currency_code'] = rec.get('currency_code') or 'CLP'

    return rec


def transform_credit_card_json(
    raw_json: bytes, raw_bucket_path: str, statement_id: str
) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(raw_json)
    except Exception:
        return None

    rec: Dict[str, Any] = {'raw_bucket_path': raw_bucket_path}

    for canonical, aliases in utils_data_structuring.COLUMN_MAP.items():
        if canonical in ('statement_id', 'data_source_hash', 'created_at'):
            continue
        value = pick_first_by_aliases(data, aliases)
        coerced = coerce_value(canonical, value)
        rec[canonical] = coerced

    compute_derived_fields(rec)

    year = derive_year(rec)
    if year is None:
        raw_date = pick_first_by_aliases(
            data, utils_data_structuring.COLUMN_MAP.get('statement_date', [])
        )
        iso = parse_date_loose(raw_date)
        if iso:
            rec['statement_date'] = iso
            year = derive_year(rec)

    if year is None:
        return None

    rec['statement_id'] = statement_id
    rec['data_source_hash'] = hash_data_source(raw_json)
    rec['created_at'] = now_utc_iso()

    if rec.get('cardholder_name'):
        rec['cardholder_name'] = collapse_spaces(rec['cardholder_name'])

    return {k: rec.get(k) for k in utils_data_structuring.CANONICAL_ORDER}


def extract_statement_year(data: dict) -> int:
    year = datetime.utcnow().year
    try:
        fecha_corte = get_by_path(data, 'estado_de_cuenta.fecha')
        if fecha_corte:
            parsed = pd.to_datetime(fecha_corte, errors='coerce', dayfirst=True)
            if pd.notnull(parsed):
                year = int(parsed.year)
    except Exception:
        pass
    return year


def process_category_items(
    payload: dict,
    category: str,
    statement_id: str,
    data_hash: str,
    created_at: str,
    ingestion_ts: str,
    raw_bucket_path: str,
    statement_year: int,
) -> List[dict]:
    rows = []
    if isinstance(payload, dict):
        items = payload.get('items', [])
        if items:
            for idx, item in enumerate(items, start=1):
                rec = build_item_record(
                    item,
                    category,
                    idx,
                    statement_id,
                    data_hash,
                    created_at,
                    ingestion_ts,
                    raw_bucket_path,
                    statement_year,
                )
                rows.append(rec)
        else:
            rec = build_item_record(
                {},
                category,
                1,
                statement_id,
                data_hash,
                created_at,
                ingestion_ts,
                raw_bucket_path,
                statement_year,
            )
            rows.append(rec)
    return rows


def process_extras(
    extras: list,
    statement_id: str,
    data_hash: str,
    created_at: str,
    ingestion_ts: str,
    raw_bucket_path: str,
    statement_year: int,
) -> List[dict]:
    rows = []
    if isinstance(extras, list) and extras:
        for section in extras:
            titulo = section.get('titulo')
            items = section.get('items', [])
            if isinstance(items, list) and items:
                for idx, item in enumerate(items, start=1):
                    rec = build_item_record(
                        item,
                        titulo,
                        idx,
                        statement_id,
                        data_hash,
                        created_at,
                        ingestion_ts,
                        raw_bucket_path,
                        statement_year,
                    )
                    rows.append(rec)
            else:
                rec = build_item_record(
                    {},
                    titulo,
                    1,
                    statement_id,
                    data_hash,
                    created_at,
                    ingestion_ts,
                    raw_bucket_path,
                    statement_year,
                )
                rows.append(rec)
    return rows


def normalize_items_types(df: pd.DataFrame) -> pd.DataFrame:
    for col, typ in utils_data_structuring.CANONICAL_TYPES_ITEMS.items():
        if typ == 'str':
            df[col] = df[col].astype('string')
        elif typ == 'float':
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif typ == 'int':
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        elif typ == 'date':
            if col in {'created_at', 'ingestion_ts'}:
                df[col] = pd.to_datetime(df[col], errors='coerce', format='ISO8601')
            elif col == 'transaction_date':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = df[col].astype('string')
    return df


def transform_statement_items_json(
    raw_json: bytes, raw_bucket_path: str, statement_id: str
) -> List[Dict[str, Any]]:
    rows = []
    try:
        data = json.loads(raw_json)
    except Exception:
        return rows

    data_hash = hash_data_source(raw_json)
    created_at = now_utc_iso()
    ingestion_ts = now_utc_iso()
    statement_year = extract_statement_year(data)

    forced_categories = {
        'pagos_a_la_cuenta': 'estado_de_cuenta.detalle.periodo_actual.operaciones.pagos_a_la_cuenta',
        'compras_en_cuotas': 'estado_de_cuenta.detalle.periodo_actual.operaciones.compras_en_cuotas',
        'voluntariamente_contratados_sin_movimientos': (
            'estado_de_cuenta.detalle.periodo_actual.voluntariamente_contratados_sin_movimientos'
        ),
        'cargos_comisiones_impuestos_abonos': (
            'estado_de_cuenta.detalle.periodo_actual.cargos_comisiones_impuestos_abonos'
        ),
    }

    for category, path in forced_categories.items():
        payload = get_by_path(data, path)
        rows.extend(
            process_category_items(
                payload,
                category,
                statement_id,
                data_hash,
                created_at,
                ingestion_ts,
                raw_bucket_path,
                statement_year,
            )
        )

    extras = get_by_path(
        data, 'estado_de_cuenta.detalle.periodo_actual.extras_periodo_actual'
    )
    rows.extend(
        process_extras(
            extras,
            statement_id,
            data_hash,
            created_at,
            ingestion_ts,
            raw_bucket_path,
            statement_year,
        )
    )
    return rows


def transform_statement_upcoming_dues_json(
    raw_json: bytes, raw_bucket_path: str, statement_id: str
) -> List[Dict[str, Any]]:
    rows = []
    try:
        data = json.loads(raw_json)
    except Exception:
        return rows

    data_hash = hash_data_source(raw_json)
    created_at = now_utc_iso()
    ingestion_ts = now_utc_iso()
    statement_year = extract_statement_year(data)

    vencimientos = get_by_path(
        data, 'estado_de_cuenta.informacion_de_pago.vencimiento_proximos_meses'
    )
    if isinstance(vencimientos, list):
        for idx, due in enumerate(vencimientos, start=1):
            month_label = due.get('mes')
            due_amount = due.get('monto')

            rec = {
                'due_id': str(uuid.uuid4()),
                'statement_id': statement_id,
                'data_source_hash': data_hash,
                'created_at': created_at,
                'ingestion_ts': ingestion_ts,
                'raw_bucket_path': raw_bucket_path,
                'month_label': month_label,
                'due_amount': due_amount,
                'currency_code': 'CLP',
                'line_order': idx,
                'year': statement_year,
            }
            rows.append(rec)
    return rows
