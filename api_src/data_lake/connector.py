import configparser
import glob
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
from google.cloud import storage


class DLDotPath:
    """
    Class to handle dot-separated path strings.
    """

    def __init__(
        self,
        path,
        reference: str,
        layer_switch: dict = None,
        fail_if_missing_table: bool = False,
    ):
        self.reference = Path(reference)

        if isinstance(path, type(self)):

            if path.reference != self.reference:

                raise ValueError(
                    f'Cannot initialize DLDotPath with different reference: {path.reference} != {self.reference}'
                )

            return path

        self.dotpath = None
        self.path = None
        self.layer = None
        self.zone = None
        self.domain = None
        self.table = None
        self.relative_path = None

        if not layer_switch:
            layer_switch = {
                'bronze': '1-bronze',
                'silver': '2-silver',
                'gold': '3-gold',
                'platinum': '4-platinum',
            }
        self.layer_switch = layer_switch
        self.inverted_layer_switch = {v: k for k, v in layer_switch.items()}

        if self._is_dotpath(path):
            self.dotpath = self.format_dotpath(path)
            self.path = self.get_path(path)

        elif self._is_path(path):
            self.dotpath = self.get_dotpath(path)
            self.path = Path(path)

        self.layer, self.zone, self.domain, self.table = self._parse_path_string(
            self.dotpath
        )

        self.relative_path = self._get_relative_path(self.path)

        if fail_if_missing_table and not self.table:
            raise ValueError(
                f'Invalid dotpath: {self.dotpath}. '
                'Ensure it contains a valid table name.'
            )

    def __str__(self):
        return self.dotpath

    def __repr__(self):
        return f'DotPath({self.dotpath})'

    def _is_dotpath(self, path_string: str, strict: bool = False) -> bool:
        """
        Check if the path string is a valid dot-separated path.
        """
        if isinstance(path_string, type(self)):
            return True

        if not isinstance(path_string, str):
            return False

        is_lower = path_string.islower()
        is_not_path = not os.path.exists(path_string)
        is_alnum = path_string.replace('.', '').replace('_', '').isalnum()

        if strict:
            return all([is_lower, is_not_path, is_alnum])

        return all([is_not_path, is_alnum])

    def _is_path(self, path_string: str) -> bool:
        """
        Check if the path string is a valid filesystem path.
        """
        return os.path.exists(path_string) and not self._is_dotpath(
            path_string, strict=True
        )

    def format_dotpath(self, path_string: str):
        """
        Format dot-separated path string to lower case and replace '-' and ' ' with '_'.

        Args:
            path_string: Dot-separated path string.

        Returns:
            Formatted dotpath.
        """

        output_dotpath = path_string.lower().replace('-', '_').replace(' ', '_')
        output_dotpath = ''.join(
            c for c in output_dotpath if c.isalnum() or c in {'.', '_'}
        )

        while '__' in output_dotpath:
            output_dotpath = output_dotpath.replace('__', '_')

        return output_dotpath

    def _get_relative_path(self, full_path: str) -> str:
        """
        Returns the relative path of a file given the base directory and the full path,
        handling differences in path separators across platforms.

        Args:
            base_path (str): The base directory path.
            full_path (str): The complete file path.

        Returns:
            str: The relative path from the base directory to the file.
        """
        base = self.reference.resolve()
        full = Path(full_path).resolve()

        return full.relative_to(base)

    def get_dotpath(self, path: str):
        """
        Convert filesystem path to dot-separated path string.
        If path points to a file, use its parent directory.

        Args:
            path: File system path.

        Returns:
            Dot-separated path string.
        """
        relative_path = self._get_relative_path(path)

        layer, zone, domain, table = str(relative_path).split(os.sep)[:4]

        layer = self.inverted_layer_switch.get(layer, layer)
        parts = [layer, zone, domain, table]

        return self.format_dotpath('.'.join(parts))

    def _parse_path_string(self, path_string: str):
        """
        Parse dot-separated path string into components.

        Args:
            path_string: Format 'layer.zone.source.domain.table_name'.

        Returns:
            Tuple of (layer, zone, domain, table_name).
        """

        missing = 3 - str(path_string).count('.')
        path_string += '.' * missing
        parts = str(path_string).split('.')

        return tuple(parts)

    def get_path(self, dotpath: str):
        """
        Convert dot-separated path string to filesystem path.

        Args:
            path_string: Dot-separated path string.

        Returns:
            Full filesystem path.
        """
        layer, zone, domain, table = self._parse_path_string(dotpath)
        layer = self.layer_switch.get(layer, layer)
        folder = self._construct_folder_path(layer, zone, domain, table)

        return folder

    def _construct_folder_path(self, layer, zone, domain, table):
        """
        Construct folder path for given components.

        Args:
            layer: Data lake layer.
            zone: Zone within layer.
            domain: Entity or database name.

        Returns:
            Full folder path.
        """
        return Path(self.reference, layer, zone, domain, table)

    def __eq__(self, other):
        if isinstance(other, DLDotPath):
            return (self.dotpath == other.dotpath) and (
                self.reference == other.reference
            )
        return False

    def __hash__(self):
        return hash((str(self.reference), self.dotpath))


class DataLakeConnector:
    """Connector for local data lake filesystem and google cloud storage."""

    def __init__(self, base_path: str):
        """
        Initialize connector with base data lake path.

        Args:
            base_path: Root path of the data lake.
        """
        self.base_path = base_path

    def _load_metadata(self, folder_path: str):
        """
        Load metadata configuration from metadata.ini.

        Args:
            folder_path: Path to domain folder containing metadata subfolder.

        Returns:
            ConfigParser with metadata values.
        """
        config = configparser.ConfigParser()
        meta_path = os.path.join(folder_path, '.metadata', 'metadata.ini')

        if not os.path.isfile(meta_path):
            raise FileNotFoundError(f'Metadata not found at {meta_path}')

        config.read(meta_path)

        return config['DEFAULT']

    def read_table(self, dotpath: str) -> pd.DataFrame:
        """
        Read table into DataFrame according to metadata.

        Args:
            dotpath: 'layer.zone.source.domain.table_name'.

        Returns:
            Pandas DataFrame.
        """

        dpath = DLDotPath(dotpath, reference=self.base_path, fail_if_missing_table=True)
        table = dpath.table
        meta = self._load_metadata(dpath.path)

        ext = meta.get('format')
        sep = meta.get('separator', ';')
        encoding = meta.get('encoding', 'utf-8')
        single = meta.get('current_file')

        if single:
            fp = Path(dpath.path, single)

        else:
            pattern = Path(dpath.path, f'*-{table}.{ext}')
            files = glob.glob(str(pattern))

            if not files:

                raise FileNotFoundError(f'No file for {table}')

            fp = max(files)

        if ext in ('csv', 'tsv', 'txt'):
            df = pd.read_csv(fp, sep=sep, encoding=encoding)

        # elif ext in ('xlsx', 'xls'):
        #     df = pd.read_excel(fp)

        # elif ext in ('parquet', 'pq'):
        #     df = pd.read_parquet(fp)

        else:
            raise ValueError(f'Unsupported format: {ext}')

        return df

    def write_table(self, path_string: str, df: pd.DataFrame):
        """
        Write DataFrame to data lake and update metadata if needed.

        Args:
            path_string: 'layer.zone.source.domain.table_name'.
            df: DataFrame to write.
        """
        dpath = DLDotPath(
            path_string, reference=self.base_path, fail_if_missing_table=True
        )
        folder = dpath.path
        meta_dir = Path(folder, '.metadata')

        os.makedirs(meta_dir, exist_ok=True)

        meta_path = Path(meta_dir, 'metadata.ini')
        config = configparser.ConfigParser()

        if meta_path.is_file():
            config.read(meta_path)

        else:
            config['DEFAULT'] = {}

        default = config['DEFAULT']
        fmt = default.get('format', 'csv')
        sep = default.get('separator', ';')
        encoding = default.get('encoding', 'utf-8')
        partitioned = default.getboolean('partitioned', False)
        date_str = datetime.today().strftime('%Y%m%d')

        filename = f'{date_str}-{dpath.table.replace("_", "-")}.{fmt}'

        if not partitioned:
            default['current_file'] = filename

        file_path = Path(folder, filename)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if fmt in ('csv', 'tsv', 'txt'):
            df.to_csv(file_path, sep=sep, encoding=encoding, index=False)

        # elif fmt in ('xlsx', 'xls'):
        #     df.to_excel(file_path, index=False)

        # elif fmt in ('parquet', 'pq'):
        #     df.to_parquet(file_path, index=False)

        else:
            raise ValueError(f'Unsupported format: {fmt}')

        default['format'] = fmt
        default['separator'] = sep
        default['encoding'] = encoding
        default['partitioned'] = str(partitioned).lower()

        with open(meta_path, 'w') as cfgfile:
            config.write(cfgfile)

    def list_tables(self, path_string: str = '') -> list:
        """
        List all tables in a given path, always returning full dot-paths (layer.zone.domain.table_name).

        Args:
            path_string: '', 'layer', 'layer.zone', or 'layer.zone.domain'.

        Returns:
            List of table dot-paths.
        """
        dpath = DLDotPath(path_string, reference=self.base_path)

        base_folder = dpath.path

        if not base_folder.is_dir():

            raise FileNotFoundError(f'No such directory: {base_folder}')

        all_files = []
        for root, dirs, files in os.walk(base_folder):
            for file in files:
                dpath = DLDotPath(Path(root, file), reference=self.base_path)
                all_files.append(dpath)

        tables = {dpath for dpath in all_files if dpath.table}

        return tables

    def read_multiple(self, path_strings: list) -> pd.DataFrame:
        """
        Read multiple tables.

        Args:
            path_strings: List of dot-separated path strings.

        Returns:
            dataframes dictionary
        """
        dataframes_dict = {}

        for path_string in path_strings:
            table = path_string.split('.')[-1]
            df = self.read_table(path_string)
            dataframes_dict[table] = df

        return dataframes_dict

    def list_files(self, path_string: str = '') -> list:
        """
        List all files in a given dot-path.
        Args:
            path_string: '', 'layer', 'layer.zone', 'layer.zone.domain' or 'layer.zone.domain.table_name'.

        Returns:
            List of file paths.
        """

        dpath = DLDotPath(path_string, reference=self.base_path)
        base_folder = dpath.path

        if not os.path.isdir(base_folder):
            raise FileNotFoundError(f'No such directory: {base_folder}')

        file_paths = {}

        for root, _, files in os.walk(base_folder):

            for file in files:
                path = os.path.join(root, file)
                tmp_dpath = DLDotPath(path, reference=self.base_path)

                table = tmp_dpath.dotpath
                if not table:
                    table = Path(path).parent.name

                if table in file_paths:
                    file_paths[table].append(path)
                else:
                    file_paths[table] = [path]

        return file_paths

    @staticmethod
    def _normalize_gcs_uri(raw: str) -> str:
        """
        Normalize various GCS-like inputs to canonical gs://bucket/prefix form.

        Supports:
        - gs://bucket/prefix
        - https://console.cloud.google.com/storage/browser/bucket/prefix
        - Strings accidentally prefixed with 'gs://' + console URL
        """
        s = raw.strip()

        # Strip accidental double prefix like "gs://https://console.cloud.google.com/..."
        if s.startswith('gs://https://'):
            s = s[len('gs://') :]  # remove the "gs://", keep the https part

        # If it's a console URL, extract bucket and prefix
        if 'console.cloud.google.com/storage/browser/' in s:
            # drop query/fragment or console-specific suffixes (;tab=..., ?..., #...)
            s = s.split(';', 1)[0]
            s = s.split('?', 1)[0]
            s = s.split('#', 1)[0]

            marker = 'console.cloud.google.com/storage/browser/'
            idx = s.find(marker)
            path = s[idx + len(marker) :].lstrip('/')  # bucket/prefix
            # path like "bucket/dir1/dir2"
            parts = path.split('/', 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ''
            return f'gs://{bucket}/{prefix}'.rstrip('/')

        # Already gs:// form
        if s.startswith('gs://'):
            return s.rstrip('/')

        # If it looks like "bucket/prefix" (no scheme), keep as-is is risky; enforce gs://
        if '://' not in s and s:
            # assume "bucket/prefix"
            first_slash = s.find('/')
            if first_slash == -1:
                return f'gs://{s}'
            bucket = s[:first_slash]
            prefix = s[first_slash + 1 :]
            return f'gs://{bucket}/{prefix}'.rstrip('/')

        raise ValueError(f'Unsupported GCS path format: {raw}')

    @staticmethod
    def _split_gs_uri(gs_uri: str) -> Tuple[str, str]:
        """
        Split gs://bucket/prefix into (bucket, prefix).
        """
        if not gs_uri.startswith('gs://'):
            raise ValueError(f'Not a gs:// URI: {gs_uri}')
        rest = gs_uri[len('gs://') :]
        parts = rest.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        return bucket, prefix

    @staticmethod
    def _try_parse_date_any(s: str) -> Optional[date]:
        """
        Try to parse a date string in several common formats.
        Returns date or None.

        Supported:
        - YYYYMMDD
        - YYYY-MM-DD
        - YYYYMM  (assumes day=01)
        - YYYY-MM (assumes day=01)
        """
        from datetime import datetime

        candidates = [
            ('%Y%m%d', r'\b(\d{8})\b'),
            ('%Y-%m-%d', r'\b(\d{4}-\d{2}-\d{2})\b'),
            ('%Y%m', r'\b(\d{6})\b'),
            ('%Y-%m', r'\b(\d{4}-\d{2})\b'),
        ]

        for fmt, pattern in candidates:
            m = re.search(pattern, s)
            if not m:
                continue
            token = m.group(1)
            try:
                dt = datetime.strptime(token, fmt)
                return date(dt.year, dt.month, getattr(dt, 'day', 1))
            except ValueError:
                continue
        return None


class GCSConnector:
    """
    Minimal GCS file lister with date-range filtering for PDFs.
    """

    def __init__(self, service_account_info: dict, bucket_name: str):
        self.client = storage.Client.from_service_account_info(service_account_info)
        self.bucket_name = bucket_name
        self.data_lake_connector = DataLakeConnector(base_path=f'gs://{bucket_name}')

    def list_pdfs_by_date(
        self,
        path: str,
        start: date,
        end: date,
        recursive: bool = True,
    ) -> List[str]:
        """
        List gs:// URIs of PDFs under a GCS prefix filtered by filename date within [start, end] inclusive.
        """
        if start > end:
            raise ValueError('start date must be <= end date')

        gs_uri = self.data_lake_connector._normalize_gcs_uri(path)
        bucket_name, prefix = self.data_lake_connector._split_gs_uri(gs_uri)

        bucket = self.client.bucket(bucket_name)

        delimiter = '/' if not recursive else None
        blobs_iter = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

        results = []
        for blob in blobs_iter:
            name = blob.name
            if not name.lower().endswith('.pdf'):
                continue

            base = name.rsplit('/', 1)[-1]
            d = self.data_lake_connector._try_parse_date_any(base)
            if d is None:
                continue

            if start <= d <= end:
                results.append((d, base.lower(), f'gs://{bucket_name}/{name}'))

        results.sort(key=lambda t: (t[0], t[1]))
        return [uri for _, __, uri in results]

    def list_files_by_date(
        self,
        path: str,
        start: date,
        end: date,
        extension: str = '.json',
        recursive: bool = True,
    ) -> List[str]:
        """
        Lista gs:// URIs bajo un prefijo GCS filtradas por:
        - extensión (case-insensitive),
        - fecha YYYYMMDD al inicio del filename, dentro de [start, end].
        """
        if start > end:
            raise ValueError('start date must be <= end date')

        gs_uri = self.data_lake_connector._normalize_gcs_uri(path)
        bucket_name, prefix = self.data_lake_connector._split_gs_uri(gs_uri)

        bucket = self.client.bucket(bucket_name)

        delimiter = '/' if not recursive else None
        blobs_iter = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

        ext_l = extension.lower()
        results = []

        for blob in blobs_iter:
            name = blob.name
            if not name.lower().endswith(ext_l):
                continue

            base = name.rsplit('/', 1)[-1]
            d = None
            if len(base) >= 8 and base[:8].isdigit():
                ymd = base[:8]
                try:
                    d = date(int(ymd[0:4]), int(ymd[4:6]), int(ymd[6:8]))
                except ValueError:
                    d = None

            if d is None:
                d = self.data_lake_connector._try_parse_date_any(base)

            if d is None:
                continue

            if start <= d <= end:
                results.append((d, base.lower(), f'gs://{bucket_name}/{name}'))

        results.sort(key=lambda t: (t[0], t[1]))
        return [uri for _, __, uri in results]
