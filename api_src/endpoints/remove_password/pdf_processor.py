import os
import tempfile
from pathlib import PurePosixPath

import fitz  # PyMuPDF

from api_src.data_lake.connector import DataLakeConnector, GCSConnector
from api_src.utils import BUCKET_NAME, PASSWORD_PDF, SERVICE_ACCOUNT_INFO


class PDFProcessor:
    def __init__(
        self,
        pdf_path: str,
        original_path: str = None,
        service_account_info: dict = SERVICE_ACCOUNT_INFO,
        bucket_name: str = BUCKET_NAME,
    ):
        """
        pdf_path puede ser:
        - Ruta local: 'cuentas_pdf/account_state_example.pdf'
        - Ruta GCS: 'gs://bucket/prefix/file.pdf'
        - URL de consola GCS
        """
        self.original_path = original_path or pdf_path
        self.service_account_info = service_account_info
        self.bucket_name = bucket_name
        self.pdf_path = self._resolve_path(pdf_path)

    def _resolve_path(self, path: str) -> str:
        """
        Si es GCS, descarga a un archivo temporal y devuelve esa ruta local.
        Si es local, devuelve la ruta tal cual.
        """
        if (
            path.startswith('gs://')
            or 'console.cloud.google.com/storage/browser/' in path
        ):
            # Normalizar y descargar desde GCS
            dlc = DataLakeConnector(base_path='')
            gs_uri = dlc._normalize_gcs_uri(path)
            bucket_name, prefix = dlc._split_gs_uri(gs_uri)

            gcs = GCSConnector(
                service_account_info=self.service_account_info,
                bucket_name=self.bucket_name,
            )
            bucket = gcs.client.bucket(bucket_name)
            blob = bucket.blob(prefix)

            if not blob.exists():
                raise FileNotFoundError(f'No existe el archivo en GCS: {gs_uri}')

            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.pdf')
            os.close(tmp_fd)  # cerramos el descriptor, fitz abrirá por ruta
            blob.download_to_filename(tmp_path)
            return tmp_path

        # Si es ruta local
        if not os.path.isfile(path):
            raise FileNotFoundError(f'No existe el archivo local: {path}')
        return path

    def is_encrypted(self) -> bool:
        with fitz.open(self.pdf_path) as doc:
            return doc.is_encrypted

    def remove_password_if_needed(
        self,
        output_path: str,
        upload_to_gcs_prefix: str = None,
        keep_local_copy: bool = True,
    ) -> str:
        import shutil

        with fitz.open(self.pdf_path) as doc:
            if not doc.is_encrypted:
                print('✅ PDF abierto sin necesidad de contraseña.')
                original_basename = os.path.basename(self.pdf_path)
                name_root, ext = os.path.splitext(original_basename)
                unlocked_name = f"{name_root}_unlocked{ext or '.pdf'}"
                target_path = os.path.join(output_path, unlocked_name)
                shutil.copy(self.pdf_path, target_path)
                print(f'📁 Copia local creada en: {target_path}')
            else:
                if not PASSWORD_PDF or not doc.authenticate(PASSWORD_PDF):
                    raise RuntimeError(
                        '❌ Contraseña incorrecta o no se pudo desbloquear el PDF.'
                    )

                if (
                    self.original_path.startswith('gs://')
                    or 'console.cloud.google.com/storage/browser/' in self.original_path
                ):
                    dlc = DataLakeConnector(base_path='')
                    gs_uri = dlc._normalize_gcs_uri(self.original_path)
                    _, original_object_path = dlc._split_gs_uri(gs_uri)
                    original_basename = os.path.basename(original_object_path)
                    name_root, ext = os.path.splitext(original_basename)
                    unlocked_name = f"{name_root}_unlocked{ext or '.pdf'}"

                    p = PurePosixPath(original_object_path)
                    original_basename = p.name
                    name_root, ext = os.path.splitext(original_basename)
                    unlocked_name = f"{name_root}_unlocked{ext or '.pdf'}"

                    rel_dir_posix = (
                        p.parent.as_posix()
                        .removeprefix('landing/')
                        .replace('/pdf/', '/pdf_unlocked/')
                    )
                    # Sanear output_path si accidentalmente incluye el nombre del archivo
                    if (
                        os.path.splitext(os.path.basename(output_path))[1].lower()
                        == '.pdf'
                    ):
                        output_path = os.path.dirname(output_path)

                    if not keep_local_copy:
                        target_path = None  # se definirá luego como temporal
                    else:
                        local_dir = os.path.join(output_path, *rel_dir_posix.split('/'))
                        os.makedirs(local_dir, exist_ok=True)
                        target_path = os.path.join(local_dir, unlocked_name)

                    print(f' original_object_path: {original_object_path}')
                    print(f' rel_dir_posix: {rel_dir_posix}')
                    if keep_local_copy:
                        print(f' local_dir: {local_dir}')
                        print(f' target_path: {target_path}')

                else:
                    original_basename = os.path.basename(self.pdf_path)
                    name_root, ext = os.path.splitext(original_basename)
                    unlocked_name = f"{name_root}_unlocked{ext or '.pdf'}"
                    target_path = os.path.join(output_path, unlocked_name)
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)

                if (
                    self.original_path.startswith('gs://')
                    or 'console.cloud.google.com/storage/browser/' in self.original_path
                ):
                    if not keep_local_copy:
                        # Guardar en archivo temporal
                        with tempfile.NamedTemporaryFile(
                            delete=False, suffix='_unlocked.pdf'
                        ) as tmp_file:
                            temp_path = tmp_file.name

                            if not keep_local_copy and (
                                self.original_path.startswith('gs://')
                                or 'console.cloud.google.com/storage/browser/'
                                in self.original_path
                            ):

                                with tempfile.NamedTemporaryFile(
                                    delete=False, suffix='_unlocked.pdf'
                                ) as tmp_file:
                                    temp_path = tmp_file.name
                                doc.save(temp_path, encryption=fitz.PDF_ENCRYPT_NONE)
                                target_path = temp_path
                                print(
                                    f'🔓 PDF desencriptado temporalmente en: {target_path}'
                                )
                            else:
                                doc.save(target_path, encryption=fitz.PDF_ENCRYPT_NONE)
                                print(
                                    f'🔓 PDF desencriptado y guardado localmente como: {target_path}'
                                )
                    else:
                        doc.save(target_path, encryption=fitz.PDF_ENCRYPT_NONE)
                        print(
                            f'🔓 PDF desencriptado y guardado localmente como: {target_path}'
                        )

        self.pdf_path = target_path
        self.last_uploaded_gs_uri = None

        # Subida a GCS
        if upload_to_gcs_prefix:
            if (
                self.original_path.startswith('gs://')
                or 'console.cloud.google.com/storage/browser/' in self.original_path
            ):
                # -- Lógica de Carga a GCS (Opción A: Carpeta por Año) --
                dlc = DataLakeConnector(base_path='')
                src_gs_uri = dlc._normalize_gcs_uri(self.original_path)
                bucket_name, original_object_path = dlc._split_gs_uri(src_gs_uri)

                # Extraer el año del nombre del archivo original
                date_obj = dlc._try_parse_date_any(original_object_path)
                year_folder = str(date_obj.year) if date_obj else 'unclassified'

                # Normalizar prefijo de destino
                if upload_to_gcs_prefix.startswith('gs://'):
                    _, dest_prefix = dlc._split_gs_uri(upload_to_gcs_prefix)
                else:
                    dest_prefix = upload_to_gcs_prefix.lstrip('/')

                # Construir el nombre del archivo de destino (sin prefijo de año)
                name_root, ext = os.path.splitext(
                    os.path.basename(original_object_path)
                )
                dest_filename = f"{name_root}_unlocked{ext or '.pdf'}"

                # Construir la ruta completa del blob incluyendo la carpeta del año
                dest_blob_path = (
                    f"{dest_prefix.rstrip('/')}/{year_folder}/{dest_filename}"
                )

                # Subir el archivo
                gcs = GCSConnector(
                    service_account_info=self.service_account_info,
                    bucket_name=self.bucket_name,
                )
                bucket = gcs.client.bucket(self.bucket_name)
                blob = bucket.blob(dest_blob_path)
                blob.upload_from_filename(target_path)

                self.last_uploaded_gs_uri = f'gs://{self.bucket_name}/{dest_blob_path}'
                print(f'☁️  Subido a {self.last_uploaded_gs_uri}')

                if not keep_local_copy:
                    try:
                        os.remove(target_path)
                        print('🧹 Copia local eliminada tras la subida a GCS.')
                    except OSError:
                        print('⚠️ No se pudo eliminar la copia local.')
            else:
                print(
                    "⚠️ Se solicitó 'upload_to_gcs_prefix', pero el origen no es GCS. Se omite la subida."
                )

        assert (
            self.pdf_path is not None
        ), '❌ No se pudo determinar la ruta del PDF procesado'
        return self.pdf_path

    def read_content(self) -> str:
        contenido = ''
        try:
            with fitz.open(self.pdf_path) as doc:
                for num_pagina, pagina in enumerate(doc, start=1):
                    texto = pagina.get_text().strip()
                    if texto:
                        contenido += f'\n--- Página {num_pagina} ---\n{texto}\n'

            texto_limpio = contenido.strip()
            if not texto_limpio:
                raise ValueError('⚠️ El PDF no contiene texto legible.')

            return texto_limpio

        except Exception as e:
            raise RuntimeError(f'Ocurrió un error al procesar el PDF: {e}')
