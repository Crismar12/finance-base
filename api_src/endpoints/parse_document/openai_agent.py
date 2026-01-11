import copy
import json
import os
import re
import unicodedata
from typing import Optional

from dotenv import load_dotenv
from jsonschema import ValidationError, validate
from openai import OpenAI

load_dotenv()
api_key = os.getenv('OPENAI_API_KEY')


class OpenAIAssistant:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)
        current_dir = os.path.dirname(__file__)
        schema_path = os.path.join(current_dir, 'json_schema.json')

        try:
            with open(schema_path, 'r', encoding='utf-8') as schema_file:
                self.schema = json.load(schema_file)
        except FileNotFoundError:
            raise RuntimeError(
                f'No se encontró el archivo de esquema en: {schema_path}'
            )
        except json.JSONDecodeError:
            raise RuntimeError('El esquema JSON está mal formado.')

    def _allow_additional_props(self, schema_node):
        """Agrega additionalProperties=True recursivamente en el esquema."""
        if isinstance(schema_node, dict):
            schema_node['additionalProperties'] = True
            for key in schema_node.get('properties', {}).values():
                self._allow_additional_props(key)
            if 'items' in schema_node:
                self._allow_additional_props(schema_node['items'])
        elif isinstance(schema_node, list):
            for item in schema_node:
                self._allow_additional_props(item)

    def extract_data(self, raw_text: str) -> dict:
        schema_str = json.dumps(self.schema, indent=2)
        prompt = (
            'You are a financial document parser.\n\n'
            'Output rules:\n'
            '1) Return ONLY a valid JSON object (no markdown, no comments).\n'
            '2) All required keys from the schema MUST be present with correct types.\n'
            '3) Map known sections to the schema as usual:\n'
            '   - detalle.periodo_actual.operaciones.pagos_a_la_cuenta.items\n'
            '   - detalle.periodo_actual.operaciones.compras_en_cuotas.items\n'
            '   - detalle.periodo_actual.voluntariamente_contratados_sin_movimientos.items\n'
            '   - detalle.periodo_actual.cargos_comisiones_impuestos_abonos.items\n'
            '4) If the PDF contains EXTRA sections or items that do not fit schema keys,\n'
            '   INCLUDE them under detalle.periodo_actual.extras_periodo_actual using exact '
            'original headings and labels.\n'
            '   Structure for each extra section:\n'
            '   {\n'
            '     "titulo": "<heading as in PDF>",\n'
            '     "total": <number or null>,\n'
            '     "items": [\n'
            '       {\n'
            '         "linea_original": "<line as in PDF>",\n'
            '         "campos": { "<label as-is>": <value>, ... }\n'
            '       }\n'
            '     ]\n'
            '   }\n'
            '5) Maintain the EXACT order of items as in the PDF for all sections, including extras.\n'
            '6) Do NOT translate, rename, or alter any labels from the original document.\n'
            '7) Monetary amounts:\n'
            '   - MUST be JSON numbers (no quotes).\n'
            '   - Remove any currency symbols, spaces, and thousands separators.\n'
            '   - Keep decimals if present.\n'
            "   - Do NOT convert percentages; percentages must remain strings with the '%' symbol.\n"
            '   - Apply these rules to all monetary fields, whether in schema-defined keys or extras.\n'
            '8) Never omit extra data; if not mappable to schema keys, put it in extras_periodo_actual.\n\n'
            f'Text to analyze:\n{raw_text}\n\n'
            f'JSON schema (required fields reference):\n{schema_str}\n'
        )

        response = self.client.chat.completions.create(
            model='gpt-4-1106-preview',
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.0,
            response_format={'type': 'json_object'},
        )

        raw_content = response.choices[0].message.content.strip()
        json_match = re.search(r'\{.*\}', raw_content, re.DOTALL)

        if not json_match:
            self._save_debug_output(
                raw_content, 'No se pudo encontrar un bloque JSON válido.'
            )
            raise ValueError('No se pudo extraer JSON')

        parsed_json = json.loads(json_match.group())

        # Permitir propiedades adicionales en memoria (sin tocar el schema en disco)
        schema_clone = copy.deepcopy(self.schema)
        self._allow_additional_props(schema_clone)

        # Normaliza montos monetarios en extras a números
        self._normalize_extras_items(parsed_json)

        try:
            validate(instance=parsed_json, schema=schema_clone)
            return parsed_json
        except ValidationError as ve:
            print('JSON inválido, intentando corregir…')
            corrected = self._attempt_correction(parsed_json)
            if corrected:
                # Reaplicar normalización tras corrección
                self._normalize_extras_items(corrected)
                try:
                    validate(instance=corrected, schema=schema_clone)
                    return corrected
                except ValidationError as ve2:
                    self._save_debug_output(corrected, ve2.message)
                    raise ValueError('La corrección automática falló.')
            else:
                self._save_debug_output(parsed_json, ve.message)
                raise ValueError('JSON inválido y no se pudo corregir.')

    def _normalize_extras_items(self, data: dict) -> None:
        ITEM_SCHEMA_KEYS = [
            'lugar_operacion',
            'fecha',
            'codigo',
            'referencia',
            'descripcion',
            'monto_operacion',
            'monto_total_a_pagar',
            'numero_cuota',
            'valor_cuota_mensual',
        ]
        """
        Estructura extras_periodo_actual.items para que usen las claves del esquema y
        conviertan montos a number. Quita 'linea_original' y 'campos' tras mapear.
        """

        def strip_accents(s: str) -> str:
            return ''.join(
                c
                for c in unicodedata.normalize('NFD', s)
                if unicodedata.category(c) != 'Mn'
            )

        def norm_label(label: str) -> str:
            up = strip_accents(label).upper().strip()
            up = re.sub(r'\s+', ' ', up)
            return up

        # Aliases de labels -> claves del esquema
        LABEL_MAP = {
            'MONTO': 'monto_operacion',
            'MONTO OPERACION': 'monto_operacion',
            'MONTO OPERACIÓN': 'monto_operacion',
            'TOTAL A PAGAR': 'monto_total_a_pagar',
            'MONTO TOTAL A PAGAR': 'monto_total_a_pagar',
            'VALOR CUOTA MENSUAL': 'valor_cuota_mensual',
            'VALOR DE CUOTA MENSUAL': 'valor_cuota_mensual',
            'N° CUOTA': 'numero_cuota',
            'Nº CUOTA': 'numero_cuota',
            'NUMERO CUOTA': 'numero_cuota',
            'NÚMERO CUOTA': 'numero_cuota',
        }

        extras = (
            data.get('estado_de_cuenta', {})
            .get('detalle', {})
            .get('periodo_actual', {})
            .get('extras_periodo_actual', [])
        )
        if not isinstance(extras, list):
            return

        for section in extras:
            items = section.get('items', [])
            if not isinstance(items, list):
                continue

            new_items = []
            for it in items:
                # Base con todas las claves del esquema (None por defecto)
                mapped = {k: None for k in ITEM_SCHEMA_KEYS}

                # 1) Parse básico desde linea_original (si existe)
                raw = it.get('linea_original')
                if isinstance(raw, str):
                    raw = raw.strip()
                    m = re.match(
                        r'^(?P<lugar>[A-ZÑÁÉÍÓÚÜ\s]+)\s'
                        r'(?P<fecha>\d{2}/\d{2}/\d{2})\s'
                        r'(?P<codigo>\d+)\s'
                        r'(?P<referencia>\d+)\s'
                        r'(?P<descripcion>.+)$',
                        raw,
                    )
                    if m:
                        mapped['lugar_operacion'] = m.group('lugar').strip()
                        mapped['fecha'] = m.group('fecha')
                        mapped['codigo'] = m.group('codigo')
                        mapped['referencia'] = m.group('referencia')
                        mapped['descripcion'] = re.sub(
                            r'\s+', ' ', m.group('descripcion').strip()
                        )
                    else:
                        mapped['descripcion'] = raw

                # 2) Mapear desde 'campos' a las claves del esquema
                campos = it.get('campos', {})
                if isinstance(campos, dict):
                    for k, v in campos.items():
                        key_norm = norm_label(str(k))
                        schema_key = LABEL_MAP.get(key_norm)
                        if not schema_key:
                            continue
                        # numero_cuota es string; los demás son montos
                        if schema_key == 'numero_cuota':
                            mapped['numero_cuota'] = (
                                str(v).strip() if v is not None else None
                            )
                        else:
                            num = None
                            if isinstance(v, (int, float)):
                                num = v
                            elif isinstance(v, str):
                                num = self._parse_money(v)
                            if isinstance(num, (int, float)):
                                mapped[schema_key] = num

                # 3) Usar ítem ya estructurado (si venía así) como respaldo
                for key in ITEM_SCHEMA_KEYS:
                    if mapped[key] is None and key in it:
                        mapped[key] = it.get(key)

                # Añadir ítem normalizado (sin linea_original/campos)
                new_items.append(mapped)

            section['items'] = new_items

    def _attempt_correction(self, json_data: dict) -> Optional[dict]:
        corrected = copy.deepcopy(json_data)

        def fill_defaults(schema_node, target_node):
            if not isinstance(target_node, dict):
                return
            schema_type = schema_node.get('type')
            if isinstance(schema_type, list):
                schema_type = schema_type[0]
            if schema_type == 'object':
                props = schema_node.get('properties', {})
                required = schema_node.get('required', [])
                for key in required:
                    if key not in target_node or target_node[key] == {}:
                        val = props.get(key, {})
                        default_map = {
                            'string': '',
                            'number': 0,
                            'array': [],
                            'object': {},
                        }
                        target_node[key] = default_map.get(
                            val.get('type', 'string'), None
                        )
                    if isinstance(target_node[key], dict):
                        fill_defaults(props.get(key, {}), target_node[key])
                for key in props:
                    if key not in target_node:
                        val = props[key]
                        default_map = {
                            'string': '',
                            'number': 0,
                            'array': [],
                            'object': {},
                        }
                        target_node[key] = default_map.get(
                            val.get('type', 'string'), None
                        )
                    elif isinstance(target_node[key], dict):
                        fill_defaults(props[key], target_node[key])

        try:
            fill_defaults(self.schema, corrected)
            return corrected
        except Exception as e:
            print('Error en corrección:', e)
            return None

    def _save_debug_output(self, data, error_message: str):
        import datetime

        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'debug_invalid_output_{timestamp}.json'
        output_dir = 'debug_outputs'
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)
        debug_data = {'error': error_message, 'raw_output': data}
        with open(output_path, 'w', encoding='utf-8') as debug_file:
            json.dump(debug_data, debug_file, indent=2, ensure_ascii=False)
