from typing import Dict, List

COLUMN_MAP = {
    # Cabecera (ok)
    'cardholder_name': ['estado_de_cuenta.nombre_del_titular', 'nombre_del_titular'],
    'card_number_masked': ['estado_de_cuenta.numero_de_tarjeta', 'numero_de_tarjeta'],
    'statement_date': ['estado_de_cuenta.fecha', 'fecha'],
    # Período de facturación (ok en periodo_facturado, corrige due_date)
    'billing_period_start': [
        'estado_de_cuenta.informacion_general.periodo_facturado.desde',
        'informacion_general.periodo_facturado.desde',
    ],
    'billing_period_end': [
        'estado_de_cuenta.informacion_general.periodo_facturado.hasta',
        'informacion_general.periodo_facturado.hasta',
    ],
    # FIX: pagar_hasta está dentro de informacion_general
    'due_date': [
        'estado_de_cuenta.informacion_general.pagar_hasta',
        'informacion_general.pagar_hasta',
    ],
    # Dirección (ok)
    'address_line': ['estado_de_cuenta.direccion', 'direccion'],
    'commune': ['estado_de_cuenta.comuna', 'comuna'],
    # Cupos (FIX: todos viven bajo informacion_general)
    'total_credit_limit': [
        'estado_de_cuenta.informacion_general.cupo_total',
        'informacion_general.cupo_total',
    ],
    'used_credit': [
        'estado_de_cuenta.informacion_general.cupo_utilizado',
        'informacion_general.cupo_utilizado',
    ],
    'available_credit': [
        'estado_de_cuenta.informacion_general.cupo_disponible',
        'informacion_general.cupo_disponible',
    ],
    # Cupos de avance en efectivo (FIX: bajo informacion_general)
    'cash_advance_limit': [
        'estado_de_cuenta.informacion_general.cupo_total_avance_en_efectivo',
        'informacion_general.cupo_total_avance_en_efectivo',
    ],
    'cash_advance_used': [
        'estado_de_cuenta.informacion_general.cupo_utilizado_avance_en_efectivo',
        'informacion_general.cupo_utilizado_avance_en_efectivo',
    ],
    'cash_advance_available': [
        'estado_de_cuenta.informacion_general.cupo_disponible_avance_en_efectivo',
        'informacion_general.cupo_disponible_avance_en_efectivo',
    ],
    # Tasas (FIX: bajo informacion_general.tasas)
    'rate_rotating_interest_pct': [
        'estado_de_cuenta.informacion_general.tasas.rotativo.interes',
        'informacion_general.tasas.rotativo.interes',
    ],
    'rate_rotating_cae_pct': [
        'estado_de_cuenta.informacion_general.tasas.rotativo.cae',
        'informacion_general.tasas.rotativo.cae',
    ],
    'rate_installment_interest_pct': [
        'estado_de_cuenta.informacion_general.tasas.compra_en_cuotas.interes',
        'informacion_general.tasas.compra_en_cuotas.interes',
    ],
    'rate_installment_cae_pct': [
        'estado_de_cuenta.informacion_general.tasas.compra_en_cuotas.cae',
        'informacion_general.tasas.compra_en_cuotas.cae',
    ],
    'rate_cash_advance_interest_pct': [
        'estado_de_cuenta.informacion_general.tasas.avance_en_cuotas.interes',
        'informacion_general.tasas.avance_en_cuotas.interes',
    ],
    'rate_cash_advance_cae_pct': [
        'estado_de_cuenta.informacion_general.tasas.avance_en_cuotas.cae',
        'informacion_general.tasas.avance_en_cuotas.cae',
    ],
    'rate_prepayment_cae_pct': [
        'estado_de_cuenta.informacion_general.tasas.prepago.cae',
        'informacion_general.tasas.prepago.cae',
    ],
    # Período anterior (OK en rutas, pero FIX de nombre de campo)
    'prev_period_start': [
        'estado_de_cuenta.detalle.periodo_anterior.inicio',
        'detalle.periodo_anterior.inicio',
    ],
    'prev_period_end': [
        'estado_de_cuenta.detalle.periodo_anterior.fin',
        'detalle.periodo_anterior.fin',
    ],
    'prev_opening_balance': [
        'estado_de_cuenta.detalle.periodo_anterior.saldo_inicio',
        'detalle.periodo_anterior.saldo_inicio',
    ],
    # FIX: el esquema usa "monto_facturado_A"
    'prev_billed_amount_A': [
        'estado_de_cuenta.detalle.periodo_anterior.monto_facturado_A',
        'detalle.periodo_anterior.monto_facturado_A',
    ],
    'prev_paid_amount': [
        'estado_de_cuenta.detalle.periodo_anterior.monto_pagado',
        'detalle.periodo_anterior.monto_pagado',
    ],
    'prev_closing_balance': [
        'estado_de_cuenta.detalle.periodo_anterior.saldo_final',
        'detalle.periodo_anterior.saldo_final',
    ],
    # Período actual (FIX: bajo detalle.periodo_actual)
    'current_ops_total': [
        'estado_de_cuenta.detalle.periodo_actual.operaciones.total',
        'detalle.periodo_actual.operaciones.total',
    ],
    # Información de pago (OK, está al nivel superior dentro de estado_de_cuenta)
    'billed_amount': [
        'estado_de_cuenta.informacion_de_pago.monto_facturado',
        'informacion_de_pago.monto_facturado',
    ],
    'minimum_payment': [
        'estado_de_cuenta.informacion_de_pago.monto_minimo',
        'informacion_de_pago.monto_minimo',
    ],
    'prepayment_cost': [
        'estado_de_cuenta.informacion_de_pago.costo_prepago',
        'informacion_de_pago.costo_prepago',
    ],
    'autodebit_amount': [
        'estado_de_cuenta.informacion_de_pago.cargo_automatico',
        'informacion_de_pago.cargo_automatico',
    ],
    'next_billing_start': [
        'estado_de_cuenta.informacion_de_pago.proximo_periodo_facturacion.desde',
        'informacion_de_pago.proximo_periodo_facturacion.desde',
    ],
    'next_billing_end': [
        'estado_de_cuenta.informacion_de_pago.proximo_periodo_facturacion.hasta',
        'informacion_de_pago.proximo_periodo_facturacion.hasta',
    ],
    # Costos por atraso (OK)
    'late_interest_rate_pct': [
        'estado_de_cuenta.costos_por_atraso.interes_moratorio',
        'costos_por_atraso.interes_moratorio',
    ],
    'collection_fee_to_10_uf_pct': [
        'estado_de_cuenta.costos_por_atraso.cargo_de_cobranza.hasta_10_uf',
        'costos_por_atraso.cargo_de_cobranza.hasta_10_uf',
    ],
    'collection_fee_10_to_50_uf_pct': [
        'estado_de_cuenta.costos_por_atraso.cargo_de_cobranza.entre_10_y_50_uf',
        'costos_por_atraso.cargo_de_cobranza.entre_10_y_50_uf',
    ],
    'collection_fee_over_50_uf_pct': [
        'estado_de_cuenta.costos_por_atraso.cargo_de_cobranza.excedan_50_uf',
        'costos_por_atraso.cargo_de_cobranza.excedan_50_uf',
    ],
    # Moneda
    'currency_code': [],  # default CLP
}


CANONICAL_ORDER: List[str] = [
    'statement_id',
    'data_source_hash',
    'created_at',
    'cardholder_name',
    'card_number_masked',
    'statement_date',
    'billing_period_start',
    'billing_period_end',
    'due_date',
    'address_line',
    'commune',
    'total_credit_limit',
    'used_credit',
    'available_credit',
    'cash_advance_limit',
    'cash_advance_used',
    'cash_advance_available',
    'rate_rotating_interest_pct',
    'rate_rotating_cae_pct',
    'rate_installment_interest_pct',
    'rate_installment_cae_pct',
    'rate_cash_advance_interest_pct',
    'rate_cash_advance_cae_pct',
    'rate_prepayment_cae_pct',
    'prev_period_start',
    'prev_period_end',
    'prev_opening_balance',
    'prev_billed_amount_A',
    'prev_paid_amount',
    'prev_closing_balance',
    'current_ops_total',
    'billed_amount',
    'minimum_payment',
    'prepayment_cost',
    'autodebit_amount',
    'next_billing_start',
    'next_billing_end',
    'late_interest_rate_pct',
    'collection_fee_to_10_uf_pct',
    'collection_fee_10_to_50_uf_pct',
    'collection_fee_over_50_uf_pct',
    'currency_code',
]


CANONICAL_TYPES = {
    # strings
    'cardholder_name': 'str',
    'card_number_masked': 'card_mask',
    'address_line': 'str',
    'commune': 'str',
    'currency_code': 'str',
    # fechas
    'statement_date': 'date',
    'billing_period_start': 'date',
    'billing_period_end': 'date',
    'due_date': 'date',
    'prev_period_start': 'date',
    'prev_period_end': 'date',
    'next_billing_start': 'date',
    'next_billing_end': 'date',
    # montos CLP (float)
    'total_credit_limit': 'money',
    'used_credit': 'money',
    'available_credit': 'money',
    'cash_advance_limit': 'money',
    'cash_advance_used': 'money',
    'cash_advance_available': 'money',
    'prev_opening_balance': 'money',
    'prev_billed_amount_A': 'money',
    'prev_paid_amount': 'money',
    'prev_closing_balance': 'money',
    'current_ops_total': 'money',
    'billed_amount': 'money',
    'minimum_payment': 'money',
    'prepayment_cost': 'money',
    'autodebit_amount': 'money',
    # porcentajes
    'rate_rotating_interest_pct': 'pct',
    'rate_rotating_cae_pct': 'pct',
    'rate_installment_interest_pct': 'pct',
    'rate_installment_cae_pct': 'pct',
    'rate_cash_advance_interest_pct': 'pct',
    'rate_cash_advance_cae_pct': 'pct',
    'rate_prepayment_cae_pct': 'pct',
}


DEBUG_FIELDS_STATEMENTS = [
    'due_date',
    'total_credit_limit',
    'used_credit',
    'available_credit',
    'cash_advance_limit',
    'cash_advance_used',
    'cash_advance_available',
    'rate_rotating_interest_pct',
    'rate_rotating_cae_pct',
    'rate_installment_interest_pct',
    'rate_installment_cae_pct',
    'rate_cash_advance_interest_pct',
    'rate_cash_advance_cae_pct',
    'current_ops_total',
]

COLUMN_MAP_ITEMS = {
    'item_id': [],
    'statement_id': [],
    'data_source_hash': [],
    'created_at': [],
    'ingestion_ts': [],
    'raw_bucket_path': [],
    'category': ['category', 'titulo'],
    'line_order': [],
    'transaction_date': ['fecha'],
    'transaction_place': ['lugar_operacion', 'transaction_place', 'comercio'],
    'description': ['descripcion'],
    'code': ['codigo'],
    'reference': ['referencia'],
    'operation_amount': ['monto_operacion'],
    'total_amount_due': ['monto_total_a_pagar'],
    'installment_number': ['numero_cuota'],
    'currency_code': ['moneda'],
    'monthly_installment_value': ['valor_cuota_mensual'],
    'sign': [],
}

CANONICAL_ORDER_ITEMS = [
    'item_id',
    'statement_id',
    'data_source_hash',
    'created_at',
    'ingestion_ts',
    'raw_bucket_path',
    'category',
    'line_order',
    'transaction_date',
    'transaction_place',
    'code',
    'reference',
    'description',
    'operation_amount',
    'total_amount_due',
    'installment_number',
    'monthly_installment_value',
    'currency_code',
    'sign',
]

CANONICAL_TYPES_ITEMS = {
    'transaction_place': 'str',
    'transaction_date': 'date',
    'code': 'str',
    'reference': 'str',
    'description': 'str',
    'operation_amount': 'float',
    'total_amount_due': 'float',
    'installment_number': 'str',
    'monthly_installment_value': 'float',
    'currency_code': 'str',
    'sign': 'int',
    'line_order': 'int',
    'category': 'str',
}


COLUMN_MAP_UPCOMING_DUES: Dict[str, List[str]] = {
    'due_id': [],
    'statement_id': [],
    'data_source_hash': [],
    'created_at': [],
    'ingestion_ts': [],
    'raw_bucket_path': [],
    'month_label': ['mes'],
    'due_amount': ['monto'],
    # 'month_date': [],
    'currency_code': [],
    'line_order': [],
}

CANONICAL_ORDER_UPCOMING_DUES = [
    'due_id',
    'statement_id',
    'data_source_hash',
    'created_at',
    'ingestion_ts',
    'raw_bucket_path',
    'month_label',
    'due_amount',
    #  'month_date',
    'currency_code',
    'line_order',
]

CANONICAL_TYPES_UPCOMING_DUES = {
    'month_label': 'str',
    'due_amount': 'float',
    #  'month_date': 'passthrough',
    'currency_code': 'str',
    'line_order': 'int',
}
