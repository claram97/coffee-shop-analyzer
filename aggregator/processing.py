from collections import defaultdict
from datetime import datetime
import logging

from protocol.entities import RawTransaction, RawTransactionItem, RawUser

# Constants
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
VALID_YEARS = [2024, 2025]
MIN_HOUR = 6  # 06:00
MAX_HOUR = 23  # 23:00
FIRST_SEMESTER_MONTHS = 6  # Months 1-6 = Semester 1
FIRST_SEMESTER = 1
SECOND_SEMESTER = 2

def process_query_2(transaction_items: list[RawTransactionItem]):
    # Agregar year(created_at), month(created_at), item_id con agregaciones

    # Diccionario para agrupar: (year, month, item_id) -> {total_quantity, total_revenue}
    aggregated_data = defaultdict(lambda: {'total_quantity': 0.0, 'total_revenue': 0.0})

    for item in transaction_items:
        # Parsear fecha para extraer año y mes
        created_at = datetime.strptime(item.created_at, DATETIME_FORMAT)
        year = created_at.year
        month = created_at.month
        item_id = item.item_id
        
        # Crear clave de agrupación
        key = (year, month, item_id)
        
        # Sumar quantity y subtotal
        aggregated_data[key]['total_quantity'] += float(item.quantity)
        aggregated_data[key]['total_revenue'] += float(item.subtotal)

    # Log de los resultados agregados
    logging.debug('Query 2 Results - Aggregated by year, month, item_id:')
    logging.debug('Year | Month | Item_ID | Total_Quantity | Total_Revenue')
    logging.debug('-' * 60)

    for (year, month, item_id), totals in aggregated_data.items():
        logging.debug(f'{year:4d} | {month:5d} | {item_id:7s} | {totals["total_quantity"]:13.1f} | {totals["total_revenue"]:12.2f}')

    logging.debug(f'Total groups: {len(aggregated_data)}')
    logging.debug('')

def process_query_3(transactions: list[RawTransaction]):
    # Query 3: Agregar por (store_id, year, semester) con condiciones específicas
    aggregated_data = defaultdict(lambda: {
        'transaction_count': 0,
        'total_original_amount': 0.0,
        'total_discount_applied': 0.0,
        'total_final_amount': 0.0
    })

    filtered_count = 0
    total_count = len(transactions)

    for transaction in transactions:
        # Parsear fecha y hora
        created_at = datetime.strptime(transaction.created_at, DATETIME_FORMAT)
        year = created_at.year
        month = created_at.month
        hour = created_at.hour
        
        # Aplicar condiciones:
        # 1. Año ∈ {2024, 2025}
        if year not in VALID_YEARS:
            continue
            
        # 2. Hora entre 06:00 y 23:00
        if hour < MIN_HOUR or hour > MAX_HOUR:
            continue
        
        filtered_count += 1
        
        # Calcular semestre: 1-6 = S1, 7-12 = S2
        semester = FIRST_SEMESTER if month <= FIRST_SEMESTER_MONTHS else SECOND_SEMESTER
        
        # Crear clave de agrupación
        key = (transaction.store_id, year, semester)
        
        # Agregar métricas
        aggregated_data[key]['transaction_count'] += 1
        aggregated_data[key]['total_original_amount'] += float(transaction.original_amount)
        aggregated_data[key]['total_discount_applied'] += float(transaction.discount_applied)
        aggregated_data[key]['total_final_amount'] += float(transaction.final_amount)

    # Log de los resultados agregados
    logging.debug('Query 3 Results - Aggregated by store_id, year, semester:')
    valid_years_str = f"{{{VALID_YEARS[0]},{VALID_YEARS[1]}}}"
    logging.debug(f'Filtered {filtered_count}/{total_count} transactions (year ∈ {valid_years_str}, hour ∈ [{MIN_HOUR:02d}:00-{MAX_HOUR:02d}:00])')
    logging.debug('Store_ID | Year | Semester | Count | Original_Amount | Discounts | Final_Amount')
    logging.debug('-' * 80)

    for (store_id, year, semester), totals in aggregated_data.items():
        logging.debug(f'{store_id:8s} | {year:4d} | {semester:8d} | {totals["transaction_count"]:5d} | '
                    f'{totals["total_original_amount"]:14.2f} | {totals["total_discount_applied"]:9.2f} | '
                    f'{totals["total_final_amount"]:11.2f}')

    logging.debug(f'Total groups: {len(aggregated_data)}')
    logging.debug('')


def process_query_4_transactions(transactions: list[RawTransaction]):
    """Process transactions for Query 4 - TABLE 1: Transaction counts by store and user."""
    logging.info('Processing query 4 - Transactions')

    # Filtrar transacciones por año 2024-2025 y agrupar por (store_id, user_id)
    transaction_counts = defaultdict(int)
    filtered_count = 0
    total_count = len(transactions)

    for transaction in transactions:
        # Parsear fecha
        created_at = datetime.strptime(transaction.created_at, DATETIME_FORMAT)
        year = created_at.year
        
        # Filtrar por años válidos
        if year not in VALID_YEARS:
            continue
            
        filtered_count += 1
        
        # Agrupar por (store_id, user_id) y contar transacciones
        key = (transaction.store_id, transaction.user_id)
        transaction_counts[key] += 1

    # Log de resultados - TABLA 1: Transacciones agregadas
    logging.debug(f'Query 4 - TABLE 1: Transaction counts by store and user ({VALID_YEARS[0]}-{VALID_YEARS[1]}):')
    valid_years_str = f"{{{VALID_YEARS[0]},{VALID_YEARS[1]}}}"
    logging.debug(f'Filtered {filtered_count}/{total_count} transactions (year ∈ {valid_years_str})')
    logging.debug('Store_ID | User_ID | Total_Purchases')
    logging.debug('-' * 40)

    for (store_id, user_id), total_purchases in transaction_counts.items():
        logging.debug(f'{store_id:8s} | {user_id:7s} | {total_purchases:15d}')

    logging.debug(f'Total customer-store combinations: {len(transaction_counts)}')
    logging.info('')


def process_query_4_users(users: list[RawUser]):
    """Process users for Query 4 - TABLE 2: Users data."""
    logging.info('Processing query 4 - Users')

    # Log de resultados - TABLA 2: Usuarios
    logging.debug('Query 4 - TABLE 2: Users data:')
    logging.debug('User_ID | Birthdate')
    logging.debug('-' * 25)

    for user in users:
        logging.debug(f'{user.user_id:7s} | {user.birthdate}')

    logging.debug(f'Total users: {len(users)}')
    logging.info('')