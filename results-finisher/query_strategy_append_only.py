"""
Query strategy implementations using the 'Append-Only' model.

This model prioritizes low lock contention and high ingestion throughput by
performing minimal work during the consolidation step (appending rows) and
concentrating all business logic in the finalization step.

Its primary trade-off is higher memory usage.
"""

import datetime
from collections import defaultdict
from typing import Dict, Any, List, Callable

from constants import QueryType

def _group_by(data: List[Dict[str, Any]], key_func: Callable) -> Dict[Any, List[Dict[str, Any]]]:
    grouped = defaultdict(list)
    for item in data:
        key = key_func(item)
        grouped[key].append(item)
    return grouped

# --- Base Strategy ---
class BaseQueryStrategy:
    """
    Abstract base class for query-specific data consolidation and finalization logic.
    """
    def consolidate(self, state_data: Dict[str, Any], table_type: str, new_rows: List[Any]):
        """
        Default consolidation behavior: stores all raw rows, categorized by their table type.
        """
        state_data.setdefault(table_type, []).extend(new_rows)

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("Each query strategy must implement the 'finalize' method.")
    
    def _safe_extract_date(self, row: Any, date_field: str = 'created_at') -> datetime.datetime:
        date_str = getattr(row, date_field, None)
        if not date_str:
            raise ValueError(f"Date field '{date_field}' is missing.")
        return datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    
    def _safe_extract_numeric(self, row: Any, field: str, default: float = 0.0) -> float:
        try:
            return float(getattr(row, field, default))
        except (ValueError, TypeError):
            return default

# --- Strategy Implementations ---
class Q1Strategy(BaseQueryStrategy):
    """Strategy for Q1: Filters transactions based on amount and time."""
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        transactions = consolidated_data.get('Transactions', [])
        filtered_transactions = []
        for row in transactions:
            try:
                date = self._safe_extract_date(row)
                if not (6 <= date.hour < 12):
                    continue
                
                final_amount = self._safe_extract_numeric(row, 'final_amount')
                if final_amount > 75.0:
                    filtered_transactions.append({
                        "transaction_id": row.transaction_id,
                        "final_amount": final_amount,
                    })
            except ValueError:
                continue
        return {"transactions": filtered_transactions}

class Q2Strategy(BaseQueryStrategy):
    """Strategy for Q2: Ranks products by sales quantity and revenue per month."""
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        metrics = defaultdict(lambda: {'quantity': 0, 'revenue': 0.0})
        for row in consolidated_data.get('TransactionItemsMenuItems', []):
            try:
                date = self._safe_extract_date(row)
                month_key = date.strftime('%Y-%m')
                item_name = getattr(row, 'item_name', 'Unknown Item')
                key = f"{month_key}|{item_name}"
                metrics[key]['quantity'] += int(getattr(row, 'quantity', 0))
                metrics[key]['revenue'] += self._safe_extract_numeric(row, 'subtotal')
            except (ValueError, AttributeError):
                continue

        parsed_data = [
            {"month": key.split('|')[0], "name": key.split('|')[1], **value}
            for key, value in metrics.items()
        ]
        
        final_result = {}
        for month, items in _group_by(parsed_data, key_func=lambda x: x['month']).items():
            final_result[month] = {
                "by_quantity": sorted(
                    [{"name": p['name'], "quantity": p['quantity']} for p in items],
                    key=lambda x: x['quantity'], reverse=True
                ),
                "by_revenue": sorted(
                    [{"name": p['name'], "revenue": round(p['revenue'], 2)} for p in items],
                    key=lambda x: x['revenue'], reverse=True
                )
            }
        return dict(sorted(final_result.items()))

class Q3Strategy(BaseQueryStrategy):
    """Strategy for Q3: Calculates Total Processing Volume (TPV) by store per semester."""
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        tpv_metrics = defaultdict(float)
        for row in consolidated_data.get('TransactionStores', []):
            try:
                date = self._safe_extract_date(row)
                if not (6 <= date.hour < 23):
                    continue
                semester = "S1" if date.month <= 6 else "S2"
                store_name = getattr(row, 'store_name', 'Unknown Store')
                key = f"{date.year}-{semester}|{store_name}"
                tpv_metrics[key] += self._safe_extract_numeric(row, 'final_amount')
            except (ValueError, AttributeError):
                continue

        final_result = defaultdict(dict)
        for key, total_volume in tpv_metrics.items():
            year_semester, store_name = key.split('|', 1)
            final_result[store_name][year_semester] = round(total_volume, 2)
        return dict(final_result)

class Q4Strategy(BaseQueryStrategy):
    """Strategy for Q4: Identifies top 3 customers by purchase count for each store."""
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        purchase_counts = defaultdict(int)
        user_birthdates = {}
        for row in consolidated_data.get('TransactionStoresUsers', []):
            user_id = getattr(row, 'user_id', None)
            if not user_id:
                continue
            store_name = getattr(row, 'store_name', 'Unknown Store')
            key = (store_name, user_id)
            purchase_counts[key] += 1
            if user_id not in user_birthdates:
                user_birthdates[user_id] = getattr(row, 'birthdate', 'Unknown')

        parsed_data = [
            {"store_name": key[0], "user_id": key[1], "count": count}
            for key, count in purchase_counts.items()
        ]
        
        final_result = {}
        for store_name, customers in _group_by(parsed_data, key_func=lambda x: x['store_name']).items():
            top_3 = sorted(customers, key=lambda x: x['count'], reverse=True)[:3]
            final_result[store_name] = [
                {
                    "birthdate": user_birthdates.get(c['user_id'], 'Unknown'), 
                    "purchase_count": c['count']
                }
                for c in top_3
            ]
        return final_result

_STRATEGY_MAPPING = {
    QueryType.Q1: Q1Strategy(),
    QueryType.Q2: Q2Strategy(),
    QueryType.Q3: Q3Strategy(),
    QueryType.Q4: Q4Strategy(),
}

def get_strategy(query_type: QueryType) -> BaseQueryStrategy:
    strategy = _STRATEGY_MAPPING.get(query_type)
    if not strategy:
        raise ValueError(f"No strategy found for query type: {query_type}")
    return strategy
