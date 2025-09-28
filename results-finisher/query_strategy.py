from typing import List, Dict, Any
from collections import defaultdict
import datetime
from constants import QueryType

def _manual_group_by(data: List[Dict], key_func) -> Dict[Any, List[Dict]]:
    grouped = defaultdict(list)
    for item in data:
        key = key_func(item)
        grouped[key].append(item)
    return grouped

class BaseQueryStrategy:
    """Base class for query-specific consolidation and finalization logic."""
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        raise NotImplementedError("Consolidation logic not implemented")

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("Finalization logic not implemented")

class Q1Strategy(BaseQueryStrategy):
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        # All rows are pre-joined transaction+store data, so just accumulate them
        state_data.setdefault('transactions', []).extend(new_rows)

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        # Process all pre-joined transaction records (now with store data available)
        return {
            "transactions": [
                {
                    "transaction_id": tx.transaction_id,
                    "final_amount": float(tx.final_amount),
                    "store_id": tx.store_id,
                    # Include store data if available (from joined data)
                    "store_name": getattr(tx, 'store_name', 'Unknown'),
                    "store_city": getattr(tx, 'city', 'Unknown'),
                    "store_state": getattr(tx, 'state', 'Unknown')
                }
                for tx in consolidated_data.get('transactions', [])
            ]
        }

class Q2Strategy(BaseQueryStrategy):
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        metrics = state_data.setdefault('product_metrics', defaultdict(lambda: {'quantity': 0, 'revenue': 0.0}))
        for row in new_rows:
            try:
                date = datetime.datetime.fromisoformat(row.created_at.replace('Z', '+00:00'))
                month_key = date.strftime('%Y-%m')
                key = f"{month_key}|{row.item_name}"
                metrics[key]['quantity'] += int(row.quantity)
                metrics[key]['revenue'] += float(row.final_amount)
            except (ValueError, AttributeError):
                continue

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        metrics = consolidated_data.get('product_metrics', {})
        parsed_data = []
        for key, value in metrics.items():
            month, item_name = key.split('|', 1)
            parsed_data.append({
                "month": month, "name": item_name,
                "quantity": value['quantity'], "revenue": value['revenue']
            })
        
        month_groups = _manual_group_by(parsed_data, key_func=lambda x: x['month'])
        
        final_result = {}
        for month, items in sorted(month_groups.items()):
            by_qty = sorted(items, key=lambda x: x['quantity'], reverse=True)
            by_rev = sorted(items, key=lambda x: x['revenue'], reverse=True)
            final_result[month] = {
                "by_quantity": [{"name": p['name'], "quantity": p['quantity']} for p in by_qty],
                "by_revenue": [{"name": p['name'], "revenue": round(p['revenue'], 2)} for p in by_rev]
            }
        return final_result

class Q3Strategy(BaseQueryStrategy):
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        tpv = state_data.setdefault('tpv_metrics', defaultdict(float))
        for row in new_rows:
            try:
                date = datetime.datetime.fromisoformat(row.created_at.replace('Z', '+00:00'))
                year = date.year
                semester = "S1" if date.month <= 6 else "S2"
                key = f"{year}_{semester}|{row.store_name}"
                tpv[key] += float(row.final_amount)
            except (ValueError, AttributeError):
                continue

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        tpv_metrics = consolidated_data.get('tpv_metrics', {})
        final_result = defaultdict(dict)
        for key, value in tpv_metrics.items():
            year_semester, store_name = key.split('|', 1)
            final_result[store_name][year_semester] = round(value, 2)
        return dict(final_result)

class Q4Strategy(BaseQueryStrategy):
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        counts = state_data.setdefault('purchase_counts', defaultdict(int))
        for row in new_rows:
            if row.user_id:
                key = f"{row.store_name}|{row.birthdate}"
                counts[key] += 1
    
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        counts = consolidated_data.get('purchase_counts', {})
        parsed_data = []
        for key, count in counts.items():
            store_name, birthdate = key.split('|', 1)
            parsed_data.append({"store_name": store_name, "birthdate": birthdate, "count": count})
            
        store_groups = _manual_group_by(parsed_data, key_func=lambda x: x['store_name'])
        
        final_result = {}
        for store_name, customers in store_groups.items():
            top_3 = sorted(customers, key=lambda x: x['count'], reverse=True)[:3]
            final_result[store_name] = [
                {"birthdate": c['birthdate'], "purchase_count": c['count']}
                for c in top_3
            ]
        return final_result

def get_strategy(query_type: QueryType) -> BaseQueryStrategy:
    """Factory function to get the correct strategy object for a query."""
    strategies = {
        QueryType.Q1: Q1Strategy(),
        QueryType.Q2: Q2Strategy(),
        QueryType.Q3: Q3Strategy(),
        QueryType.Q4: Q4Strategy(),
    }
    return strategies.get(query_type, BaseQueryStrategy())