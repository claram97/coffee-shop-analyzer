from typing import List, Dict, Any
from collections import defaultdict
import datetime

# --- Helper function for manual grouping ---
def _manual_group_by(data: List[Dict], key_func) -> Dict[Any, List[Dict]]:
    """Groups a list of dictionaries by a key function, without using itertools."""
    grouped = defaultdict(list)
    for item in data:
        key = key_func(item)
        grouped[key].append(item)
    return grouped

# --- Base and specific strategy classes ---

class BaseQueryStrategy:
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        raise NotImplementedError

    def finalize(self, consolidated_data: Dict[str, Any], dimensional_data: Dict) -> Dict[str, Any]:
        raise NotImplementedError

class Q1Strategy(BaseQueryStrategy):
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        state_data.setdefault('transactions', []).extend(new_rows)

    def finalize(self, consolidated_data: Dict[str, Any], dimensional_data: Dict) -> Dict[str, Any]:
        transactions = consolidated_data.get('transactions', [])
        stores = dimensional_data.get('stores', {})
        
        final_txs = []
        for tx in transactions:
            store_name = stores.get(tx.store_id, f"Unknown Store ID: {tx.store_id}")
            final_txs.append({
                "transaction_id": tx.transaction_id,
                "final_amount": float(tx.final_amount),
                "store_name": store_name
            })
        return {"transactions": final_txs}

class Q2Strategy(BaseQueryStrategy):
    """Strategy for Query 2: Top Products by Month."""
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        metrics = state_data.setdefault('product_metrics', defaultdict(lambda: {'quantity': 0, 'revenue': 0.0}))
        for row in new_rows:
            key = f"{row['month']}|{row['item_id']}"
            metrics[key]['quantity'] += row['quantity']
            metrics[key]['revenue'] += row['revenue']

    def finalize(self, consolidated_data: Dict[str, Any], dimensional_data: Dict) -> Dict[str, Any]:
        metrics = consolidated_data.get('product_metrics', {})
        menu_items = dimensional_data.get('menu_items', {})
        
        parsed_data = []
        for key, value in metrics.items():
            month, item_id = key.split('|')
            item_name = menu_items.get(item_id, f"Unknown Item ID: {item_id}")
            parsed_data.append({
                "month": month,
                "name": item_name,
                "quantity": value['quantity'],
                "revenue": value['revenue']
            })
            
        month_groups = _manual_group_by(parsed_data, key_func=lambda x: x['month'])
        
        final_result = {}
        for month, items in sorted(month_groups.items()):
            by_qty = sorted(items, key=lambda x: x['quantity'], reverse=True)
            by_rev = sorted(items, key=lambda x: x['revenue'], reverse=True)
            final_result[month] = {
                "by_quantity": [{"name": p['name'], "quantity": p['quantity']} for p in by_qty],
                "by_revenue": [{"name": p['name'], "revenue": p['revenue']} for p in by_rev]
            }
        return final_result

class Q3Strategy(BaseQueryStrategy):
    """Strategy for Query 3: TPV by Semester and Sucursal."""
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        tpv = state_data.setdefault('tpv_metrics', defaultdict(float))
        for row in new_rows:
            key = row['key']
            tpv[key] += row['value']

    def finalize(self, consolidated_data: Dict[str, Any], dimensional_data: Dict) -> Dict[str, Any]:
        tpv_metrics = consolidated_data.get('tpv_metrics', {})
        stores = dimensional_data.get('stores', {})
        
        final_result = defaultdict(dict)
        
        for key, value in tpv_metrics.items():
            year_semester, store_id = key.split('|')
            store_name = stores.get(store_id, f"Unknown Store ID: {store_id}")
            final_result[store_name][year_semester] = value
            
        return dict(final_result)

class Q4Strategy(BaseQueryStrategy):
    """Strategy for Query 4: Top 3 Customers by Store."""
    def consolidate(self, state_data: Dict[str, Any], new_rows: List[Any]):
        counts = state_data.setdefault('purchase_counts', defaultdict(int))
        for row in new_rows:
            key = row['key']
            counts[key] += row['value']
    
    def finalize(self, consolidated_data: Dict[str, Any], dimensional_data: Dict) -> Dict[str, Any]:
        counts = consolidated_data.get('purchase_counts', {})
        stores = dimensional_data.get('stores', {})
        users = dimensional_data.get('users', {})
        
        parsed_data = []
        for key, count in counts.items():
            store_id, user_id = key.split('|')
            parsed_data.append({"store_id": store_id, "user_id": user_id, "count": count})
            
        store_groups = _manual_group_by(parsed_data, key_func=lambda x: x['store_id'])
        
        final_result = {}
        for store_id, customers in store_groups.items():
            top_3 = sorted(customers, key=lambda x: x['count'], reverse=True)[:3]
            
            store_name = stores.get(store_id, f"Unknown Store ID: {store_id}")
            final_result[store_name] = []
            for customer in top_3:
                birthdate = users.get(customer['user_id'], "Unknown Birthdate")
                final_result[store_name].append({
                    "birthdate": birthdate,
                    "purchase_count": customer['count']
                })
        return final_result

def get_strategy(query_type: str) -> BaseQueryStrategy:
    strategies = {
        "Q1": Q1Strategy(),
        "Q2": Q2Strategy(),
        "Q3": Q3Strategy(),
        "Q4": Q4Strategy(),
    }
    strategy = strategies.get(query_type)
    if not strategy:
        return BaseQueryStrategy()
    return strategy

