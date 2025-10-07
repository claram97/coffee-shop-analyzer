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
    """Strategy for Q1: Handles pre-filtered transactions (already filtered by amount and time)."""
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        transactions = consolidated_data.get('Transactions', [])
        filtered_transactions = []
        for row in transactions:
            try:
                # Simply extract the data without re-filtering
                # since it was already filtered upstream
                final_amount = self._safe_extract_numeric(row, 'final_amount')
                
                # Log transaction data for diagnosis
                import logging
                logging.info(f"Q1 Transaction: id={row.transaction_id}, final_amount={final_amount}")
                
                filtered_transactions.append({
                    "transaction_id": row.transaction_id,
                    "final_amount": final_amount
                })
            except (ValueError, AttributeError):
                continue
                
        # Log summary of processed transactions
        import logging
        logging.info("--- Q1 Final Transactions Summary ---")
        logging.info(f"Total filtered transactions: {len(filtered_transactions)}")
        if filtered_transactions:
            logging.info(f"Sample transaction: {filtered_transactions[0]}")
            
        return {"transactions": filtered_transactions}

class Q2Strategy(BaseQueryStrategy):
    """Strategy for Q2: Handles pre-aggregated product metrics by month.
    
    Note: The aggregator has already:
    - Aggregated quantities and revenues by month and item within each batch
    - Set the created_at field to represent the month (first day of month)
    - Set the quantity field to total quantity and subtotal to total revenue
    
    The results-finisher needs to further aggregate these metrics across batches.
    """
    
    def _log_metrics(self, month: str, item_name: str, quantity: int, revenue: float):
        """Helper method to log metrics for debugging."""
        import logging
        logging.info(f"Q2 Metrics: Month={month}, Item={item_name}, Quantity={quantity}, Revenue={revenue}")
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        # First, aggregate metrics by month and product across all batches
        metrics_by_month_product = defaultdict(lambda: {'quantity': 0, 'revenue': 0.0})
        
        for row in consolidated_data.get('TransactionItemsMenuItems', []):
            try:
                date = self._safe_extract_date(row)
                month_key = date.strftime('%Y-%m')
                item_name = getattr(row, 'item_name', 'Unknown Item')
                
                # Aggregate by month and product name
                key = (month_key, item_name)
                
                # Add the pre-aggregated quantities and revenues from this batch
                # Handle quantity safely - convert to correct integer value
                quantity_str = getattr(row, 'quantity', '0')
                try:
                    # Parse the quantity string to integer without any arbitrary adjustments
                    raw_quantity_val = float(quantity_str) if quantity_str.strip() else 0
                    quantity_val = int(raw_quantity_val)
                    
                    # Log the raw values we're seeing for diagnosis
                    import logging
                    logging.info(f"Q2 Raw Quantity: month={month_key}, item={item_name}, raw_str={quantity_str}, parsed={raw_quantity_val}, final={quantity_val}")
                except (ValueError, TypeError) as e:
                    quantity_val = 0
                    logging.warning(f"Q2 Quantity Parse Error: {e}, using default 0")
                
                # Aggregate the quantity exactly as it was provided
                metrics_by_month_product[key]['quantity'] += quantity_val
                
                # Handle subtotal/revenue safely
                revenue_val = self._safe_extract_numeric(row, 'subtotal')
                metrics_by_month_product[key]['revenue'] += revenue_val
                
                # Debug log the metrics to understand what's happening
                self._log_metrics(month_key, item_name, quantity_val, revenue_val)
            except (ValueError, AttributeError):
                continue
        
        # Format results by month
        result_by_month = {}
        
        # Log summary of aggregation before formatting
        import logging
        logging.info("--- Q2 Final Aggregated Metrics Summary ---")
        for (month, product_name), metrics in sorted(metrics_by_month_product.items()):
            logging.info(f"Month={month}, Product={product_name}, Total Quantity={metrics['quantity']}, Total Revenue={metrics['revenue']:.2f}")
        
        for (month, product_name), metrics in metrics_by_month_product.items():
            if month not in result_by_month:
                result_by_month[month] = {
                    "by_quantity": [],
                    "by_revenue": []
                }
            
            final_quantity = int(metrics['quantity'])
            logging.info(f"Q2 Final Result: month={month}, product={product_name}, quantity={final_quantity}, revenue={round(metrics['revenue'], 2)}")
            
            result_by_month[month]["by_quantity"].append(
                {"name": product_name, "quantity": final_quantity}
            )
            result_by_month[month]["by_revenue"].append(
                {"name": product_name, "revenue": round(metrics['revenue'], 2)}
            )
        
        # Sort each month's products by quantity and revenue
        for month_data in result_by_month.values():
            month_data["by_quantity"].sort(key=lambda x: x["quantity"], reverse=True)
            month_data["by_revenue"].sort(key=lambda x: x["revenue"], reverse=True)
        
        return dict(sorted(result_by_month.items()))

class Q3Strategy(BaseQueryStrategy):
    """Strategy for Q3: Handles pre-aggregated TPV data by store per semester.
    
    Note: The aggregator has already:
    - Aggregated final amounts by store, year, and semester within each batch
    - The transaction_id field contains the transaction count
    - The created_at field contains the semester in format "YYYY-S" (e.g., "2024-1")
    - The final_amount already contains the total amount for that store/semester
    
    The results-finisher needs to further aggregate these amounts across batches.
    """
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        # Aggregate TPV by store and semester across all batches
        tpv_by_store_semester = defaultdict(float)
        
        for row in consolidated_data.get('TransactionStores', []):
            try:
                # Extract data from the pre-aggregated format
                created_at = getattr(row, 'created_at', '')
                if not created_at:
                    continue
                    
                store_name = getattr(row, 'store_name', 'Unknown Store')
                
                # Get the raw amount string for logging
                amount_str = getattr(row, 'final_amount', '0')
                amount = self._safe_extract_numeric(row, 'final_amount')
                
                # Log the amount processing for diagnosis
                import logging
                logging.info(f"Q3 Amount: store={store_name}, created_at={created_at}, raw_str={amount_str}, parsed={amount}")
                
                # The created_at already contains the year-semester
                year_semester = f"{created_at}"
                if year_semester:
                    # Convert '2024-1' to '2024-S1' format if needed
                    if len(year_semester) > 0 and not year_semester.endswith('S1') and not year_semester.endswith('S2'):
                        semester = year_semester.split('-')[-1]
                        year = year_semester.split('-')[0]
                        year_semester = f"{year}-S{semester}"
                    
                    # Aggregate amounts across batches
                    key = (store_name, year_semester)
                    tpv_by_store_semester[key] += amount
            except (ValueError, AttributeError):
                continue
        
        # Format the final result
        final_result = defaultdict(dict)
        
        # Log summary of aggregation before formatting
        import logging
        logging.info("--- Q3 Final Aggregated Amounts Summary ---")
        for (store_name, year_semester), total_amount in sorted(tpv_by_store_semester.items()):
            logging.info(f"Store={store_name}, Period={year_semester}, Total Amount={total_amount:.2f}")
            final_result[store_name][year_semester] = round(total_amount, 2)
            
        return dict(final_result)

class Q4Strategy(BaseQueryStrategy):
    """Strategy for Q4: Identifies top 3 customers by purchase count for each store.
    
    Note: The aggregator has already:
    - Counted transactions by store-user pair within each batch
    - Repurposed the transaction_id field to contain the purchase count
    - Most fields are empty, with only store_id, user_id, and purchase count populated
    
    The results-finisher needs to further aggregate these counts across batches.
    """
    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        # Aggregate purchase counts by store and user across all batches
        purchase_counts = defaultdict(int)
        user_birthdates = {}
        
        for row in consolidated_data.get('TransactionStoresUsers', []):
            try:
                store_name = getattr(row, 'store_name', 'Unknown Store')
                user_id = getattr(row, 'user_id', None)
                birthdate = getattr(row, 'birthdate', 'Unknown')
                
                # The transaction_id field now contains the purchase count from this batch
                purchase_count_str = getattr(row, 'transaction_id', '0')
                
                try:
                    purchase_count = int(purchase_count_str) if purchase_count_str.strip() else 0
                    
                    # Log the purchase count processing
                    import logging
                    logging.info(f"Q4 Purchase Count: store={store_name}, user={user_id}, raw_str={purchase_count_str}, parsed={purchase_count}")
                except (ValueError, TypeError) as e:
                    purchase_count = 0
                    logging.warning(f"Q4 Purchase Count Parse Error: {e}, using default 0")
                
                if user_id and purchase_count > 0:
                    # Aggregate purchase counts across batches
                    key = (store_name, user_id)
                    purchase_counts[key] += purchase_count
                    
                    # Store the most recent birthdate information
                    if user_id not in user_birthdates or birthdate != 'Unknown':
                        user_birthdates[user_id] = birthdate
            except (ValueError, AttributeError):
                continue
        
        # Transform the aggregated data to prepare for sorting
        store_user_data = defaultdict(list)
        for (store_name, user_id), count in purchase_counts.items():
            store_user_data[store_name].append({
                "user_id": user_id,
                "birthdate": user_birthdates.get(user_id, 'Unknown'),
                "purchase_count": count
            })
        
        # Get top 3 customers by purchase count for each store
        final_result = {}
        for store_name, users in store_user_data.items():
            top_3 = sorted(users, key=lambda x: x['purchase_count'], reverse=True)[:3]
            final_result[store_name] = [
                {"birthdate": user["birthdate"], "purchase_count": user["purchase_count"]}
                for user in top_3
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
