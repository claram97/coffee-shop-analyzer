"""
Query strategy implementations using the 'Append-Only' model.

This model prioritizes low lock contention and high ingestion throughput by
performing minimal work during the consolidation step (appending rows) and
concentrating all business logic in the finalization step.

Its primary trade-off is higher memory usage.
"""

import datetime
from collections import defaultdict
from typing import Any, Callable, Dict, List

from constants import QueryType


def _group_by(
    data: List[Dict[str, Any]], key_func: Callable
) -> Dict[Any, List[Dict[str, Any]]]:
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

    def consolidate(
        self, state_data: Dict[str, Any], table_type: str, new_rows: List[Any]
    ):
        """
        Default consolidation behavior: stores all raw rows, categorized by their table type.
        """
        state_data.setdefault(table_type, []).extend(new_rows)

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError(
            "Each query strategy must implement the 'finalize' method."
        )

    def _safe_extract_date(
        self, row: Any, date_field: str = "created_at"
    ) -> datetime.datetime:
        date_str = getattr(row, date_field, None)
        if not date_str:
            raise ValueError(f"Date field '{date_field}' is missing.")
        return datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))

    def _safe_extract_numeric(
        self, row: Any, field: str, default: float = 0.0
    ) -> float:
        try:
            return float(getattr(row, field, default))
        except (ValueError, TypeError):
            return default


# --- Strategy Implementations ---
class Q1Strategy(BaseQueryStrategy):
    """Strategy for Q1: Handles pre-filtered transactions (already filtered by amount and time)."""

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        transactions = consolidated_data.get("Transactions", [])
        filtered_transactions = []
        for row in transactions:
            try:
                # Simply extract the data without re-filtering
                # since it was already filtered upstream
                final_amount = self._safe_extract_numeric(row, "final_amount")

                # Log transaction data for diagnosis
                import logging

                logging.info(
                    f"Q1 Transaction: id={row.transaction_id}, final_amount={final_amount}"
                )

                filtered_transactions.append(
                    {"transaction_id": row.transaction_id, "final_amount": final_amount}
                )
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

        logging.info(
            f"Q2 Metrics: Month={month}, Item={item_name}, Quantity={quantity}, Revenue={revenue}"
        )

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        # First, aggregate metrics by month and product across all batches
        metrics_by_month_product = defaultdict(lambda: {"quantity": 0, "revenue": 0.0})

        for row in consolidated_data.get("TransactionItemsMenuItems", []):
            try:
                date = self._safe_extract_date(row)
                month_key = date.strftime("%Y-%m")
                item_name = getattr(row, "item_name", "Unknown Item")

                # Aggregate by month and product name
                key = (month_key, item_name)

                # Add the pre-aggregated quantities and revenues from this batch
                # Handle quantity safely - convert to correct integer value
                quantity_str = getattr(row, "quantity", "0")
                try:
                    # Parse the quantity string to integer without any arbitrary adjustments
                    raw_quantity_val = (
                        float(quantity_str) if quantity_str.strip() else 0
                    )
                    quantity_val = int(raw_quantity_val)

                    # Log the raw values we're seeing for diagnosis
                    import logging

                    logging.info(
                        f"Q2 Raw Quantity: month={month_key}, item={item_name}, raw_str={quantity_str}, parsed={raw_quantity_val}, final={quantity_val}"
                    )
                except (ValueError, TypeError) as e:
                    quantity_val = 0
                    logging.warning(f"Q2 Quantity Parse Error: {e}, using default 0")

                # Aggregate the quantity exactly as it was provided
                metrics_by_month_product[key]["quantity"] += quantity_val

                # Handle subtotal/revenue safely
                revenue_val = self._safe_extract_numeric(row, "subtotal")
                metrics_by_month_product[key]["revenue"] += revenue_val

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
            logging.info(
                f"Month={month}, Product={product_name}, Total Quantity={metrics['quantity']}, Total Revenue={metrics['revenue']:.2f}"
            )

        for (month, product_name), metrics in metrics_by_month_product.items():
            if month not in result_by_month:
                result_by_month[month] = {"by_quantity": [], "by_revenue": []}

            final_quantity = int(metrics["quantity"])
            logging.info(
                f"Q2 Final Result: month={month}, product={product_name}, quantity={final_quantity}, revenue={round(metrics['revenue'], 2)}"
            )

            result_by_month[month]["by_quantity"].append(
                {"name": product_name, "quantity": final_quantity}
            )
            result_by_month[month]["by_revenue"].append(
                {"name": product_name, "revenue": round(metrics["revenue"], 2)}
            )

        # Sort each month's products by quantity and revenue and keep only the top entry
        for month_data in result_by_month.values():
            month_data["by_quantity"].sort(key=lambda x: x["quantity"], reverse=True)
            month_data["by_revenue"].sort(key=lambda x: x["revenue"], reverse=True)
            month_data["by_quantity"] = month_data["by_quantity"][:1]
            month_data["by_revenue"] = month_data["by_revenue"][:1]

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

        for row in consolidated_data.get("TransactionStores", []):
            try:
                # Extract data from the pre-aggregated format
                created_at = getattr(row, "created_at", "")
                if not created_at:
                    continue

                store_name = getattr(row, "store_name", "Unknown Store")

                # Get the raw amount string for logging
                amount_str = getattr(row, "final_amount", "0")
                amount = self._safe_extract_numeric(row, "final_amount")

                # Log the amount processing for diagnosis
                import logging

                logging.info(
                    f"Q3 Amount: store={store_name}, created_at={created_at}, raw_str={amount_str}, parsed={amount}"
                )

                # The created_at already contains the year-semester
                year_semester = f"{created_at}"
                if year_semester:
                    # Convert '2024-1' to '2024-S1' format if needed
                    if (
                        len(year_semester) > 0
                        and not year_semester.endswith("S1")
                        and not year_semester.endswith("S2")
                    ):
                        semester = year_semester.split("-")[-1]
                        year = year_semester.split("-")[0]
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
        for (store_name, year_semester), total_amount in sorted(
            tpv_by_store_semester.items()
        ):
            logging.info(
                f"Store={store_name}, Period={year_semester}, Total Amount={total_amount:.2f}"
            )
            final_result[store_name][year_semester] = round(total_amount, 2)

        return dict(final_result)


class Q4Strategy(BaseQueryStrategy):
    """
    Nuevo Q4: el Joiner manda TransactionStores (1 fila por tx, con user_id)
    y luego Users crudo. Acá contamos compras por (store,user) y al final
    hacemos el join para devolver top 3 con birthdate.
    """

    @staticmethod
    def _norm_uid(v: Any) -> str:
        s = "" if v is None else str(v)
        # normaliza casos tipo "123.0"
        return s.split(".", 1)[0] if s.endswith(".0") else s

    def consolidate(
        self, state_data: Dict[str, Any], table_type: str, new_rows: List[Any]
    ):
        # Estructuras internas:
        # - state_data['_q4_counts']: Dict[str, Dict[str,int]]  store -> {user -> count}
        # - state_data['_q4_users']:  Dict[str, str]           user  -> birthdate
        counts = state_data.setdefault(
            "_q4_counts", defaultdict(lambda: defaultdict(int))
        )
        users = state_data.setdefault("_q4_users", {})

        if table_type == "TransactionStores":
            for r in new_rows or []:
                store = getattr(r, "store_name", "") or ""
                uid = self._norm_uid(getattr(r, "user_id", None))
                if not store or not uid:
                    continue
                counts[store][uid] += 1  # una compra por fila
        elif table_type == "Users":
            for u in new_rows or []:
                uid = self._norm_uid(getattr(u, "user_id", None))
                if not uid:
                    continue
                bd = getattr(u, "birthdate", "") or ""
                # si llega vacío, no pisa un valor previo no vacío
                if bd or uid not in users:
                    users[uid] = bd
        else:
            # Para cualquier otra tabla, guardá crudo por si hiciera falta
            state_data.setdefault(table_type, []).extend(new_rows or [])

    def finalize(self, consolidated_data: Dict[str, Any]) -> Dict[str, Any]:
        counts: Dict[str, Dict[str, int]] = consolidated_data.get("_q4_counts", {})
        users: Dict[str, str] = consolidated_data.get("_q4_users", {})

        final_result: Dict[str, List[Dict[str, Any]]] = {}

        for store, umap in counts.items():
            # ordena por compras desc y desempata por user_id
            top = sorted(umap.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
            out_rows = []
            for uid, cnt in top:
                out_rows.append(
                    {
                        "birthdate": users.get(
                            uid, ""
                        ),  # puede quedar vacío si no vino ese user
                        "purchase_count": cnt,
                    }
                )
            final_result[store] = out_rows

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
