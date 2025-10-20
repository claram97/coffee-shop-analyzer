#!/usr/bin/env python3
"""Validate client query outputs against expected results.

This script compares the JSON results produced by a client run with the
reference CSV datasets stored under ``expected-results``.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, Tuple

Number = float


def _load_json(path: Path) -> Iterable[dict]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing client result file: {path}") from exc


def _isclose(a: Number, b: Number, *, rel_tol: float = 1e-9, abs_tol: float = 1e-6) -> bool:
    return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)


def validate_query1(client_dir: Path, expected_dir: Path) -> Tuple[bool, str]:
    client_path = client_dir / "query1_results.json"
    expected_path = expected_dir / "query_1_results.csv"

    client_records = _load_json(client_path)

    client_map: Dict[str, Number] = {
        str(record["transaction_id"]): float(record["final_amount"])
        for record in client_records
    }

    expected_map: Dict[str, Number] = {}
    try:
        with expected_path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                expected_map[row["transaction_id"]] = float(row["final_amount"])
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing expected results file: {expected_path}") from exc

    missing_in_client = sorted(set(expected_map) - set(client_map))
    unexpected = sorted(set(client_map) - set(expected_map))
    mismatched = [
        tid
        for tid in set(expected_map) & set(client_map)
        if not _isclose(client_map[tid], expected_map[tid])
    ]

    if missing_in_client or unexpected or mismatched:
        details = []
        if missing_in_client:
            details.append(f"missing {len(missing_in_client)} transactions in client output")
        if unexpected:
            details.append(f"found {len(unexpected)} unexpected transactions")
        if mismatched:
            details.append(f"{len(mismatched)} transactions differ in final_amount")
        return False, "query1: " + ", ".join(details)

    return True, "query1: OK"


def validate_query2(client_dir: Path, expected_dir: Path) -> Tuple[bool, str]:
    client_path = client_dir / "query2_results.json"
    best_expected_path = expected_dir / "query_2_best_selling_results.csv"
    profit_expected_path = expected_dir / "query_2_most_profit_results.csv"

    client_records = _load_json(client_path)

    client_best = {
        (record["month"], record["name"]): int(record["quantity"])
        for record in client_records
        if "quantity" in record
    }
    client_profit = {
        (record["month"], record["name"]): float(record["revenue"])
        for record in client_records
        if "revenue" in record
    }

    expected_best: Dict[Tuple[str, str], int] = {}
    expected_profit: Dict[Tuple[str, str], float] = {}

    try:
        with best_expected_path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                key = (row["year_month_created_at"], row["item_name"])
                expected_best[key] = int(row["sellings_qty"])
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing expected results file: {best_expected_path}") from exc

    try:
        with profit_expected_path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                key = (row["year_month_created_at"], row["item_name"])
                expected_profit[key] = float(row["profit_sum"])
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing expected results file: {profit_expected_path}") from exc

    def _compare(expected: Dict, received: Dict, label: str, value_label: str) -> Tuple[bool, str]:
        missing = sorted(set(expected) - set(received))
        unexpected = sorted(set(received) - set(expected))
        mismatched = [
            key
            for key in set(expected) & set(received)
            if not _isclose(received[key], expected[key])
        ]
        if missing or unexpected or mismatched:
            chunks = []
            if missing:
                chunks.append(f"missing {len(missing)} entries")
            if unexpected:
                chunks.append(f"found {len(unexpected)} unexpected entries")
            if mismatched:
                chunks.append(f"{len(mismatched)} entries differ in {value_label}")
            return False, f"{label}: " + ", ".join(chunks)
        return True, f"{label}: OK"

    best_ok, best_msg = _compare(expected_best, client_best, "query2 best-selling", "quantity")
    profit_ok, profit_msg = _compare(expected_profit, client_profit, "query2 most-profit", "revenue")

    return best_ok and profit_ok, "; ".join(filter(None, [best_msg, profit_msg]))


def validate_query3(client_dir: Path, expected_dir: Path) -> Tuple[bool, str]:
    client_path = client_dir / "query3_results.json"
    expected_path = expected_dir / "query_3_results.csv"

    client_records = _load_json(client_path)

    def normalise_period(raw: str) -> str:
        return raw.replace("-S", "-H")

    client_map = {
        (normalise_period(record["period"]), record["store_name"]): float(record["amount"])
        for record in client_records
    }

    expected_map: Dict[Tuple[str, str], float] = {}
    try:
        with expected_path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                key = (row["year_half_created_at"], row["store_name"])
                expected_map[key] = float(row["tpv"])
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing expected results file: {expected_path}") from exc

    missing = sorted(set(expected_map) - set(client_map))
    unexpected = sorted(set(client_map) - set(expected_map))
    mismatched = [
        key
        for key in set(expected_map) & set(client_map)
        if not _isclose(client_map[key], expected_map[key])
    ]

    if missing or unexpected or mismatched:
        parts = []
        if missing:
            parts.append(f"missing {len(missing)} store-period pairs")
        if unexpected:
            parts.append(f"found {len(unexpected)} unexpected store-period pairs")
        if mismatched:
            parts.append(f"{len(mismatched)} store-period pairs differ in TPV")
        return False, "query3: " + ", ".join(parts)

    return True, "query3: OK"


def validate_query4(client_dir: Path, expected_dir: Path) -> Tuple[bool, str]:
    client_path = client_dir / "query4_results.json"
    expected_path = expected_dir / "query_4_results.csv"

    client_records = _load_json(client_path)
    client_tuples = {
        (record["store_name"], record["birthdate"], int(record["purchase_count"]))
        for record in client_records
    }

    store_counts = Counter(record["store_name"] for record in client_records)
    client_max: Dict[str, int] = defaultdict(int)
    for record in client_records:
        store = record["store_name"]
        client_max[store] = max(client_max[store], int(record["purchase_count"]))

    expected_tuples = set()
    expected_max: Dict[str, int] = defaultdict(int)

    try:
        with expected_path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                store_name = row["store_name"]
                count = int(row["purchases_qty"])
                expected_tuples.add((store_name, row["birthdate"], count))
                expected_max[store_name] = max(expected_max[store_name], count)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing expected results file: {expected_path}") from exc

    missing = sorted(client_tuples - expected_tuples)
    stores_missing_max = [
        store
        for store, max_count in expected_max.items()
        if store in client_max and max_count != client_max[store]
    ]

    inconsistent_counts = [store for store, count in store_counts.items() if count != 3]
    unexpected_store = sorted(set(store_counts) - set(expected_max))

    if missing or stores_missing_max or inconsistent_counts or unexpected_store:
        messages = []
        if missing:
            messages.append(f"{len(missing)} client entries not present in expected dataset")
        if stores_missing_max:
            messages.append(
                "stores where max purchase count does not match expected: "
                + ", ".join(sorted(set(stores_missing_max)))
            )
        if inconsistent_counts:
            messages.append(
                "stores with number of returned customers different from 3: "
                + ", ".join(sorted(set(inconsistent_counts)))
            )
        if unexpected_store:
            messages.append(
                "unexpected stores in client output: " + ", ".join(unexpected_store)
            )
        return False, "query4: " + ", ".join(messages)

    return True, "query4: OK"


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate client query outputs against expected results.")
    parser.add_argument(
        "client_dir",
        type=Path,
        help="Path to the client run directory (e.g. client_runs/<client_uuid>).",
    )
    parser.add_argument(
        "--expected-dir",
        type=Path,
        default=Path("expected-results"),
        help="Path to the expected results directory (default: expected-results).",
    )
    args = parser.parse_args()

    client_dir: Path = args.client_dir
    expected_dir: Path = args.expected_dir

    if not client_dir.exists():
        raise FileNotFoundError(f"Client directory does not exist: {client_dir}")
    if not expected_dir.exists():
        raise FileNotFoundError(f"Expected results directory does not exist: {expected_dir}")

    validators = (
        validate_query1,
        validate_query2,
        validate_query3,
        validate_query4,
    )

    all_ok = True
    messages = []
    for validator in validators:
        ok, message = validator(client_dir, expected_dir)
        messages.append(message)
        all_ok &= ok

    for message in messages:
        print(message)

    if not all_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
