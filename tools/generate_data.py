#!/usr/bin/env python3
"""Generate the synthetic demo datasets under data/<dataset>/ (deterministic, seed 42).

Usage:  python3 tools/generate_data.py
"""
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def pick(rng, weighted):
    r, acc = rng.random(), 0.0
    for value, w in weighted:
        acc += w
        if r < acc:
            return value
    return weighted[-1][0]


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def retail():
    out = ROOT / "data" / "retail"
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(42)

    customers = []
    for cid in range(1, 501):
        region = pick(rng, [("EMEA", .45), ("North America", .30), ("APAC", .17), ("LATAM", .08)])
        segment = pick(rng, [("Enterprise", .2), ("SMB", .4), ("Consumer", .4)])
        customers.append((cid, f"Customer {cid:04d}", region, segment))
    write_csv(out / "dim_customer.csv", ["customer_id", "customer_name", "region", "segment"], customers)

    # Cheap categories have low ids, and low ids are ordered far more often (see fact_orders)
    categories = [("Books", 15.0), ("Beauty", 30.0), ("Home", 80.0), ("Sports", 120.0), ("Electronics", 650.0)]
    products = []
    for pid in range(1, 41):
        category, base = categories[(pid - 1) // 8]
        products.append((pid, f"{category} item {pid:02d}", category, round(base * (0.8 + rng.random() * 0.4), 2)))
    write_csv(out / "dim_product.csv", ["product_id", "product_name", "category", "unit_price"], products)

    start, days = date(2025, 1, 1), (date(2026, 10, 31) - date(2025, 1, 1)).days + 1
    orders = []
    for oid in range(1, 60001):
        orders.append((
            oid,
            (start + timedelta(days=rng.randrange(days))).isoformat(),
            rng.randint(1, 500),
            int(rng.random() ** 2.5 * 40) + 1,
            rng.randint(1, 5),
            pick(rng, [(0.10, .30), (0.25, .10), (0.0, .60)]),
            pick(rng, [("TEST", .02), ("CANCELLED", .06), ("OPEN", .04), ("COMPLETED", .88)]),
        ))
    write_csv(out / "fact_orders.csv",
              ["order_id", "order_date", "customer_id", "product_id", "quantity", "discount", "status"], orders)

    manifest = {
        "name": "retail",
        "description": "Small retail star schema: 500 customers, 40 products, 60,000 orders (Jan 2025 - Oct 2026). "
                       "Cheap Books are ordered far more often than expensive Electronics; includes CANCELLED and TEST orders.",
        "tables": [
            {"name": "dim_customer", "file": "dim_customer.csv", "comment": "Customer dimension",
             "primary_key": ["customer_id"],
             "columns": [["customer_id", "BIGINT NOT NULL", "Unique customer identifier"],
                         ["customer_name", "STRING", "Customer display name"],
                         ["region", "STRING", "Sales region: EMEA, North America, APAC, LATAM"],
                         ["segment", "STRING", "Customer segment: Enterprise, SMB, Consumer"]]},
            {"name": "dim_product", "file": "dim_product.csv", "comment": "Product dimension",
             "primary_key": ["product_id"],
             "columns": [["product_id", "BIGINT NOT NULL", "Unique product identifier"],
                         ["product_name", "STRING", "Product name"],
                         ["category", "STRING", "Product category: Books, Beauty, Home, Sports, Electronics"],
                         ["unit_price", "DOUBLE", "List price per unit in USD"]]},
            {"name": "fact_orders", "file": "fact_orders.csv", "comment": "Order fact table (one row per order)",
             "primary_key": ["order_id"],
             "foreign_keys": [{"column": "customer_id", "references": "dim_customer"},
                              {"column": "product_id", "references": "dim_product"}],
             "columns": [["order_id", "BIGINT NOT NULL", "Unique order identifier"],
                         ["order_date", "DATE", "Date the order was placed"],
                         ["customer_id", "BIGINT", "FK to dim_customer"],
                         ["product_id", "BIGINT", "FK to dim_product"],
                         ["quantity", "INT", "Units ordered"],
                         ["discount", "DOUBLE", "Discount rate applied to the order (0.0 - 1.0)"],
                         ["status", "STRING", "COMPLETED, OPEN, CANCELLED, or TEST (internal test orders - always exclude)"]]},
        ],
    }
    (out / "dataset.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"data/retail: {len(customers)} customers, {len(products)} products, {len(orders)} orders")


if __name__ == "__main__":
    retail()
