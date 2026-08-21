"""Generate the synthetic retail dataset used by mapreduce-pyspark-tutorial.md
and mapreduce-pyspark-notebook.ipynb.

Run once from anywhere:

    python3 generate_retail_data.py

Writes four related CSVs into data/ (next to this script), deterministically
(seed=7), so re-running always produces the same numbers:

    data/customers.csv   customer_id, country, segment
    data/products.csv    product_id, sku, category, price
    data/orders.csv      order_id, customer_id, order_date, channel
    data/items.csv       order_id, product_id, quantity, unit_price

Pure stdlib — no PySpark or pandas required to generate the data.
"""

import csv
import os
import random
from datetime import date, timedelta

random.seed(7)

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(ROOT, exist_ok=True)

CATEGORIES = ["Electronics", "Apparel", "Books", "Home", "Beauty"]
COUNTRIES  = ["US", "CA", "MX", "GB", "DE"]
PRODUCTS   = [(i, f"PROD-{i:03d}", random.choice(CATEGORIES),
               round(random.uniform(5, 400), 2)) for i in range(1, 51)]

# customers.csv
with open(f"{ROOT}/customers.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["customer_id", "country", "segment"])
    for cid in range(1, 201):
        w.writerow([cid, random.choice(COUNTRIES),
                    random.choice(["consumer", "smb", "enterprise"])])

# products.csv
with open(f"{ROOT}/products.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["product_id", "sku", "category", "price"])
    for pid, sku, cat, price in PRODUCTS:
        w.writerow([pid, sku, cat, price])

# orders.csv + items.csv
start = date(2026, 1, 1)
with open(f"{ROOT}/orders.csv", "w", newline="") as fo, \
     open(f"{ROOT}/items.csv", "w", newline="") as fi:
    wo = csv.writer(fo); wo.writerow(["order_id", "customer_id", "order_date", "channel"])
    wi = csv.writer(fi); wi.writerow(["order_id", "product_id", "quantity", "unit_price"])
    for oid in range(1, 1001):
        cid     = random.randint(1, 200)
        d       = start + timedelta(days=random.randint(0, 89))
        channel = random.choice(["web", "mobile", "in_store"])
        wo.writerow([oid, cid, d.isoformat(), channel])
        for _ in range(random.randint(1, 4)):
            pid, _, _, price = random.choice(PRODUCTS)
            wi.writerow([oid, pid, random.randint(1, 3), price])

print("data ready in", ROOT)
