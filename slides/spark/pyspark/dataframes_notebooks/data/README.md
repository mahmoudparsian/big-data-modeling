# data

Sample files for the DataFrame notebooks in this folder. The data is made up: *Bean & Leaf*, a small coffee chain.

| Name | Type | Description | Used in |
|---|---|---|---|
| [`coffee_sales_raw.csv`](coffee_sales_raw.csv) | CSV, 14 rows | Raw cash-register export with deliberate problems (duplicate row, extra spaces, mixed case, prices like `$4.50`, quantity `two`, two date formats, a bad date, missing payment, negative quantity) | Notebook 3, Sections 4d and 10 (cleaning) |
| [`stores.json`](stores.json) | JSON lines, 5 rows | The chain's stores: `store_id`, `name`, `city`, `opened` | Notebook 3, Section 4d and exercise 4 |
| [`app_orders.json`](app_orders.json) | JSON lines, 5 rows | Mobile-app orders with nested data: a `customer` struct and an `items` array of structs (order A4 has no items) | Notebook 7, Sections 2 and 5, exercise 1 |
| [`sales_transactions.csv`](sales_transactions.csv) | CSV, 8 rows | The 8 warm-up sales (`sale_id`, `store_id`, `product`, `category`, `quantity`, `unit_price`) | A copy of the file Notebook 1 writes; Notebooks 0–2 use the same 8 sales |

Notebooks 0, 1, 2, 4, 5 and 6 create their data inline, so each one runs on its own.
