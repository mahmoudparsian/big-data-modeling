# data

Sample files for the DataFrame notebooks in this folder. The data is made up: *Bean & Leaf*, a small coffee chain.

| Name | Type | Description | Used in |
|---|---|---|---|
| [`coffee_sales_raw.csv`](coffee_sales_raw.csv) | CSV, 14 rows | Raw cash-register export with deliberate problems (duplicate row, extra spaces, mixed case, prices like `$4.50`, quantity `two`, two date formats, a bad date, missing payment, negative quantity) | Notebook 1, Sections 4d and 10 (cleaning) |
| [`stores.json`](stores.json) | JSON lines, 5 rows | The chain's stores: `store_id`, `name`, `city`, `opened` | Notebook 1, Section 4d and exercise 4 |
| [`app_orders.json`](app_orders.json) | JSON lines, 5 rows | Mobile-app orders with nested data: a `customer` struct and an `items` array of structs (order A4 has no items) | Notebook 5, Sections 2 and 5, exercise 1 |

Notebooks 2, 3 and 4 create their data inline, so each one runs on its own.
