# data

Small CSV datasets (headlines, reviews, tickets, products) used by the Claude/LLM + PySpark demos.

## Contents

| Name | Type | Description |
|---|---|---|
| [`headlines.csv`](headlines.csv) | csv (638B) | 10 news headlines (`id,headline_text`) — used by `program_rdd_1_basics.py` |
| [`product_reviews.csv`](product_reviews.csv) | csv (1.5KB) | 18 e-commerce reviews (`id,product_category,review_text`) — used by `program_rdd_2_intermediate*.py` |
| [`products.csv`](products.csv) | csv (1.2KB) | 12 product descriptions (`product_id,product_name,raw_description`) — used by `program_df_3_intermediate_plus.py` |
| [`reviews.csv`](reviews.csv) | csv (667B) | 10 product reviews (`id,product_type,review_text`) — used by `program_df_1_basics.py` |
| [`tickets.csv`](tickets.csv) | csv (905B) | 12 support tickets (`id,email,ticket_text`) — used by `program_df_2_intermediate*.py` |
