# Data for the RDD Practice Questions

Small text input files for the RDD practice questions
([Set 01](../QUESTIONS_01.md), [Set 02](../QUESTIONS_02.md), [Set 03](../QUESTIONS_03.md),
[Set 04](../QUESTIONS_04.md), [Set 05](../QUESTIONS_05.md)).

| File | Used in | Format |
|---|---|---|
| [sales.txt](sales.txt) | Set 01 Q4 | `store,product,price` (10 lines) |
| [movie_ratings.txt](movie_ratings.txt) | Set 01 Q6 | `user,movie,rating` (12 lines) |
| [web_logs.txt](web_logs.txt) | Set 01 Q7, Set 02 Q7, Set 04 Q6 | `date,user,url,http_status` (14 lines) |
| [temperatures.txt](temperatures.txt) | Set 01 Q8 | `city,date,temp_c` (14 lines, including a header, an empty line, an `NA` value, and a record with a missing field, all on purpose) |
| [purchases.txt](purchases.txt) | Set 01 Q9, Set 05 Q9 | `customer,category,product` (15 lines) |
| [quotes.txt](quotes.txt) | Set 02 Q4, Set 03 Q4 | plain text sentences, with mixed case and punctuation (5 lines) |
| [transactions.txt](transactions.txt) | Set 02 Q5, Set 03 Q5 | `customer,amount` (12 lines) |
| [enrollments.txt](enrollments.txt) | Set 02 Q6 | `student:course1,course2,...` (6 lines) |
| [documents.txt](documents.txt) | Set 02 Q8 | `doc_id\|text` (4 lines) |
| [class_scores.txt](class_scores.txt) | Set 02 Q9, Set 04 Q8 | `class,student,score` (12 lines) |
| [customers.csv](customers.csv) | Set 03 Q6 | `id,name,city,balance`, a real CSV file with a header and quoted fields that contain commas (7 lines) |
| [sensors.txt](sensors.txt) | Set 04 Q4, Q5 | `sensor_id,temperature` (12 lines) |
| [shop_customers.txt](shop_customers.txt) | Set 05 Q4, Q5, Q8 | `customer_id,name,city` (5 lines) |
| [shop_orders.txt](shop_orders.txt) | Set 05 Q4, Q5, Q8 | `order_id,customer_id,product_id,quantity` (10 lines, including one order for an unknown customer and one for an unknown product, on purpose) |
| [shop_products.txt](shop_products.txt) | Set 05 Q8 | `product_id,name,category,price` (5 lines) |
