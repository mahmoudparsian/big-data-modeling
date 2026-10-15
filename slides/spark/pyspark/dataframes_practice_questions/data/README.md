# Data for the DataFrame Practice Questions

Small input files for the DataFrame practice questions
([Set 01](../QUESTIONS_01.md), [Set 02](../QUESTIONS_02.md), [Set 03](../QUESTIONS_03.md), [Set 04](../QUESTIONS_04.md)).

| File | Used in | Format |
|---|---|---|
| [trips.csv](trips.csv) | Set 01 Q4, Q5, Q7, Q9; Sets 02–04 | CSV with a header: `trip_id,driver_id,rider,city,hour,distance_km,fare,rating`, ride-sharing trips; an empty rating means "not rated" (20 rows). The same trips as the RDD sets' [`trips.txt`](../../rdd_practice_questions/data/trips.txt) |
| [trips_no_header.csv](trips_no_header.csv) | Set 01 Q4 | the same 20 trips as `trips.csv`, **without** the header line |
| [trips_raw.csv](trips_raw.csv) | Set 01 Q8 | the 20 trips plus 2 bad rows (a non-numeric hour, and a row with too few fields) |
| [drivers.csv](drivers.csv) | Set 02 Q5 | CSV with a header: `driver_id,name,car_type` (5 rows; one driver has no trips) |
| [riders.json](riders.json) | Set 01 Q6; Set 02 Q5, Q7; Set 03 Q4, Q5 | JSON Lines: one rider per line, with a nested `home` object (`city`, `zip`) and a `tags` list (7 riders; one with an empty list, one with no `tags` at all) |
| [payments.csv](payments.csv) | Set 02 Q5, Q6, Q9 | CSV with a header: `payment_id,trip_id,paid_at,method,amount`, one payment per trip; the amount is text like `$17.40` (20 rows) |
| [payments_raw.csv](payments_raw.csv) | Set 02 Q9 | the same payments with messy data: two date formats, inconsistent method names, and amounts like `USD 42.08`, `$ 33.60`, `N/A`, and an empty one |
| [trips_dirty.csv](trips_dirty.csv) | Set 03 Q9 | the 20 trips plus 6 bad rows (a non-numeric hour, too few fields, an unknown city, a negative distance, a missing fare, a rating of 7) and an exact copy of `t05` (27 rows) |
