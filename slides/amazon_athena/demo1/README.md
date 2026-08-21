# demo1

First Amazon Athena ETL demo: Python ETL scripts and SQL schema for a continents/countries dataset.

## Contents

| Name | Type | Description |
|---|---|---|
| [`continents_countries.csv`](continents_countries.csv) | csv (1.3KB) | Sample `continent,country,city,sdata,ndata` records used as ETL input |
| [`etl.py`](etl.py) | py (599B) | Reads the CSV from S3, partitions by continent+country, and writes Parquet to S3 |
| [`etl2.py`](etl2.py) | py (615B) | Variant of `etl.py` that partitions by continent+country+city instead |
| [`schema.sql`](schema.sql) | sql (240B) | Athena `CREATE EXTERNAL TABLE` matching `etl.py`'s Parquet output (partitioned by continent, country) |
| [`schema2.sql`](schema2.sql) | sql (239B) | Athena `CREATE EXTERNAL TABLE` matching `etl2.py`'s Parquet output (partitioned by continent, country, city) |
