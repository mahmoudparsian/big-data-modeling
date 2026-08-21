# demo2

Second Amazon Athena ETL demo: extends demo1 by partitioning the output as Parquet (see `output_as_parquet/`).

## Contents

| Name | Type | Description |
|---|---|---|
| [`output_as_parquet/`](output_as_parquet/) | folder | 8 items |
| [`continents_countries.csv`](continents_countries.csv) | csv (1.3KB) | Sample `continent,country,city,sdata,ndata` records used as ETL input |
| [`continents_countries_temp.csv`](continents_countries_temp.csv) | csv (1.9KB) | Sample `continent,country,city,temperature` records — the dataset used to produce `output_as_parquet/` |
| [`etl0.py`](etl0.py) | py (815B) | Reads a CSV (input/output paths as CLI args), partitions by continent, writes Parquet — used to build `output_as_parquet/` |
| [`etl1.py`](etl1.py) | py (674B) | Reads the CSV from S3, partitions by continent only, writes Parquet to S3 |
| [`etl2.py`](etl2.py) | py (695B) | Variant of `etl1.py` that partitions by continent+country instead |
| [`extract_and_load.sh`](extract_and_load.sh) | sh (2.1KB) | Runs `etl0.py` via `spark-submit` with the EMRFS/AWS jars on the classpath |
| [`schema1.sql`](schema1.sql) | sql (279B) | Athena `CREATE EXTERNAL TABLE` matching `etl1.py`'s Parquet output (partitioned by continent) |
| [`schema2.sql`](schema2.sql) | sql (279B) | Athena `CREATE EXTERNAL TABLE` matching `etl2.py`'s Parquet output (partitioned by continent, country) |
