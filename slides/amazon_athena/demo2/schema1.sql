CREATE EXTERNAL TABLE `scu_continents_1`(
  `country` string,
  `city` string,
  `sdata` string,
  `ndata` bigint
)
PARTITIONED BY (
  `continent` string
)
STORED AS PARQUET
LOCATION 's3://mybucket/SCU/OUTPUT2/continents_countries1/'
tblproperties ("parquet.compress"="SNAPPY");
