CREATE EXTERNAL TABLE `scu_continents_2`(
  `city` string,
  `sdata` string,
  `ndata` bigint
)
PARTITIONED BY (
  `continent` string,
  `country` string
)
STORED AS PARQUET
LOCATION 's3://mybucket/SCU/OUTPUT2/continents_countries2/'
tblproperties ("parquet.compress"="SNAPPY");
