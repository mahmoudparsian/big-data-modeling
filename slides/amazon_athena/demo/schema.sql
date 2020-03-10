CREATE EXTERNAL TABLE `continents1`(
  `city` string, 
  `data` string
) 
PARTITIONED BY ( 
  `continent` string, 
  `country`  string
)
STORED AS PARQUET
LOCATION 's3://mydevbucket/output/cc1/'
tblproperties ("parquet.compress"="SNAPPY");
