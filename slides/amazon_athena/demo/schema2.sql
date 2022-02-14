CREATE EXTERNAL TABLE `continents2`(
  `data` string
) 
PARTITIONED BY ( 
  `continent` string, 
  `country`  string,
  `city` string
)
STORED AS PARQUET
LOCATION 's3://mydevbucket/output/cc2/'
tblproperties ("parquet.compress"="SNAPPY");
