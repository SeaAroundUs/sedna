-- allocation.allocation_area_type
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.allocation_area_type (
    allocation_area_type_id INT,
    name STRING,
    remarks STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/allocation.allocation_area_type'
TBLPROPERTIES ('has_encrypted_data'='false');

-- allocation.layer
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.layer (
    layer_id INT,
    name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/allocation.layer'
TBLPROPERTIES ('has_encrypted_data'='false');
