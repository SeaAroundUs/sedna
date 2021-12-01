-- distribution.taxon_distribution
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.taxon_distribution (
    taxon_distribution_id INT,
    taxon_key INT,
    cell_id INT,
    relative_abundance DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/distribution.taxon_distribution'
TBLPROPERTIES ('has_encrypted_data'='false');

-- distribution.taxon_distribution_substitute
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.taxon_distribution_substitute (
    original_taxon_key INT,
    use_this_taxon_key_instead INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/distribution.taxon_distribution_substitute'
TBLPROPERTIES ('has_encrypted_data'='false');
