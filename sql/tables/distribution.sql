-- distribution.taxon_distribution
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.taxon_distribution (
    id INT,
    fishing_entity_id INT,
    eez_id INT,
    title_of_agreement STRING,
    access_category STRING,
    access_type_id INT,
    agreement_type_id INT,
    start_year INT,
    end_year INT,
    functional_group_id STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/distribution.taxon_distribution'
TBLPROPERTIES ('has_encrypted_data'='false');

-- distribution.taxon_distribution_substitute
