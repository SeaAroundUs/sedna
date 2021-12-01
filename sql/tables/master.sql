-- master.access_agreement
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.access_agreement (
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
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.access_agreement'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.catch_type
-- master.eez
-- master.fao_area
-- master.fishing_entity
-- master.functional_groups
-- master.gear
-- master.geo_entity
-- master.high_seas
-- master.input_type
-- master.lme
-- master.marine_layer
-- master.price
-- master.reporting_status
-- master.sector_type

-- master.taxon
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.taxon (
    taxon_key INT,
    scientific_name STRING,
    common_name STRING,
    commercial_group_id INT,
    functional_group_id INT,
    sl_max DOUBLE,
    tl DOUBLE,
    taxon_level_id INT,
    taxon_group_id INT,
    isscaap_id INT,
    lat_north INT,
    lat_south INT,
    min_depth INT,
    max_depth INT,
    loo DOUBLE,
    woo DOUBLE,
    k DOUBLE,
    has_habitat_index BOOLEAN,
    has_map BOOLEAN,
    is_baltic_only BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.taxon'
TBLPROPERTIES ('has_encrypted_data'='false');
