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
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.catch_type (
    catch_type_id INT,
    name STRING,
    abbreviation STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.catch_type'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.eez
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.eez (
    eez_id INT,
    name STRING,
    alternate_name STRING,
    geo_entity_id INT,
    area_status_id INT,
    legacy_c_number INT,
    legacy_count_code STRING,
    fishbase_id STRING,
    coords STRING,
    can_be_displayed_on_web BOOLEAN,
    is_currently_used_for_web BOOLEAN,
    is_currently_used_for_reconstruction BOOLEAN,
    declaration_year INT,
    earliest_access_agreement_date INT,
    is_home_eez_of_fishing_entity_id INT,
    allows_coastal_fishing_for_layer2_data BOOLEAN,
    ohi_link STRING,
    is_retired BOOLEAN,
    gsi_link STRING,
    issf_link STRING,
    hdi_link STRING,
    iso_3 STRING,
    iso_2 STRING,
    hdi STRING,
    hdi_source STRING,
    hdi_publication_year INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.eez'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.fao_area
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.fao_area (
    fao_area_id INT,
    name STRING,
    alternate_name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.fao_area'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.fishing_entity
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.fishing_entity (
    fishing_entity_id INT,
    name STRING,
    geo_entity_id INT,
    date_allowed_to_fish_other_eezs INT,
    date_allowed_to_fish_high_seas INT,
    legacy_c_number INT,
    is_currently_used_for_web BOOLEAN,
    is_currently_used_for_reconstruction BOOLEAN,
    is_allowed_to_fish_pre_eez_by_default BOOLEAN,
    remarks STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.fishing_entity'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.functional_groups
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.functional_groups (
    functional_group_id INT,
    target_grp INT,
    name STRING,
    description STRING,
    include_in_depth_adjustment_function BOOLEAN,
    size_range STRING,
    fgi_block STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.functional_groups'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.gear
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.gear (
    gear_id INT,
    name STRING,
    super_code STRING,
    notes STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.gear'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.geo_entity
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.geo_entity (
    geo_entity_id INT,
    name STRING,
    admin_geo_entity_id INT,
    jurisdiction_id INT,
    started_eez_at STRING,
    legacy_c_number INT,
    legacy_admin_c_number INT,
    continent_code STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.geo_entity'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.high_seas
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.high_seas (
    fao_area_id INT,
    name STRING,
    alternate_name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.high_seas'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.input_type
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.input_type (
    input_type_id INT,
    name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.input_type'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.lme
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.lme (
    lme_id INT,
    name STRING,
    profile_url STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.lme'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.marine_layer
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.marine_layer (
    marine_layer_id INT,
    remarks STRING,
    name STRING,
    bread_crumb_name STRING,
    show_sub_areas BOOLEAN,
    last_report_year INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.marine_layer'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.price
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.price (
    fishing_entity_id INT,
    year INT,
    taxon_key INT,
    end_use_type_id INT,
    price DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.price'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.reporting_status
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.reporting_status (
    reporting_status_id INT,
    name STRING,
    abbreviation STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.reporting_status'
TBLPROPERTIES ('has_encrypted_data'='false');

-- master.sector_type
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.sector_type (
    sector_type_id INT,
    name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.sector_type'
TBLPROPERTIES ('has_encrypted_data'='false');

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

-- master.time
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.time (
    time_key INT,
    year INT,
    eez_id INT,
    is_used_for_allocation BOOLEAN,
    is_used_for_web BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/master.time'
TBLPROPERTIES ('has_encrypted_data'='false');
