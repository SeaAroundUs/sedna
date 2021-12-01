-- recon.catch
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.catch (
    id INT,
    fishing_entity_id INT,
    eez_sub_area STRING,
    subregional_area STRING,
    province_state STRING,
    ccamlr_area STRING,
    layer INT,
    original_sector STRING,
    year INT,
    amount DOUBLE,
    adjustment_factor DOUBLE,
    gear_type_id INT,
    input_type_id INT,
    forward_carry_rule_id INT,
    disaggregation_rule_id INT,
    layer_rule_id INT,
    notes STRING,
    catch_type_id INT,
    reporting_status_id INT,
    eez_id INT,
    fao_area_id INT,
    ices_area_id INT,
    nafo_division_id INT,
    original_country_fishing_id INT,
    original_fao_name_id INT,
    original_taxon_name_id INT,
    raw_catch_id INT,
    reference_id INT,
    sector_type_id INT,
    taxon_key INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/recon.catch'
TBLPROPERTIES ('has_encrypted_data'='false');

-- recon.data_raw_layer3
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.data_raw_layer3 (
    row_id INT,
    rfmo_id INT,
    year INT,
    fishing_entity_id INT,
    layer3_gear_id INT,
    taxon_key INT,
    big_cell_id INT,
    catch DOUBLE,
    catch_type_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/recon.data_raw_layer3'
TBLPROPERTIES ('has_encrypted_data'='false');
