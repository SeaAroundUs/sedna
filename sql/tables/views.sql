-- views.v_internal_generate_allocation_simple_area_table
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.v_internal_generate_allocation_simple_area_table (
    marine_layer_id INT,
    area_id INT,
    fao_area_id DOUBLE,
    is_active INT,
    inherited_att_belongs_to_reconstruction_eez_id INT,
    inherited_att_is_ifa INT,
    inherited_att_allows_coastal_fishing_for_layer2_data INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/views.internal_generate_allocation_simple_area_table/';
