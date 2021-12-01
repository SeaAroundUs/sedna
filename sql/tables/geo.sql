-- geo.big_cell
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.big_cell (
    big_cell_id INT,
    big_cell_type_id INT,
    x DOUBLE,
    y DOUBLE,
    is_land_locked BOOLEAN,
    is_in_med BOOLEAN,
    is_in_pacific BOOLEAN,
    is_in_indian BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.big_cell'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.big_cell_type
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.big_cell_type (
    big_cell_type_id INT,
    type_desc STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.big_cell_type'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.cell
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.cell (
    cell_id INT,
    total_area DOUBLE,
    water_area DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.cell'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.cell_is_coastal
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.cell_is_coastal (
    cell_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.cell_is_coastal'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.depth_adjustment_row_cell
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.depth_adjustment_row_cell (
    local_depth_adjustment_row_id INT,
    eez_id INT,
    cell_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.depth_adjustment_row_cell'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.eez_big_cell_combo
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.eez_big_cell_combo (
    eez_big_cell_combo_id INT,
    eez_id INT,
    fao_area_id INT,
    big_cell_id INT,
    is_ifa BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.eez_big_cell_combo'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.eez_ccamlr_combo
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.eez_ccamlr_combo (
    eez_ccamlar_combo_id INT,
    eez_id INT,
    fao_area_id INT,
    ccamlr_area_id STRING,
    is_ifa BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.eez_ccamlr_combo'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.eez_fao_combo
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.eez_fao_combo (
    eez_fao_area_id INT,
    reconstruction_eez_id INT,
    fao_area_id INT,
    socio_economic_area_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.eez_fao_combo'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.eez_ices_combo
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.eez_ices_combo (
    eez_ices_combo_id INT,
    eez_id INT,
    fao_area_id INT,
    ices_area_id STRING,
    is_ifa BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.eez_ices_combo'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.eez_nafo_combo
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.eez_nafo_combo (
    eez_nafo_combo_id INT,
    eez_id INT,
    fao_area_id INT,
    nafo_division STRING,
    is_ifa BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.eez_nafo_combo'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.fao_cell
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.fao_cell (
    fao_area_id INT,
    cell_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.fao_cell'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.fao_map
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.fao_map (
    fao_area_id INT,
    upper_left_cell_cell_id INT,
    scale INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.fao_map'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.ifa_fao
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.ifa_fao (
    eez_id INT,
    ifa_is_located_in_this_fao INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.ifa_fao'
TBLPROPERTIES ('has_encrypted_data'='false');

-- geo.simple_area_cell_assignment_raw
CREATE EXTERNAL TABLE IF NOT EXISTS sedna.simple_area_cell_assignment_raw (
    id INT,
    marine_layer_id INT,
    area_id INT,
    fao_area_id INT,
    cell_id INT,
    water_area DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://sedna-catshark-storage/sedna-export/sedna-sau-int-export/seaaroundus/geo.simple_area_cell_assignment_raw'
TBLPROPERTIES ('has_encrypted_data'='false');
