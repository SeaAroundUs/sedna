-- simple area cell assignment table creation
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L1920
-- and https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/views.sql#L8
CREATE TABLE IF NOT EXISTS sedna.simple_area_cell_assignment
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.simple_area_cell_assignment',
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  bucketed_by = ARRAY['allocation_simple_area_id'],
  bucket_count = 75
)
AS SELECT
    row_number() OVER () AS row_id,
    asa.allocation_simple_area_id, --TODO only return one of these?
    cell_id,
    water_area
FROM sedna.simple_area_cell_assignment_raw sacar
JOIN sedna.allocation_simple_area asa ON (
    sacar.marine_layer_id = asa.marine_layer_id AND
    sacar.area_id = asa.area_id AND
    sacar.fao_area_id = asa.fao_area_id AND
    asa.active = 1
);
