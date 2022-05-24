-- allocation unique area cell table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L83
-- and https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L251
CREATE TABLE IF NOT EXISTS sedna.allocation_unique_area_cell
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.allocation_unique_area_cell',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT unique_area_id,
       allocation_simple_area_id,
       NULL AS cell_id, --TODO
       0.0 AS water_area --TODO
FROM sedna.allocation_unique_area;

--TODO needs SimpleAreaCellAssignment first