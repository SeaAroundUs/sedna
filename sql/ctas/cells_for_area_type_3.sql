-- cells for area type 3 table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L213
CREATE TABLE IF NOT EXISTS sedna.cells_for_area_type_3
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.cells_for_area_type_3',
  format = 'PARQUET',
  write_compression = 'SNAPPY'
)
AS SELECT NULL --TODO

--TODO need DepthAdjustmentFunction_Area first
