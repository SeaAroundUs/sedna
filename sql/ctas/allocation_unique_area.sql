-- allocation unique area table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L74
CREATE TABLE IF NOT EXISTS sedna.allocation_unique_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.allocation_unique_area',
  format = 'PARQUET',
  write_compression = 'SNAPPY'
)
AS SELECT DISTINCT unique_area_id,
	data_layer_id,
	allocation_area_type_id,
	generic_allocation_area_id
FROM sedna.data
ORDER BY unique_area_id;
