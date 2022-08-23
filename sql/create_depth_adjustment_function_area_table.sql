-- depth adjustment function area table
-- from
CREATE TABLE IF NOT EXISTS sedna.depth_adjustment_function_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.depth_adjustment_function_area',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT NULL --TODO

