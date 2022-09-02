-- cells for generic area table
-- https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L261
CREATE TABLE IF NOT EXISTS sedna.cells_for_generic_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.cells_for_generic_area',
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  partitioned_by = ARRAY['allocation_area_type_id'],
  bucketed_by = ARRAY['generic_area_id'],
  bucket_count = 25
)
AS SELECT saca.allocation_simple_area_id AS generic_area_id,
          saca.allocation_simple_area_id,
          saca.cell_id,
          saca.water_area,
          -- partition columns at the end
          1 AS allocation_area_type_id
FROM sedna.simple_area_cell_assignment saca
UNION ALL
SELECT hsam.hybrid_area_id AS generic_area_id,
       saca.allocation_simple_area_id,
       saca.cell_id,
       saca.water_area,
       -- partition columns at the end
       2 AS allocation_area_type_id
FROM sedna.hybrid_to_simple_area_mapper hsam
JOIN sedna.simple_area_cell_assignment saca
  ON (hsam.contains_simple_area_id = saca.allocation_simple_area_id)
UNION ALL
SELECT cat3.depth_adjustment_function_area_id AS generic_area_id,
       cat3.allocation_simple_area_id,
       cat3.cell_id,
       cat3.water_area,
       -- partition columns at the end
       3 AS allocation_area_type_id
FROM sedna.cells_for_area_type_3 cat3;
