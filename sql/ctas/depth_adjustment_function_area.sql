-- depth adjustment function area table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/tables.sql#L453
-- and https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L160
CREATE TABLE IF NOT EXISTS sedna.depth_adjustment_function_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.depth_adjustment_function_area',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS WITH distinct_area_taxon AS (
    SELECT DISTINCT d.generic_allocation_area_id AS allocation_simple_area_id,
                    d.taxon_key
    FROM sedna.predepth_data d
    JOIN sedna.depth_adjustment_function_eligible_rows er ON (d.universal_data_id = er.universal_data_id)
)
SELECT row_number() OVER () AS depth_adjustment_function_area_id,
       dat.allocation_simple_area_id,
       dafca.local_depth_adjustment_row_id,
       dat.taxon_key,
       dafca.ratio AS coverage_ratio,
       dafca.min_possible_row
FROM distinct_area_taxon dat
JOIN sedna.depth_adjustment_function_create_areas dafca
  ON (dat.taxon_key = dafca.taxon_key AND dat.allocation_simple_area_id = dafca.allocation_simple_area_id);
