-- depth adjustment function create areas table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L36
CREATE TABLE IF NOT EXISTS sedna.depth_adjustment_function_create_areas
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.depth_adjustment_function_create_areas',
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  bucketed_by = ARRAY['allocation_simple_area_id', 'taxon_key'],
  bucket_count = 50
)
AS WITH possible_combos_cumulative AS (
    SELECT p1.allocation_simple_area_id,
           p1.local_depth_adjustment_row_id,
           p1.taxon_key,
           p1.water_area,
           (SELECT SUM(p2.water_area)
            FROM sedna.depth_adjustment_function_area_possible_combos p2
            WHERE p1.allocation_simple_area_id = p2.allocation_simple_area_id
              AND p1.taxon_key = p2.taxon_key
              AND p1.local_depth_adjustment_row_id >= p2.local_depth_adjustment_row_id
           ) AS cumulative_water_area
    FROM sedna.depth_adjustment_function_area_possible_combos p1
), possible_combo_aggregates AS (
    SELECT pc.allocation_simple_area_id,
           pc.taxon_key,
           SUM(pc.water_area) AS total_water_area,
           MIN(pc.local_depth_adjustment_row_id) AS min_possible_row
    FROM sedna.depth_adjustment_function_area_possible_combos pc
    GROUP BY 1,2
)
SELECT pcc.allocation_simple_area_id,
       pcc.local_depth_adjustment_row_id,
       pcc.taxon_key,
       pcc.cumulative_water_area / pca.total_water_area AS ratio,
       pca.min_possible_row
    FROM possible_combos_cumulative pcc
    JOIN possible_combo_aggregates pca
      ON (pcc.allocation_simple_area_id = pca.allocation_simple_area_id AND
          pcc.taxon_key = pca.taxon_key);
