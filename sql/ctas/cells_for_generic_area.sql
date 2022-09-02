-- cells for generic area table
-- https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L261
CREATE TABLE IF NOT EXISTS sedna.cells_for_generic_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.cells_for_generic_area',
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  partitioned_by = ARRAY['allocation_area_type_id', 'data_layer_id']
) AS WITH area_type_1_cells AS (
    SELECT saca.allocation_simple_area_id AS generic_area_id,
           1 AS allocation_area_type_id,
           saca.allocation_simple_area_id,
           saca.cell_id,
           saca.water_area
    FROM sedna.simple_area_cell_assignment saca
), area_type_2_cells AS (
    SELECT hsam.hybrid_area_id AS generic_area_id,
           2 AS allocation_area_type_id,
           saca.allocation_simple_area_id,
           saca.cell_id,
           saca.water_area
    FROM sedna.hybrid_to_simple_area_mapper hsam
    JOIN sedna.simple_area_cell_assignment saca
      ON (hsam.contains_simple_area_id = saca.allocation_simple_area_id)
), area_type_3_cells AS (
    SELECT cat3.depth_adjustment_function_area_id AS generic_area_id,
           3 AS allocation_area_type_id,
           cat3.allocation_simple_area_id,
           cat3.cell_id,
           cat3.water_area
    FROM sedna.cells_for_area_type_3 cat3
)
-- data layer 1
SELECT generic_area_id,
       allocation_simple_area_id,
       cell_id,
       water_area,
       -- partition columns at the end
       allocation_area_type_id,
       1 AS data_layer_id
FROM area_type_1_cells
UNION ALL
SELECT generic_area_id,
       allocation_simple_area_id,
       cell_id,
       water_area,
       -- partition columns at the end
       allocation_area_type_id,
       1 AS data_layer_id
FROM area_type_2_cells
UNION ALL
SELECT generic_area_id,
       allocation_simple_area_id,
       cell_id,
       water_area,
       -- partition columns at the end
       allocation_area_type_id,
       1 AS data_layer_id
FROM area_type_3_cells

--TODO data_layer_id 2 with coastal cells removed if not allowed via:
-- JOIN sedna.allocation_simple_area asa
--   ON (saca.allocation_simple_area_id = asa.allocation_simple_area_id)
-- WHERE asa.inherited_att_allows_coastal_fishing_for_layer2_data = 1
--    OR saca.cell_id NOT IN (SELECT cell_id FROM sedna.cell_is_coastal)

--TODO data_layer_id 3 (coastal cell removal is commented out in code)
