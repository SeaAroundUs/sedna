-- depth adjustment function area possible combos table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L36
CREATE TABLE IF NOT EXISTS sedna.depth_adjustment_function_area_possible_combos
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.depth_adjustment_function_area_possible_combos',
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  bucketed_by = ARRAY['taxon_key', 'local_depth_adjustment_row_id'],
  bucket_count = 50
)
AS SELECT DISTINCT saca.allocation_simple_area_id,
                   darc.local_depth_adjustment_row_id,
                   td.taxon_key,
                   SUM(saca.water_area) AS water_area
FROM sedna.simple_area_cell_assignment saca
JOIN sedna.allocation_simple_area asa
  ON (saca.allocation_simple_area_id = asa.allocation_simple_area_id)
JOIN sedna.depth_adjustment_row_cell darc
  ON (asa.inherited_att_belongs_to_reconstruction_eez_id = darc.eez_id AND
      saca.cell_id = darc.cell_id)
JOIN sedna.taxon_distribution td ON (saca.cell_id = td.cell_id)
GROUP BY 1,2,3;
