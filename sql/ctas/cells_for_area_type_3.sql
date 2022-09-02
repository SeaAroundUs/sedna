-- cells for area type 3 table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L213
CREATE TABLE IF NOT EXISTS sedna.cells_for_area_type_3
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.cells_for_area_type_3',
  format = 'PARQUET',
  write_compression = 'SNAPPY'
)
AS SELECT dafa.depth_adjustment_function_area_id,
          saca.allocation_simple_area_id,
          saca.cell_id,
          saca.water_area
FROM sedna.simple_area_cell_assignment saca
JOIN sedna.depth_adjustment_function_area dafa
  ON (saca.allocation_simple_area_id = dafa.allocation_simple_area_id)
JOIN sedna.allocation_simple_area asa
  ON (saca.allocation_simple_area_id = asa.allocation_simple_area_id)
JOIN sedna.depth_adjustment_row_cell darc
  ON (asa.inherited_att_belongs_to_reconstruction_eez_id = darc.eez_id AND
      saca.cell_id = darc.cell_id AND
      darc.local_depth_adjustment_row_id >= dafa.min_possible_row AND
      darc.local_depth_adjustment_row_id <= dafa.local_depth_adjustment_row_id);
