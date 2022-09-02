-- allocation unique area cell table
-- https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L83
CREATE TABLE IF NOT EXISTS sedna.allocation_unique_area_cell
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.allocation_unique_area_cell',
  format = 'PARQUET',
  write_compression = 'SNAPPY'
) AS SELECT aua.unique_area_id,
            cga.allocation_simple_area_id,
            cga.cell_id,
            cga.water_area
FROM sedna.allocation_unique_area aua
JOIN sedna.cells_for_generic_area cga
  ON (aua.generic_allocation_area_id = cga.generic_area_id AND
      aua.allocation_area_type_id = cga.allocation_area_type_id AND
      aua.data_layer_id = cga.data_layer_id);
