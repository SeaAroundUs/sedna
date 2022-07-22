-- allocation unique area cell table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L83
-- and https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L251
CREATE TABLE IF NOT EXISTS sedna.allocation_unique_area_cell
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.allocation_unique_area_cell',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT aua.unique_area_id,
          seca.allocation_simple_area_id,
          seca.cell_id,
          seca.water_area
   FROM sedna.allocation_unique_area aua
   JOIN sedna.simple_area_cell_assignment seca ON (aua.generic_allocation_area_id = seca.allocation_simple_area_id)
   WHERE aua.allocation_area_type_id = 1;

--TODO allocation_area_type_id = 2 and 3 in UNIONS, type 2 uses AutoGen_HybridToSimpleAreaMapper and type 3 uses GetCellsForAreaType3