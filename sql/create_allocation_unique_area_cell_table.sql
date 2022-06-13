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
       CASE
           WHEN aua.allocation_area_type_id = 1
           THEN seca.cell_id --TODO
           WHEN aua.allocation_area_type_id = 2
           THEN NULL --TODO need AutoGen_HybridToSimpleAreaMapper
           WHEN aua.allocation_area_type_id = 3
           THEN NULL --TODO need GetCellsForAreaType3
           ELSE NULL -- this should never happen
       END AS cell_id,
       CASE
           WHEN aua.allocation_area_type_id = 1
           THEN seca.water_area --TODO
           WHEN aua.allocation_area_type_id = 2
           THEN NULL --TODO need AutoGen_HybridToSimpleAreaMapper
           WHEN aua.allocation_area_type_id = 3
           THEN NULL --TODO need GetCellsForAreaType3
           ELSE NULL -- this should never happen
       END AS water_area
FROM sedna.allocation_unique_area aua
JOIN sedna.simple_area_cell_assignment seca ON (aua.generic_allocation_area_id = seca.allocation_simple_area_id);

--TODO this might work better as splitting on the allocation_area_type_id, doing 3 separate joins, then doing a UNION
