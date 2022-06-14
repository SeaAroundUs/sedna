-- hybrid to simple area mapper table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/tables.sql#L183
-- and https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Resolve999/step4/HybridToSimpleAreaMapper.cs
CREATE TABLE IF NOT EXISTS sedna.hybrid_to_simple_mapper
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.hybrid_to_simple_mapper',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT
    row_number() OVER () AS row_id,
    NULL AS hybrid_area_id,
    NULL AS contains_allocation_simple_area_id

--TODO need allocation_hybrid_area first

-- shape
-- 	[RowID] [int] IDENTITY(1,1) NOT NULL,
-- 	[HybridAreaID] [int] NOT NULL,
-- 	[ContainsAllocationSimpleAreaID] [int] NOT NULL,
--  CONSTRAINT [PK_AutoGen_HybridToSimpleAreaMapping] PRIMARY KEY CLUSTERED
