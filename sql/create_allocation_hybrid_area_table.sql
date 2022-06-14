-- allocation hybrid area table creation
-- from https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/MerlinGen.sql#L690
-- and https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Resolve999/step2/CreateAllocationHybridArea.cs

CREATE TABLE IF NOT EXISTS sedna.allocation_hybrid_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.allocation_hybrid_area',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT
       row_number() OVER () AS allocation_hybrid_area_id,
       NULL AS fao_area_id,
       NULL AS marine_layer_id_1,
       NULL AS area_ids_1,
       NULL AS marine_layer_id_2,
       NULL AS area_ids_2,
       NULL AS has_agreement_eezs,
       NULL AS undeclared_eezs

-- create these against dataraw

-- docs
-- https://github.com/SeaAroundUs/sau_manual/wiki/positions.DBA.overview.terminology#3-allocationhybridarea

-- steps
-- 1. get access agreements
-- 2. get undeclared EEZs
-- 3. this it think? https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Factory/Factory.cs#L34

-- shape
-- 	[AllocationHybridAreaID] [int] IDENTITY(1,1) NOT NULL,
-- 	[FaoAreaID] [tinyint] NOT NULL,
-- 	[MarineLayerID1] [tinyint] NOT NULL,
-- 	[AreaIDs1] [nvarchar](255) NOT NULL,
-- 	[MarineLayerID2] [tinyint] NOT NULL,
-- 	[AreaIDs2] [nvarchar](255) NOT NULL,
-- 	[internalAudit_hasAgreementEEZs] [nvarchar](255) NOT NULL,
-- 	[internalAudit_unDeclaredEEZs] [nvarchar](255) NOT NULL,