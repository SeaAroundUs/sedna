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
       0 AS marine_layer_id_2, -- default 0
       '' AS area_ids_2, -- default ''
       NULL AS internal_has_agreement_eezs,
       NULL AS internal_undeclared_eezs

--NAFO https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_NAFO.cs


-- use this CTE to power things:
--
SELECT dr.layer,
	dr.fishing_entity_id,
	dr.fao_area_id,
	dr.year,
	dr.ices_area,
	dr.big_cell_id,
	dr.ccamlr_area,
	dr.nafo_division,
	array_agg(DISTINCT aa.eez_id) as access_agreement_eezs
	-- TODO group undeclared eezs
FROM dataraw dr
JOIN taxon tx ON (dr.taxon_key = tx.taxon_key)
JOIN eez_fao_combo efc ON (dr.fao_area_id = efc.fao_area_id)
JOIN access_agreement aa ON (
	aa.fishing_entity_id = dr.fishing_entity_id
	AND aa.start_year <= dr.year
	AND aa.end_year >= dr.year
	AND (
		aa.functional_group_id IS NULL
		OR CONTAINS(SPLIT(aa.functional_group_id, ';'), CAST(tx.functional_group_id AS VARCHAR))
	)
	AND efc.reconstruction_eezid = aa.eez_id
)
-- TODO join undeclared EEZs
WHERE dr.area_type = 'Hybrid'
GROUP BY dr.layer,
	dr.fishing_entity_id,
	dr.fao_area_id,
	dr.year,
	dr.ices_area,
	dr.big_cell_id,
	dr.ccamlr_area,
	dr.nafo_division

-- create these against dataraw

-- data layers in marine_layer table

-- docs
-- https://github.com/SeaAroundUs/sau_manual/wiki/positions.DBA.overview.terminology#3-allocationhybridarea
-- step by step criteria in there
-- looks like includeHighSeas is always true according to this call:
-- https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Process/ProcessAllocationHybridArea.cs#L40

-- steps
-- 1. get access agreements
--      where a.FishingEntityID == fishingEntityID
--      && year >= a.StartYear
--      && year <= a.EndYear
--      && in FAO area
-- 2. get undeclared EEZs (only if isAllowedToFishPreEEZByDefault is true)
--      all EEZs in the FAO area WHERE dataraw year < declaration year of eez AND not in home EEZs
--
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