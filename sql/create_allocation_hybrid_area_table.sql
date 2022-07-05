-- allocation hybrid area table creation
-- from https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/MerlinGen.sql#L690
-- and https://github.com/SeaAroundUs/sau_manual/wiki/positions.DBA.overview.terminology#3-allocationhybridarea
CREATE TABLE IF NOT EXISTS sedna.allocation_hybrid_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.allocation_hybrid_area',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS WITH access_agreement_eezs AS (
    SELECT dr.universal_data_id, array_agg(DISTINCT aa.eez_id) as access_agreement_eezs
    FROM dataraw dr
    JOIN taxon tx ON (dr.taxon_key = tx.taxon_key)
    JOIN eez_fao_combo efc ON (dr.fao_area_id = efc.fao_area_id)
    JOIN access_agreement aa ON (
        aa.fishing_entity_id = dr.fishing_entity_id
        AND aa.eez_id = efc.reconstruction_eez_id
        AND aa.start_year <= dr.year
        AND aa.end_year >= dr.year
        AND (
            aa.functional_group_id IS NULL
            OR CONTAINS(SPLIT(aa.functional_group_id, ';'), CAST(tx.functional_group_id AS VARCHAR))
        )
    )
    WHERE dr.area_type = 'Hybrid'
    GROUP BY dr.universal_data_id
), undeclared_eezs AS (
    SELECT dr.universal_data_id, array_agg(DISTINCT e.eez_id ORDER BY e.eez_id) as undeclared_eezs
    FROM dataraw dr
    JOIN eez_fao_combo efc ON (dr.fao_area_id = efc.fao_area_id)
    JOIN eez e ON (
        e.eez_id = efc.reconstruction_eez_id
        AND e.is_currently_used_for_reconstruction = TRUE
        AND e.declaration_year > dr.year
        AND e.is_home_eez_of_fishing_entity_id != dr.fishing_entity_id
    )
    WHERE dr.area_type = 'Hybrid'
    GROUP BY dr.universal_data_id
), hybrid_data AS (
    SELECT dr.layer,
        dr.fishing_entity_id,
        dr.fao_area_id,
        dr.year,
        dr.ices_area,
        dr.big_cell_id,
        dr.ccamlr_area,
        dr.nafo_division,
        access_agreement_eezs,
        undeclared_eezs
    FROM dataraw dr
    LEFT JOIN access_agreement_eezs aae ON (aae.universal_data_id = dr.universal_data_id)
    LEFT JOIN undeclared_eezs ue ON (ue.universal_data_id = dr.universal_data_id)
    WHERE dr.area_type = 'Hybrid'
)
SELECT row_number() OVER () AS allocation_hybrid_area_id, *
FROM (
    -- TODO ICES https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_ICES.cs
    -- TODO BigCell https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_BigCell.cs
    -- TODO CCAMLR https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_CCAMLR.cs
    -- TODO NAFO https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_NAFO.cs
    -- TODO other https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea.cs
)

-- looks like includeHighSeas is always true according to this call:
-- https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Process/ProcessAllocationHybridArea.cs#L40

-- shape
-- 	[AllocationHybridAreaID] [int] IDENTITY(1,1) NOT NULL,
-- 	[FaoAreaID] [tinyint] NOT NULL,
-- 	[MarineLayerID1] [tinyint] NOT NULL,
-- 	[AreaIDs1] [nvarchar](255) NOT NULL,
-- 	[MarineLayerID2] [tinyint] NOT NULL,
-- 	[AreaIDs2] [nvarchar](255) NOT NULL,
-- 	[internalAudit_hasAgreementEEZs] [nvarchar](255) NOT NULL,
-- 	[internalAudit_unDeclaredEEZs] [nvarchar](255) NOT NULL,