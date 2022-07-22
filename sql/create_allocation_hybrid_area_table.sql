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
-- note that includeHighSeas is always true according to this call:
-- https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Process/ProcessAllocationHybridArea.cs#L40
-- TODO check for dupes? do we only need one of each combination? omit empty rows or no?
SELECT row_number() OVER () AS allocation_hybrid_area_id, *
FROM (
    -- ICES https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_ICES.cs
    SELECT hd.fao_area_id AS fao_area_id,
           15 AS marine_layer_id_1,
           array_sort(array_intersect((access_agreement_eezs || undeclared_eezs || ARRAY[0]), array_agg(eic.eez_id))) AS area_ids_1,
           2 AS marine_layer_id_2,
           ARRAY[hd.fao_area_id] AS area_ids_2,
           FALSE AS reassign_to_unknown_fishing_entity,
           hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
           hd.undeclared_eezs AS internal_audit_undeclared_eezs
    FROM hybrid_data hd
    JOIN eez_ices_combo eic ON (eic.ices_area_id = hd.ices_area AND eic.is_ifa = FALSE)
    WHERE hd.ices_area IS NOT NULL
      AND hd.fao_area_id = 27
    GROUP BY 1,2,4,5,6,7
    HAVING cardinality(array_intersect((access_agreement_eezs || undeclared_eezs), array_agg(eic.eez_id))) > 0
    UNION ALL
    -- TODO BigCell https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_BigCell.cs
    SELECT hd.fao_area_id AS fao_area_id,
           16 AS marine_layer_id_1,
           NULL AS area_ids_1,
           0 AS marine_layer_id_2,
           NULL AS area_ids_2,
           -- TODO if no access agreement EEZs assign to UnknownFishingEntity: https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_BigCell.cs#L21
           FALSE AS reassign_to_unknown_fishing_entity,
           hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
           hd.undeclared_eezs AS internal_audit_undeclared_eezs
    FROM hybrid_data hd
    WHERE hd.big_cell_id IS NOT NULL
    UNION ALL
    -- TODO CCAMLR https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_CCAMLR.cs
    SELECT 0 AS fao_area_id,
           0 AS marine_layer_id_1,
           NULL AS area_ids_1,
           0 AS marine_layer_id_2,
           NULL AS area_ids_2,
           FALSE AS reassign_to_unknown_fishing_entity,
           hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
           hd.undeclared_eezs AS internal_audit_undeclared_eezs
    FROM hybrid_data hd
    WHERE hd.ccamlr_area IS NOT NULL
    UNION ALL
    -- TODO NAFO https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_NAFO.cs
    SELECT 0 AS fao_area_id,
           0 AS marine_layer_id_1,
           NULL AS area_ids_1,
           0 AS marine_layer_id_2,
           NULL AS area_ids_2,
           FALSE AS reassign_to_unknown_fishing_entity,
           hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
           hd.undeclared_eezs AS internal_audit_undeclared_eezs
    FROM hybrid_data hd
    WHERE hd.nafo_division IS NOT NULL
    UNION ALL
    -- TODO other https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea.cs
    SELECT 0 AS fao_area_id,
           0 AS marine_layer_id_1,
           NULL AS area_ids_1,
           0 AS marine_layer_id_2,
           NULL AS area_ids_2,
           FALSE AS reassign_to_unknown_fishing_entity,
           hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
           hd.undeclared_eezs AS internal_audit_undeclared_eezs
    FROM hybrid_data hd
    WHERE hd.ices_area IS NULL
      AND hd.big_cell_id IS NULL
      AND hd.ccamlr_area IS NULL
      AND hd.nafo_division IS NULL
);
