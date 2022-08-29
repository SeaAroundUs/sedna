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
    FROM sedna.dataraw dr
    JOIN sedna.taxon tx ON (dr.taxon_key = tx.taxon_key)
    JOIN sedna.eez_fao_combo efc ON (dr.fao_area_id = efc.fao_area_id)
    JOIN sedna.access_agreement aa ON (
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
    FROM sedna.dataraw dr
    JOIN sedna.eez_fao_combo efc ON (dr.fao_area_id = efc.fao_area_id)
    JOIN sedna.eez e ON (
        e.eez_id = efc.reconstruction_eez_id
        AND e.is_currently_used_for_reconstruction = TRUE
        AND e.declaration_year > dr.year
        AND e.is_home_eez_of_fishing_entity_id != dr.fishing_entity_id
    )
    WHERE dr.area_type = 'Hybrid'
    GROUP BY dr.universal_data_id
), hybrid_data AS (
    SELECT DISTINCT dr.layer, -- dupe check here reduces table size by ~15x
        dr.fishing_entity_id,
        dr.fao_area_id,
        dr.year,
        dr.taxon_key,
        dr.ices_area,
        dr.big_cell_id,
        dr.ccamlr_area,
        dr.nafo_division,
        access_agreement_eezs,
        undeclared_eezs
    FROM sedna.dataraw dr
    LEFT JOIN access_agreement_eezs aae ON (aae.universal_data_id = dr.universal_data_id)
    LEFT JOIN undeclared_eezs ue ON (ue.universal_data_id = dr.universal_data_id)
    WHERE dr.area_type = 'Hybrid'
), hybrid_agg_data AS (
    -- note that includeHighSeas is always true according to this call:
    -- https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Process/ProcessAllocationHybridArea.cs#L40
    SELECT row_number() OVER () AS allocation_hybrid_area_id, *
    FROM (
        -- ICES https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_ICES.cs
        SELECT hd.fao_area_id AS fao_area_id,
               15 AS marine_layer_id_1,
               array_agg(eic.eez_id) AS agg_area_ids_1,
               2 AS marine_layer_id_2,
               ARRAY[hd.fao_area_id] AS area_ids_2,
               FALSE AS reassign_to_unknown_fishing_entity,
               hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
               hd.undeclared_eezs AS internal_audit_undeclared_eezs,
               hd.layer,
               hd.fishing_entity_id,
               hd.year,
               hd.taxon_key,
               hd.ices_area,
               hd.big_cell_id,
               hd.ccamlr_area,
               hd.nafo_division
        FROM hybrid_data hd
        JOIN sedna.eez_ices_combo eic ON (eic.ices_area_id = hd.ices_area AND eic.is_ifa = FALSE)
        WHERE hd.ices_area IS NOT NULL
          AND hd.fao_area_id = 27
        GROUP BY 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16
        UNION ALL
        -- BigCell https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_BigCell.cs
        SELECT hd.fao_area_id AS fao_area_id,
               16 AS marine_layer_id_1,
               array_agg(ebc.eez_id) AS agg_area_ids_1,
               2 AS marine_layer_id_2,
               ARRAY[hd.fao_area_id] AS area_ids_2,
               FALSE AS reassign_to_unknown_fishing_entity,
               hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
               hd.undeclared_eezs AS internal_audit_undeclared_eezs,
               hd.layer,
               hd.fishing_entity_id,
               hd.year,
               hd.taxon_key,
               hd.ices_area,
               hd.big_cell_id,
               hd.ccamlr_area,
               hd.nafo_division
        FROM hybrid_data hd
        JOIN sedna.eez_big_cell_combo ebc ON (ebc.big_cell_id = hd.big_cell_id AND ebc.is_ifa = FALSE)
        WHERE hd.big_cell_id IS NOT NULL
          AND cardinality(access_agreement_eezs || undeclared_eezs) > 0
        GROUP BY 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16
        UNION ALL
        -- BigCell (unknown fishing entity) https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_BigCell.cs
        -- if no access agreement EEZs assign to UnknownFishingEntity:
        -- https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_BigCell.cs#L21
        SELECT hd.fao_area_id AS fao_area_id,
               16 AS marine_layer_id_1,
               array_agg(ebc.eez_id) AS agg_area_ids_1,
               2 AS marine_layer_id_2,
               ARRAY[hd.fao_area_id] AS area_ids_2,
               TRUE AS reassign_to_unknown_fishing_entity,
               hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
               hd.undeclared_eezs AS internal_audit_undeclared_eezs,
               hd.layer,
               hd.fishing_entity_id,
               hd.year,
               hd.taxon_key,
               hd.ices_area,
               hd.big_cell_id,
               hd.ccamlr_area,
               hd.nafo_division
        FROM hybrid_data hd
        JOIN sedna.eez_big_cell_combo ebc ON (ebc.big_cell_id = hd.big_cell_id AND ebc.is_ifa = FALSE)
        WHERE hd.big_cell_id IS NOT NULL
          AND cardinality(access_agreement_eezs || undeclared_eezs) > 0
        GROUP BY 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16
        UNION ALL
        -- CCAMLR https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_CCAMLR.cs
        SELECT hd.fao_area_id AS fao_area_id,
               17 AS marine_layer_id_1,
               array_agg(ecc.eez_id) AS agg_area_ids_1,
               2 AS marine_layer_id_2,
               ARRAY[hd.fao_area_id] AS area_ids_2,
               FALSE AS reassign_to_unknown_fishing_entity,
               hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
               hd.undeclared_eezs AS internal_audit_undeclared_eezs,
               hd.layer,
               hd.fishing_entity_id,
               hd.year,
               hd.taxon_key,
               hd.ices_area,
               hd.big_cell_id,
               hd.ccamlr_area,
               hd.nafo_division
        FROM hybrid_data hd
        JOIN sedna.eez_ccamlr_combo ecc ON (ecc.ccamlr_area_id = hd.ccamlr_area AND ecc.is_ifa = FALSE)
        WHERE hd.ccamlr_area IS NOT NULL
          AND hd.fao_area_id IN (48,58,88)
        GROUP BY 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16
        UNION ALL
        -- NAFO https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea_NAFO.cs
        SELECT hd.fao_area_id AS fao_area_id,
               18 AS marine_layer_id_1,
               array_agg(enc.eez_id) AS agg_area_ids_1,
               2 AS marine_layer_id_2,
               ARRAY[hd.fao_area_id] AS area_ids_2,
               FALSE AS reassign_to_unknown_fishing_entity,
               hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
               hd.undeclared_eezs AS internal_audit_undeclared_eezs,
               hd.layer,
               hd.fishing_entity_id,
               hd.year,
               hd.taxon_key,
               hd.ices_area,
               hd.big_cell_id,
               hd.ccamlr_area,
               hd.nafo_division
        FROM hybrid_data hd
        JOIN sedna.eez_nafo_combo enc ON (enc.nafo_division = hd.nafo_division AND enc.is_ifa = FALSE)
        WHERE hd.nafo_division IS NOT NULL
          AND hd.fao_area_id = 21
        GROUP BY 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16
        UNION ALL
        -- other https://github.com/SeaAroundUs/MerlinCSharp/blob/master/Resolve999/step2/CreateAllocationHybridArea.cs
        SELECT hd.fao_area_id AS fao_area_id,
               12 AS marine_layer_id_1,
               ARRAY[] AS agg_area_ids_1,
               IF(hd.fao_area_id != 37,2,0) AS marine_layer_id_2,
               IF(hd.fao_area_id != 37,ARRAY[hd.fao_area_id],ARRAY[]) AS area_ids_2,
               FALSE AS reassign_to_unknown_fishing_entity,
               hd.access_agreement_eezs AS internal_audit_has_agreement_eezs,
               hd.undeclared_eezs AS internal_audit_undeclared_eezs,
               hd.layer,
               hd.fishing_entity_id,
               hd.year,
               hd.taxon_key,
               hd.ices_area,
               hd.big_cell_id,
               hd.ccamlr_area,
               hd.nafo_division
        FROM hybrid_data hd
        WHERE hd.ices_area IS NULL
          AND hd.big_cell_id IS NULL
          AND hd.ccamlr_area IS NULL
          AND hd.nafo_division IS NULL
    )
)
SELECT had.allocation_hybrid_area_id,
       had.fao_area_id,
       had.marine_layer_id_1,
       CASE
           WHEN had.marine_layer_id_1 = 16 AND had.reassign_to_unknown_fishing_entity = TRUE
           THEN array_join(array_sort(ARRAY[0] || had.agg_area_ids_1), ',')
           WHEN had.marine_layer_id_1 = 12
           THEN array_join(array_sort(internal_audit_has_agreement_eezs || internal_audit_undeclared_eezs), ',')
           ELSE array_join(array_sort(array_intersect((internal_audit_has_agreement_eezs || internal_audit_undeclared_eezs || ARRAY[0]), had.agg_area_ids_1)), ',')
       END AS area_ids_1,
       had.marine_layer_id_2,
       array_join(had.area_ids_2, ',') AS area_ids_2,
       had.reassign_to_unknown_fishing_entity,
       array_join(had.internal_audit_has_agreement_eezs, ',') AS internal_audit_has_agreement_eezs,
       array_join(had.internal_audit_undeclared_eezs, ',') AS internal_audit_undeclared_eezs,
       had.layer,
       had.fishing_entity_id,
       had.year,
       had.taxon_key,
       had.ices_area,
       had.big_cell_id,
       had.ccamlr_area,
       had.nafo_division
FROM hybrid_agg_data had;
