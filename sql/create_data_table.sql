-- data table shape: (from https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/IntegrateDataRaw/ImportDataRaw.cs#L71)
-- area assignment nonsense handled here:
-- https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Area/AssignGenericAreaIDToData.cs#L9
-- its a conditional on spatial type which is determined via this enum:
-- https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Factory/ResolveSpatialType.cs

--TODO maybe skip layer 3 for now as those seem super complicated? also no layer 3 in test data?

-- test query finished REALLY fast (3s) with 100_000 rows of dataraw (leaving Layer3, NAFO, ICES, and CCAMLR null)
-- test query of full dataraw 13_979_347 rows finished in just under a minute! (leaving Layer3, NAFO, ICES, and CCAMLR null)

SELECT universal_data_id,
       IF(eez_id = 999, 2, 1) AS allocation_area_type_id,
       CASE
           -- Layer 3
           WHEN eez_id = 999 -- OR (dr.BigCellID != null && dr.BigCellID > 0) --TODO where is this field coming from?
           THEN NULL --TODO https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Area/AssignGenericAreaIDToData.cs#L129
           -- NAFO
           WHEN fao_area_id = 21 AND nafo_division IS NOT NULL
           THEN (SELECT allocation_simple_area_id
                 FROM allocation_simple_area asa
                 WHERE asa.marine_layer_id = 18
                   AND asa.area_id = (SELECT eez_nafo_combo_id
                                      FROM eez_nafo_combo enc
                                      WHERE enc.eez_id = dr.eez_id
                                        AND enc.nafo_division = dr.nafo_division
                                        AND enc.is_ifa = (st.sector_type_id != 1))
                   AND asa.fao_area_id = dr.fao_area_id)
           -- CCAMLR
           WHEN fao_area_id IN (48, 58, 88) AND ccamlr_area IS NOT NULL
           THEN (SELECT allocation_simple_area_id
                 FROM allocation_simple_area asa
                 WHERE asa.marine_layer_id = 17
                   AND asa.area_id = (SELECT eez_ccamlar_combo_id
                                      FROM eez_ccamlr_combo ecc
                                      WHERE ecc.eez_id = dr.eez_id
                                        AND ecc.ccamlr_area_id = dr.ccamlr_area
                                        AND ecc.is_ifa = (st.sector_type_id != 1))
                   AND asa.fao_area_id = dr.fao_area_id)
           -- ICES
           WHEN fao_area_id = 27
           THEN (SELECT allocation_simple_area_id
                 FROM allocation_simple_area asa
                 WHERE asa.marine_layer_id = 15
                   AND asa.area_id = (SELECT eez_ices_combo_id
                                      FROM eez_ices_combo eic
                                      WHERE eic.eez_id = dr.eez_id
                                        AND eic.ices_area_id = dr.ices_area
                                        AND eic.is_ifa = (st.sector_type_id != 1))
                   AND asa.fao_area_id = dr.fao_area_id)
           -- High Seas
           WHEN eez_id = 0 AND fao_area_id > 0
           THEN (SELECT allocation_simple_area_id FROM allocation_simple_area asa
                 WHERE asa.marine_layer_id = 2
                   AND asa.area_id = dr.fao_area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           -- IFA
           WHEN eez_id > 0 AND fao_area_id > 0 AND sector_type_id != 1
           THEN (SELECT allocation_simple_area_id FROM allocation_simple_area asa
                 WHERE asa.marine_layer_id = 14
                   AND asa.area_id = dr.eez_id
                   AND asa.fao_area_id = dr.fao_area_id)
           -- EEZ
           WHEN eez_id > 0 AND fao_area_id > 0 AND sector_type_id = 1
           THEN (SELECT allocation_simple_area_id FROM allocation_simple_area asa
                 WHERE asa.marine_layer_id = 12
                   AND asa.area_id = dr.eez_id
                   AND asa.fao_area_id = dr.fao_area_id)
           -- this shouldn't happen; check for these
           ELSE NULL
       END AS generic_allocation_area_id,
       fishing_entity_id AS original_fishing_entity_id,
       fishing_entity_id AS fishing_entity_id, --TODO 213 if it needs to be reassigned to unknown
       amount AS catch_amount,
       catch_type_id,
       reporting_status_id,
       layer AS data_layer_id,
       gear_type_id,
       it.input_type_id,
       st.sector_type_id,
       taxon_key,
       year
FROM dataraw dr
JOIN input_type it ON (dr.input = it.name)
JOIN sector_type st ON (dr.sector = st.name);
