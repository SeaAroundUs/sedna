-- data table creation
-- from https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/IntegrateDataRaw/ImportDataRaw.cs#L71
-- (had to move some of the area_id logic to the dataraw table due to nested sub-queries)
SELECT universal_data_id,
       IF(eez_id = 999, 2, 1) AS allocation_area_type_id,
       CASE
           -- Layer 3
           WHEN eez_id = 999 -- OR (dr.BigCellID != null && dr.BigCellID > 0) --TODO where is this field coming from?
           THEN NULL --TODO https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Area/AssignGenericAreaIDToData.cs#L129
           -- NAFO
           WHEN fao_area_id = 21 AND nafo_division IS NOT NULL
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 18
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           -- CCAMLR
           WHEN fao_area_id IN (48, 58, 88) AND ccamlr_area IS NOT NULL
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 17
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           -- ICES
           WHEN fao_area_id = 27
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 15
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           -- High Seas
           WHEN eez_id = 0 AND fao_area_id > 0
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 2
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           -- IFA
           WHEN dr.area_type = 'IFA'
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 14
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           -- EEZ
           WHEN dr.area_type = 'EEZ'
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 12
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           ELSE NULL -- this shouldn't happen; check for these
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
FROM sedna.dataraw dr
JOIN sedna.input_type it ON (dr.input = it.name)
JOIN sedna.sector_type st ON (dr.sector = st.name);
