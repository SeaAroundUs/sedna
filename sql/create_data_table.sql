-- data table creation
-- from https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/IntegrateDataRaw/ImportDataRaw.cs#L71
-- (had to move some of the area_id logic to the dataraw table due to nested sub-queries)
CREATE TABLE IF NOT EXISTS sedna.data
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.data',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT universal_data_id,
       IF(eez_id = 999, 2, 1) AS allocation_area_type_id,
       dr.area_type,
       CASE
           WHEN dr.area_type = 'Layer3'
           THEN NULL --TODO https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Area/AssignGenericAreaIDToData.cs#L129
           WHEN dr.area_type = 'NAFO'
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 18 -- TODO this doesn't seem to exist in the allocation_simple_area table
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           WHEN dr.area_type = 'CCAMLR'
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 17
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           WHEN dr.area_type = 'ICES'
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 15
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           WHEN dr.area_type = 'High Seas'
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 2
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           WHEN dr.area_type = 'IFA'
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 14
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           WHEN dr.area_type = 'EEZ'
           THEN (SELECT allocation_simple_area_id
                 FROM sedna.allocation_simple_area asa
                 WHERE asa.marine_layer_id = 12
                   AND asa.area_id = dr.area_id
                   AND asa.fao_area_id = dr.fao_area_id)
           ELSE NULL -- this shouldn't happen; check for these
       END AS generic_allocation_area_id,
       fishing_entity_id AS original_fishing_entity_id,
       -- per https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Resolve999/step2/CreateAllocationHybridArea.cs#L17
       -- fishing_entity_id is assigned to 213 only for layer 3 when there are no access agreements or un-declared EEzs or high seas
       fishing_entity_id AS fishing_entity_id,
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
