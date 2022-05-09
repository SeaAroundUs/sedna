-- area table creation
-- from https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/MerlinGen.sql#L375
CREATE TABLE IF NOT EXISTS sedna.area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.area',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT row_number() OVER () AS allocation_simple_area_id, * FROM (
    -- EEZs
    SELECT 12 AS marine_layer_id,
           reconstruction_eez_id AS area_id,
           fao_area_id,
           1 AS active,
           reconstruction_eez_id AS inherited_att_belongs_to_reconstruction_eez_id,
           0 AS inhertied_att_is_ifa,
           CAST(allows_coastal_fishing_for_layer2_data AS INT) AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.eez_fao_combo
    JOIN sedna.eez ON (eez.eez_id = eez_fao_combo.reconstruction_eez_id)
    WHERE is_currently_used_for_reconstruction = TRUE
    UNION ALL
    -- IFAs
    SELECT 14 AS marine_layer_id,
           ifa_fao.eez_id AS area_id,
           ifa_is_located_in_this_fao AS fao_area_id,
           1 AS active,
           ifa_fao.eez_id AS inherited_att_belongs_to_reconstruction_eez_id,
           1 AS inhertied_att_is_ifa,
           0 AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.ifa_fao
    JOIN sedna.eez ON (eez.eez_id = ifa_fao.eez_id)
    WHERE is_currently_used_for_reconstruction = TRUE
    UNION ALL
    -- High Seas
    SELECT 2 AS marine_layer_id,
           fao_area_id AS area_id,
           fao_area_id AS fao_area_id,
           1 AS active,
           0 AS inherited_att_belongs_to_reconstruction_eez_id,
           0 AS inhertied_att_is_ifa,
           1 AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.high_seas
    UNION ALL
    -- ICES (High Seas)
    SELECT 15 AS marine_layer_id,
           eez_ices_combo_id AS area_id,
           fao_area_id AS fao_area_id,
           1 AS active,
           0 AS inherited_att_belongs_to_reconstruction_eez_id,
           0 AS inhertied_att_is_ifa,
           1 AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.eez_ices_combo
    WHERE eez_id = 0
    UNION ALL
    -- ICES (IFAs)
    SELECT 15 AS marine_layer_id,
           eez_ices_combo_id AS area_id,
           fao_area_id AS fao_area_id,
           1 AS active,
           eez_ices_combo.eez_id AS inherited_att_belongs_to_reconstruction_eez_id,
           1 AS inhertied_att_is_ifa,
           0 AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.eez_ices_combo
    JOIN sedna.eez ON (eez_ices_combo.eez_id = eez.eez_id)
    WHERE is_ifa = TRUE AND is_currently_used_for_reconstruction = TRUE
    UNION ALL
    -- ICES (EEZs)
    SELECT 15 AS marine_layer_id,
           eez_ices_combo_id AS area_id,
           fao_area_id AS fao_area_id,
           1 AS active,
           eez_ices_combo.eez_id AS inherited_att_belongs_to_reconstruction_eez_id,
           0 AS inhertied_att_is_ifa,
           CAST(allows_coastal_fishing_for_layer2_data AS INT) AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.eez_ices_combo
    JOIN sedna.eez ON (eez_ices_combo.eez_id = eez.eez_id)
    WHERE is_ifa = FALSE AND eez_ices_combo.eez_id > 0 AND is_currently_used_for_reconstruction = TRUE
    UNION ALL
    -- BigCells (EEZs and High Seas)
    SELECT 16 AS marine_layer_id,
           eez_big_cell_combo_id AS area_id,
           fao_area_id AS fao_area_id,
           1 AS active,
           eez_big_cell_combo.eez_id AS inherited_att_belongs_to_reconstruction_eez_id,
           0 AS inhertied_att_is_ifa,
           1 AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.eez_big_cell_combo
    JOIN sedna.eez ON (eez_big_cell_combo.eez_id = eez.eez_id)
    WHERE eez_big_cell_combo.eez_id = 0 OR is_currently_used_for_reconstruction = TRUE
    UNION ALL
    -- CCAMLR (High Seas)
    SELECT 17 AS marine_layer_id,
           eez_ccamlar_combo_id AS area_id,
           fao_area_id AS fao_area_id,
           1 AS active,
           0 AS inherited_att_belongs_to_reconstruction_eez_id,
           0 AS inhertied_att_is_ifa,
           1 AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.eez_ccamlr_combo
    WHERE eez_ccamlr_combo.eez_id = 0
    UNION ALL
    -- CCAMLR (EEZs)
    SELECT 17 AS marine_layer_id,
           eez_ccamlar_combo_id AS area_id,
           fao_area_id AS fao_area_id,
           1 AS active,
           eez_ccamlr_combo.eez_id AS inherited_att_belongs_to_reconstruction_eez_id,
           0 AS inhertied_att_is_ifa,
           CAST(allows_coastal_fishing_for_layer2_data AS INT) AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.eez_ccamlr_combo
    JOIN sedna.eez ON (eez_ccamlr_combo.eez_id = eez.eez_id)
    WHERE is_ifa = FALSE AND eez_ccamlr_combo.eez_id > 0 AND is_currently_used_for_reconstruction = TRUE
    UNION ALL
    -- CCAMLR (IFAs)
    SELECT 17 AS marine_layer_id,
           eez_ccamlar_combo_id AS area_id,
           fao_area_id AS fao_area_id,
           1 AS active,
           eez_ccamlr_combo.eez_id AS inherited_att_belongs_to_reconstruction_eez_id,
           1 AS inhertied_att_is_ifa,
           0 AS inherited_att_allows_coastal_fishing_for_layer_2_data
    FROM sedna.eez_ccamlr_combo
    JOIN sedna.eez ON (eez_ccamlr_combo.eez_id = eez.eez_id)
    WHERE is_ifa = TRUE AND is_currently_used_for_reconstruction = TRUE
);