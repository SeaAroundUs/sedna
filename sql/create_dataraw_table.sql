-- dataraw table creation
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L1515
-- had to bring some of the area_id logic over from the data table to handle nested sub-queries in athena
-- from https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Area/AssignGenericAreaIDToData.cs#L30
CREATE TABLE IF NOT EXISTS sedna.dataraw
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.dataraw',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT
    row_number() OVER () AS universal_data_id,
    c.raw_catch_id,
	c.layer,
	c.fishing_entity_id,
	c.eez_id,
	c.fao_area_id,
	c.year,
	COALESCE(tds.use_this_taxon_key_instead, c.taxon_key) AS taxon_key,
	c.amount,
	st.name AS sector,
	c.catch_type_id,
	c.reporting_status_id,
	it.name AS input,
	c.gear_type_id,
	ia.ices_area,
	c.ccamlr_area,
	na.nafo_division,
    CASE
        WHEN c.eez_id = 999 THEN 'Layer3' -- OR (dr.BigCellID != null && dr.BigCellID > 0) --TODO where is this field coming from?
        WHEN c.fao_area_id = 21 AND na.nafo_division IS NOT NULL THEN 'NAFO'
        WHEN c.fao_area_id IN (48, 58, 88) AND c.ccamlr_area IS NOT NULL THEN 'CCAMLR'
        WHEN c.fao_area_id = 27 THEN 'ICES'
        WHEN c.eez_id = 0 AND c.fao_area_id > 0 THEN 'High Seas'
        WHEN c.eez_id > 0 AND c.fao_area_id > 0 AND c.sector_type_id != 1 THEN 'IFA'
        WHEN c.eez_id > 0 AND c.fao_area_id > 0 AND c.sector_type_id = 1 THEN 'EEZ'
        ELSE NULL -- this shouldn't happen
    END AS area_type,
    CASE
        -- Layer3
        WHEN c.eez_id = 999
        THEN NULL --TODO
        -- NAFO
        WHEN c.fao_area_id = 21 AND na.nafo_division IS NOT NULL
        THEN (SELECT eez_nafo_combo_id
              FROM sedna.eez_nafo_combo enc
              WHERE enc.eez_id = c.eez_id
                AND enc.nafo_division = na.nafo_division
                AND enc.is_ifa = (st.sector_type_id != 1))
        -- CCAMLR
        WHEN c.fao_area_id IN (48, 58, 88) AND c.ccamlr_area IS NOT NULL
        THEN (SELECT eez_ccamlar_combo_id
              FROM sedna.eez_ccamlr_combo ecc
              WHERE ecc.eez_id = c.eez_id
                AND ecc.ccamlr_area_id = c.ccamlr_area
                AND ecc.is_ifa = (st.sector_type_id != 1))
        -- ICES
        WHEN c.fao_area_id = 27
        THEN (SELECT eez_ices_combo_id
              FROM sedna.eez_ices_combo eic
              WHERE eic.eez_id = c.eez_id
                AND eic.ices_area_id = ia.ices_area
                AND eic.is_ifa = (st.sector_type_id != 1))
        -- High Seas
        WHEN c.eez_id = 0 AND c.fao_area_id > 0
        THEN c.fao_area_id
        -- IFA
        WHEN c.eez_id > 0 AND c.fao_area_id > 0 AND c.sector_type_id != 1
        THEN c.eez_id
        -- EEZ
        WHEN c.eez_id > 0 AND c.fao_area_id > 0 AND c.sector_type_id = 1
        THEN c.eez_id
        ELSE NULL -- this shouldn't happen
    END AS area_id
FROM sedna.catch c
JOIN sedna.time y ON (y.year = c.year AND y.is_used_for_allocation)
JOIN sedna.sector_type st ON (st.sector_type_id = c.sector_type_id)
JOIN sedna.input_type it ON (it.input_type_id = c.input_type_id)
LEFT JOIN sedna.ices_area ia ON (ia.ices_area_id = c.ices_area_id)
LEFT JOIN sedna.nafo na ON (na.nafo_division_id = c.nafo_division_id)
LEFT JOIN sedna.taxon_distribution_substitute tds ON (c.taxon_key = tds.original_taxon_key);
