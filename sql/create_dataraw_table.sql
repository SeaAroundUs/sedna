-- dataraw table creation
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L1515
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
	na.nafo_division
FROM sedna.catch c
JOIN sedna.time y ON (y.year = c.year AND y.is_used_for_allocation)
JOIN sedna.sector_type st ON (st.sector_type_id = c.sector_type_id)
JOIN sedna.input_type it ON (it.input_type_id = c.input_type_id)
LEFT JOIN sedna.ices_area ia ON (ia.ices_area_id = c.ices_area_id)
LEFT JOIN sedna.nafo na ON (na.nafo_division_id = c.nafo_division_id)
LEFT JOIN sedna.taxon_distribution_substitute tds ON (c.taxon_key = tds.original_taxon_key);
