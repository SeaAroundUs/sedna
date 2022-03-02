SELECT
    c.raw_catch_id,
	c.layer,
	c.fishing_entity_id,
	c.eez_id,
	c.fao_area_id,
	c.year,
	c.taxon_key,
	c.amount,
	st.name as sector,
	c.catch_type_id,
	c.reporting_status_id,
	it.name as input,
	c.gear_type_id,
	ia.ices_area,
	c.ccamlr_area,
	na.nafo_division
FROM catch c
JOIN time y ON (y.year = c.year AND y.is_used_for_allocation)
JOIN sector_type st ON (st.sector_type_id = c.sector_type_id)
JOIN input_type it ON (it.input_type_id = c.input_type_id)
LEFT JOIN ices_area ia ON (ia.ices_area_id = c.ices_area_id)
LEFT JOIN nafo na ON (na.nafo_division_id = c.nafo_division_id);
