-- TODO export query data to S3
-- https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_StartExportTask.html
CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;

-- original query source:
-- https://github.com/SeaAroundUs/Merlin-database-mssql/blob/master/sprocs.sql#L1164

-- master.access_agreement -> dbo.AgreementRaw
SELECT id,
       fishing_entity_id,
       eez_id,
       title_of_agreement,
       NULL::VARCHAR                            AS original_area_code,
       generate_simple_acronym(access_category) AS fishing_access,
       access_type_id,
       agreement_type_id,
       start_year,
       end_year,
       functional_group_id
FROM master.access_agreement;

-- master.taxon -> dbo.Cube_DimTaxon
SELECT taxon_key,
       scientific_name,
       common_name,
       commercial_group_id,
       functional_group_id,
       sl_max,
       tl,
       taxon_level_id,
       taxon_group_id,
       isscaap_id,
       lat_north,
       lat_south,
       min_depth,
       max_depth,
       loo,
       woo,
       k,
       has_habitat_index::INT AS has_habitat_index,
       has_map::INT           AS has_map,
       is_baltic_only::INT    AS is_baltic_only
FROM master.taxon
WHERE NOT is_retired
  AND tl IS NOT NULL;

-- distribution.taxon_distribution -> dbo.TaxonDistribution
SELECT taxon_distribution_id, taxon_key, cell_id, relative_abundance
FROM distribution.taxon_distribution;

-- master.time -> dbo.AllocationYear
SELECT time_key, year
FROM master.time
WHERE is_used_for_allocation;

-- master.catch_type -> dbo.CatchType
SELECT catch_type_id, name
FROM master.catch_type;

-- master.reporting_status -> dbo.ReportingStatus
SELECT reporting_status_id, name
FROM master.reporting_status;

-- master.input_type -> dbo.InputType
SELECT input_type_id, name
FROM master.input_type;

-- allocation.layer -> dbo.Layer
SELECT layer_id, name
FROM allocation.layer;

-- allocation.allocation_area_type -> dbo.AllocationAreaType
SELECT allocation_area_type_id, name, remarks
FROM allocation.allocation_area_type;

-- master.eez -> dbo.EEZ
SELECT eez_id,
       name,
       alternate_name,
       geo_entity_id,
       area_status_id,
       legacy_c_number,
       legacy_count_code,
       fishbase_id,
       coords,
       can_be_displayed_on_web::INT                AS can_be_displayed_on_web,
       is_currently_used_for_web::INT              AS is_currently_used_for_web,
       is_currently_used_for_reconstruction::INT   AS is_currently_used_for_reconstruction,
       declaration_year,
       earliest_access_agreement_date,
       is_home_eez_of_fishing_entity_id,
       allows_coastal_fishing_for_layer2_data::INT AS allows_coastal_fishing_for_layer2_data
FROM master.eez
WHERE eez_id NOT IN (0, 999);

-- master.fao_area -> dbo.FaoArea
SELECT fao_area_id, name, alternate_name
FROM master.fao_area;

-- master.fishing_entity -> dbo.FishingEntity
SELECT fishing_entity_id,
       name,
       geo_entity_id,
       date_allowed_to_fish_other_eezs,
       date_allowed_to_fish_high_seas,
       legacy_c_number,
       is_currently_used_for_web::INT            AS is_currently_used_for_web,
       is_currently_used_for_reconstruction::INT AS is_currently_used_for_reconstruction,
       is_allowed_to_fish_pre_eez_by_default::INT,
       remarks
FROM master.fishing_entity
WHERE is_currently_used_for_reconstruction;

-- master.functional_groups -> dbo.FunctionalGroup
SELECT functional_group_id,
       target_grp,
       name,
       description,
       include_in_depth_adjustment_function::INT AS include_in_depth_adjustment_function
FROM master.functional_groups;

-- master.gear -> dbo.GearType
SELECT gear_id, name, super_code
FROM master.gear;

-- master.geo_entity -> dbo.GeoEntity
SELECT geo_entity_id, name, admin_geo_entity_id, jurisdiction_id, started_eez_at, legacy_c_number, legacy_admin_c_number
FROM master.geo_entity
WHERE geo_entity_id != 0;

-- master.high_seas -> dbo.HighSea
SELECT fao_area_id
FROM master.high_seas;

-- allocation.ices_area -> dbo.ICES_Area
SELECT ices_division, ices_subdivision, ices_area_id
FROM allocation.ices_area;

-- geo.ifa_fao -> dbo.IFA
SELECT eez_id, ifa_is_located_in_this_fao
FROM geo.ifa_fao;

-- master.lme -> dbo.LME
SELECT lme_id, name, profile_url
FROM master.lme;

-- master.marine_layer -> dbo.MarineLayer
SELECT marine_layer_id, remarks, name, bread_crumb_name, show_sub_areas::INT AS show_sub_areas, last_report_year
FROM master.marine_layer;

-- master.sector_type -> dbo.SectorType
SELECT sector_type_id, name
FROM master.sector_type;

-- recon.catch -> dbo.DataRaw
SELECT c.raw_catch_id,
       c.layer,
       c.fishing_entity_id,
       c.eez_id,
       c.fao_area_id,
       c.year,
       c.taxon_key,
       c.amount,
       st.name AS sector,
       c.catch_type_id,
       c.reporting_status_id,
       it.name AS input,
       c.gear_type_id,
       ia.ices_area,
       c.ccamlr_area,
       na.nafo_division
FROM recon.catch c
JOIN master.time y ON (y.year = c.year AND y.is_used_for_allocation)
JOIN master.sector_type st ON (st.sector_type_id = c.sector_type_id)
JOIN master.input_type it ON (it.input_type_id = c.input_type_id)
LEFT JOIN recon.ices_area ia ON (ia.ices_area_id = c.ices_area_id)
LEFT JOIN recon.nafo na ON (na.nafo_division_id = c.nafo_division_id);

-- distribution.taxon_distribution_substitute -> dbo.TaxonDistributionSubstitute
SELECT original_taxon_key, use_this_taxon_key_instead
FROM distribution.taxon_distribution_substitute;

-- geo.simple_area_cell_assignment_raw -> dbo.SimpleAreaCellAssignmentRaw
SELECT id, marine_layer_id, area_id, fao_area_id, cell_id, water_area
FROM geo.simple_area_cell_assignment_raw;

-- geo.cell -> dbo.Cell
SELECT cell_id, total_area, water_area
FROM geo.cell
WHERE water_area > 0;

-- geo.big_cell_type -> dbo.BigCellType
SELECT big_cell_type_id, type_desc
FROM geo.big_cell_type;

-- geo.big_cell -> dbo.BigCell
SELECT big_cell_id,
       big_cell_type_id,
       x,
       y,
       is_land_locked::INT AS ll,
       is_in_med::INT      AS med,
       is_in_pacific::INT  AS pac,
       is_in_indian::INT   AS ind
FROM geo.big_cell;

-- geo.cell_is_coastal -> dbo.CellIsCoastal
SELECT cell_id
FROM geo.cell_is_coastal;

-- geo.depth_adjustment_row_cell -> dbo.CellIsCoastal
SELECT local_depth_adjustment_row_id, eez_id, cell_id
FROM geo.depth_adjustment_row_cell;

-- geo.eez_big_cell_combo -> dbo.EEZ_BigCell_Combo
SELECT eez_big_cell_combo_id, eez_id, fao_area_id, big_cell_id, is_ifa::INT AS ifa
FROM geo.eez_big_cell_combo;

-- geo.eez_ccamlr_combo -> dbo.EEZ_CCAMLR_Combo
SELECT eez_ccamlar_combo_id, eez_id, fao_area_id, ccamlr_area_id, is_ifa::INT AS ifa
FROM geo.eez_ccamlr_combo;

-- geo.eez_fao_combo -> dbo.EEZ_FAO_Combo
SELECT eez_fao_area_id, reconstruction_eez_id, fao_area_id
FROM geo.eez_fao_combo;

-- geo.eez_ices_combo -> dbo.EEZ_ICES_Combo
SELECT eez_ices_combo_id, eez_id, fao_area_id, ices_area_id, is_ifa::INT AS ifa
FROM geo.eez_ices_combo;

-- geo.eez_nafo_combo -> dbo.EEZ_NAFO_Combo
SELECT eez_nafo_combo_id, eez_id, fao_area_id, nafo_division, is_ifa::INT AS ifa
FROM geo.eez_nafo_combo;

-- geo.fao_cell -> dbo.FAOCell
SELECT fao_area_id, cell_id
FROM geo.fao_cell;

-- geo.fao_map -> dbo.FAOMap
SELECT fao_area_id, upper_left_cell_cell_id, scale
FROM geo.fao_map;

-- master.price -> dbo.Price (optional)
SELECT year, fishing_entity_id, taxon_key, price
FROM master.price;

-- allocation.v_internal_generate_allocation_simple_area_table -> imported.AllocationSimpleArea
SELECT marine_layer_id,
       area_id,
       fao_area_id,
       is_active,
       inherited_att_belongs_to_reconstruction_eez_id,
       inherited_att_is_ifa,
       inherited_att_allows_coastal_fishing_for_layer2_data
FROM allocation.v_internal_generate_allocation_simple_area_table;

-- recon.data_raw_layer3 -> dbo.DataRaw_Layer3
SELECT row_id,
       rfmo_id,
       year,
       fishing_entity_id,
       layer3_gear_id,
       taxon_key,
       big_cell_id,
       catch,
       catch_type_id
FROM recon.data_raw_layer3;
