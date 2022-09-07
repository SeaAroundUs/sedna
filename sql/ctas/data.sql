-- data table creation (update predepth_data with depth function output)
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L226
CREATE TABLE IF NOT EXISTS sedna.data
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.data',
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  partitioned_by = ARRAY['allocation_area_type_id', 'data_layer_id'],
  bucketed_by = ARRAY['unique_area_id', 'original_fishing_entity_id'],
  bucket_count = 10
) AS WITH matching_coverage_ratio AS (
    SELECT dafer.universal_data_id,
           MIN(dafa.coverage_ratio) AS ratio
    FROM sedna.depth_adjustment_function_eligible_rows dafer
    JOIN sedna.depth_adjustment_function_area dafa
      ON (dafer.generic_allocation_area_id = dafa.allocation_simple_area_id AND
          dafer.taxon_key = dafa.taxon_key)
    WHERE dafer.peak_catch_ratio_ratchet_effect <= dafa.coverage_ratio
    GROUP BY dafer.universal_data_id
), matched_area_type_3 AS (
    SELECT dafer.universal_data_id,
           dafa.depth_adjustment_function_area_id,
           3 AS depth_allocation_area_type,
           TRUE AS internal_audit_depth_function_override
    FROM matching_coverage_ratio mcr
    JOIN sedna.depth_adjustment_function_eligible_rows dafer
      ON (mcr.universal_data_id = dafer.universal_data_id)
    JOIN sedna.depth_adjustment_function_area dafa
      ON (dafa.allocation_simple_area_id = dafer.generic_allocation_area_id AND
          dafa.taxon_key = dafer.taxon_key AND
          dafa.coverage_ratio = mcr.ratio)
)
SELECT dense_rank() OVER (ORDER BY data_layer_id, allocation_area_type_id, generic_allocation_area_id) AS unique_area_id,
       *
FROM (
    SELECT pd.universal_data_id,
           pd.area_type,
           COALESCE(mat.depth_adjustment_function_area_id, pd.generic_allocation_area_id) AS generic_allocation_area_id,
           pd.original_fishing_entity_id,
           pd.fishing_entity_id,
           pd.catch_amount,
           pd.catch_type_id,
           pd.reporting_status_id,
           pd.gear_type_id,
           pd.input_type_id,
           pd.sector_type_id,
           pd.taxon_key,
           pd.year,
           COALESCE(mat.internal_audit_depth_function_override, FALSE) AS internal_audit_depth_function_override,
           -- partition columns at the end
           COALESCE(mat.depth_allocation_area_type, pd.allocation_area_type_id) AS allocation_area_type_id,
           pd.data_layer_id
    FROM sedna.predepth_data pd
    LEFT JOIN matched_area_type_3 mat ON (mat.universal_data_id = pd.universal_data_id)
);
