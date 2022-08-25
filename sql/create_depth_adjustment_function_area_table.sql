-- depth adjustment function area table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/tables.sql#L453
-- and https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L160
-- and https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/functions.sql#L105
CREATE TABLE IF NOT EXISTS sedna.depth_adjustment_function_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.depth_adjustment_function_area',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS WITH qualifying_rows AS (
    SELECT d.universal_data_id,
           d.generic_allocation_area_id,
           d.year,
           d.taxon_key,
           d.catch_amount
    FROM sedna.data d
    JOIN sedna.taxon t ON (d.taxon_key = t.taxon_key)
    JOIN sedna.functional_groups fg ON (t.functional_group_id = fg.functional_group_id)
    WHERE d.allocation_area_type_id = 1
      AND d.data_layer_id = 1
      AND d.sector_type_id = 1
      AND fg.include_in_depth_adjustment_function = TRUE
      AND t.taxon_key != 100339 -- don't use unidentified pelagic fishes
), combined_catch AS (
    SELECT qr.generic_allocation_area_id,
           qr.year,
           qr.taxon_key,
           SUM(qr.catch_amount) AS sum_catch_amount
    FROM qualifying_rows qr
    GROUP BY 1,2,3
), peak_catch AS (
    SELECT cc.generic_allocation_area_id,
           cc.taxon_key,
           MAX(cc.sum_catch_amount) AS max_sum_catch_amount
    FROM combined_catch cc
    GROUP BY 1,2
), peak_catch_ratio AS (
    SELECT cc.generic_allocation_area_id,
           cc.year,
           cc.taxon_key,
           cc.sum_catch_amount / pc.max_sum_catch_amount AS ratio
    FROM combined_catch cc
    JOIN peak_catch pc ON (
        cc.generic_allocation_area_id = pc.generic_allocation_area_id AND
        cc.taxon_key = pc.taxon_key)
), peak_catch_ratio_ratchet_effect AS (
    SELECT pcr.generic_allocation_area_id,
           pcr.year,
           pcr.taxon_key,
           MAX(pcr2.ratio) AS max_peak_catch_ratio_2
    FROM peak_catch_ratio pcr
    JOIN peak_catch_ratio pcr2
      ON (pcr2.taxon_key = pcr.taxon_key AND pcr2.year <= pcr.year)
    GROUP BY 1,2,3
), eligible_rows AS (
    SELECT qr.universal_data_id,
           qr.generic_allocation_area_id,
           qr.year,
           qr.taxon_key,
           pcrre.max_peak_catch_ratio_2 AS peak_catch_ratio_ratchet_effect
    FROM qualifying_rows qr
    JOIN peak_catch_ratio_ratchet_effect pcrre ON (
        qr.generic_allocation_area_id = pcrre.generic_allocation_area_id AND
        qr.year = pcrre.year AND
        qr.taxon_key = pcrre.taxon_key)
    WHERE pcrre.max_peak_catch_ratio_2 < 0.99
), distinct_area_taxon AS (
    SELECT DISTINCT d.generic_allocation_area_id AS allocation_simple_area_id,
                    d.taxon_key
    FROM sedna.data d
    JOIN eligible_rows er ON (d.universal_data_id = er.universal_data_id)
)
SELECT row_number() OVER () AS depth_adjustment_function_area_id,
       dat.allocation_simple_area_id,
       dafca.local_depth_adjustment_row_id,
       dat.taxon_key,
       dafca.ratio AS coverage_ratio,
       dafca.min_possible_row
FROM distinct_area_taxon dat
JOIN sedna.depth_adjustment_function_create_areas dafca
  ON (dat.taxon_key = dafca.taxon_key AND dat.allocation_simple_area_id = dafca.allocation_simple_area_id);
