-- allocation process
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L15
CREATE TABLE IF NOT EXISTS sedna.allocation_result
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.allocation_result',
  format = 'PARQUET',
  write_compression = 'SNAPPY'
)
AS WITH results AS (
    SELECT d.universal_data_id,
           d.unique_area_id,
           auac.allocation_simple_area_id,
           auac.cell_id,
           auac.water_area * td.relative_abundance AS water_area_x_relative_abundance,
           d.catch_amount AS total_catch,
           d.taxon_key
    FROM sedna.data d
    JOIN sedna.allocation_unique_area_cell auac
      ON (d.unique_area_id = auac.unique_area_id)
    JOIN sedna.taxon_distribution td
      ON (auac.cell_id = td.cell_id AND d.taxon_key = td.taxon_key)
), sum_relative_abundance AS (
    SELECT r.universal_data_id,
           SUM(r.water_area_x_relative_abundance) AS sum_relative_abundance
    FROM results r
    GROUP BY 1
    HAVING sum_relative_abundance > 0
)
SELECT r.universal_data_id,
       r.allocation_simple_area_id,
       r.cell_id,
       r.total_catch / sra.sum_relative_abundance
FROM results r
JOIN sum_relative_abundance sra
  ON (r.universal_data_id = sra.universal_data_id);
