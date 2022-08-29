-- hybrid to simple area mapper table
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/tables.sql#L183
-- and https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/Resolve999/step4/HybridToSimpleAreaMapper.cs
CREATE TABLE IF NOT EXISTS sedna.hybrid_to_simple_area_mapper
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.hybrid_to_simple_area_mapper',
  format = 'PARQUET',
  write_compression = 'SNAPPY'
)
AS SELECT row_number() OVER () AS row_id, *
FROM (
    SELECT aha.allocation_hybrid_area_id AS hybrid_area_id,
           asa.allocation_simple_area_id AS contains_simple_area_id
    FROM sedna.allocation_hybrid_area aha
    CROSS JOIN UNNEST(SPLIT(aha.area_ids_1, ',')) AS area_ids(id)
    JOIN sedna.allocation_simple_area asa ON (
        asa.marine_layer_id = aha.marine_layer_id_1 AND
        CAST(area_ids.id AS INT) = asa.area_id)
    WHERE aha.area_ids_1 != ''
UNION ALL
    SELECT aha.allocation_hybrid_area_id AS hybrid_area_id,
           asa.allocation_simple_area_id AS contains_simple_area_id
    FROM sedna.allocation_hybrid_area aha
    CROSS JOIN UNNEST(SPLIT(aha.area_ids_2, ',')) AS area_ids(id)
    JOIN sedna.allocation_simple_area asa ON (
        asa.marine_layer_id = aha.marine_layer_id_2 AND
        CAST(area_ids.id AS INT) = asa.area_id)
    WHERE aha.area_ids_2 != ''
);
