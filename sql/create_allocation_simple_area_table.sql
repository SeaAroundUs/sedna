-- allocation_simple_area table creation
-- from https://github.com/SeaAroundUs/MerlinCSharp_MSSQL/blob/a90ee38b5b9fc7827803b6a267c4e681a764bfa2/MerlinGen.sql#L375
CREATE TABLE IF NOT EXISTS sedna.allocation_simple_area
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.allocation_simple_area',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
)
AS SELECT row_number() OVER () AS allocation_simple_area_id,
          marine_layer_id,
          area_id,
          CAST(fao_area_id AS INT) AS fao_area_id,
          is_active AS active,
          inherited_att_belongs_to_reconstruction_eez_id,
          inherited_att_is_ifa,
          inherited_att_allows_coastal_fishing_for_layer2_data
   FROM sedna.v_internal_generate_allocation_simple_area_table;
