-- data table creation (update predepth_data with depth function output)
-- from https://github.com/SeaAroundUs/Merlin-database-mssql/blob/4b223108bad7e6863e7feae853053778026568c8/sprocs.sql#L226
CREATE TABLE IF NOT EXISTS sedna.data
WITH (
  external_location = 's3://{BUCKET_NAME}/{PARQUET_PREFIX}/ctas.data',
  format = 'PARQUET',
  write_compression = 'SNAPPY'
) AS WITH matching_coverage_ratio AS (
    SELECT dafer.universal_data_id,
           MIN(dafa.coverage_ratio) AS ratio
    FROM sedna.depth_adjustment_function_eligible_rows dafer
    JOIN sedna.depth_adjustment_function_area dafa
      ON (dafer.generic_allocation_area_id = dafa.allocation_simple_area_id AND dafer.taxon_key = dafa.taxon_key)
    WHERE dafer.peak_catch_ratio = dafa.covere_ratio
    GROUP BY dafa.unviversal_data_id
), matched_area_type_3 AS (
    SELECT dafer.universal_data_id,
           dafa.depth_adjustment_function_area_id
    FROM matching_coverage_ratio mcr
    JOIN sedna.depth_adjustment_function_eligible_rows dafer
      ON (mcr.universal_data_id = dafer.universal_data_id)
    JOIN sedna.depth_adjustment_function_area dafa
      ON (dafa.allocation_simple_area_id = dafer.generic_allocation_area_id AND
          dafa.taxon_key = dafer.taxon_key AND
          dafa.coverage_ratio = mcr.ratio)
)

SELECT * FROM matched_area_type_3 limit 100;

-- with matching_CoverageRatio as
-- 	( select universalDataID, min(a.coverageRatio) as MatchedCoverageRatio
-- 	 from [dbo].[DepthAdjustmentFunction_EligibleRowsOfData] e inner join
-- 	 [dbo].[DepthAdjustmentFunction_Area] a on
-- 	 e.[GenericAllocationAreaID] = a.[AllocationSimpleAreaID] and e.TaxonKey = a.taxonkey
-- 	where  e.PeakCatchRatio <= a.CoverageRatio
-- 		group by universalDataID
-- 	),
-- 	Matched_AreaType3 as (select e.UniversalDataID, a.[DepthAdjustmentFunction_AreaID]
-- 	from  matching_CoverageRatio m inner join [dbo].[DepthAdjustmentFunction_EligibleRowsOfData] e
-- 	on m.universalDataID = e.universalDataID
-- 	inner join [dbo].[DepthAdjustmentFunction_Area] a
-- 	on a.[AllocationSimpleAreaID] = e.GenericAllocationAreaID and a.TaxonKey = e.taxonkey and a.CoverageRatio = m.MatchedCoverageRatio
-- 	)
-- Update dbo.data
-- set AllocationAreaTypeID = 3,
-- GenericAllocationAreaID = m.[DepthAdjustmentFunction_AreaID]
-- from dbo.data d inner join Matched_AreaType3 m on d.UniversalDataID = m.UniversalDataID
