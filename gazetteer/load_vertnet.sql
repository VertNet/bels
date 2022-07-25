--------------------------------------------------------------------------------
-- Load VertNet - load_vertnet.sql  run time ~3 m
-- Script to prepare VertNet Gazetteer Location data for the BELS gazetteer.
-- Before running this script:
--   1) run the script load_gbif.sql
--   2) load the latest source data into the table vertnet.locations in BigQuery.
--   3) make sure the user defined functions saveNumbers(), removeSymbols(), and
--      simplifyDiacritics() are created.
--------------------------------------------------------------------------------
BEGIN
-- Make table locations (n=523,091 , need not persist)
CREATE OR REPLACE TABLE `localityservice.vertnet.locations`
AS
SELECT
waterbody AS v_waterbody,
islandGroup AS v_islandgroup,
island AS v_island,
country AS v_country,
countryCode AS interpreted_countrycode,
stateProvince AS v_stateprovince,
county AS v_county,
locality AS v_locality,
verbatimLocality AS v_verbatimlocality,
minimumElevationInMeters AS v_minimumelevationinmeters,
maximumElevationInMeters AS v_maximumelevationinmeters,
verbatimElevation AS v_verbatimelevation,
locationRemarks AS v_locationremarks,
decimalLatitude AS v_decimallatitude,
decimalLongitude AS v_decimallongitude,
SAFE_CAST(decimalLatitude AS FLOAT64) AS interpreted_decimallatitude,
SAFE_CAST(decimalLongitude AS FLOAT64) AS interpreted_decimallongitude,
geodeticDatum AS v_geodeticdatum,
coordinateUncertaintyInMeters AS v_coordinateuncertaintyinmeters,
georeferencedBy AS v_georeferencedby,
georeferencedDate AS v_georeferenceddate,
georeferenceProtocol AS v_georeferenceprotocol,
georeferenceSources AS v_georeferencesources,
georeferenceRemarks AS v_georeferenceremarks,
SourceProject AS source,
Quad AS quad,
Feature AS feature,
Sea AS sea,
MinimumElevation AS minimumelevation,
MaximumElevation AS maximumelevation,
ElevationUnits AS elevationunits,
TRStext AS trstext,
TRSTextcombo AS trstextcombo,
Township AS township,
TownshipDirection AS townshipdirection,
`Range` AS trs_range,
RangeDirection AS rangedirection,
Section AS section,
SectionPart AS sectionpart,
verbatimGeoreferencedDate AS verbatimgeoreferenceddate,
verbatimCoordinateSystem AS v_verbatimcoordinatesystem
FROM `localityservice.vertnet.MaNISORNISHerpNETBestPracticeGeorefs`;
-- End table locations
--------------------------------------------------------------------------------
-- Make table temp_locations_manis_distinct (n=152,854 , need not persist)
CREATE OR REPLACE TABLE localityservice.vertnet.temp_locations_manis_distinct
AS
SELECT
SHA256(CONCAT(
'dwc:highergeographyid', '',
'dwc:highergeography', '',
'dwc:continent', '',
'dwc:waterbody', IFNULL(v_waterbody,''),
'dwc:islandgroup', IFNULL(v_islandgroup,''),
'dwc:island', IFNULL(v_island,''),
'dwc:country', IFNULL(v_country,''),
'dwc:countrycode', '',
'dwc:stateprovince', IFNULL(v_stateprovince,''),
'dwc:county', IFNULL(v_county,''),
'dwc:municipality', '',
'dwc:locality', IFNULL(v_locality,''),
'dwc:verbatimlocality', IFNULL(v_verbatimlocality,''),
'dwc:minimumelevationinmeters', IFNULL(v_minimumelevationinmeters,''),
'dwc:maximumelevationinmeters', IFNULL(v_maximumelevationinmeters,''),
'dwc:verbatimelevation', IFNULL(v_verbatimelevation,''),
'dwc:verticaldatum', '',
'dwc:minimumdepthinmeters', '',
'dwc:maximumdepthinmeters', '',
'dwc:verbatimdepth', '',
'dwc:minimumdistanceabovesurfaceinmeters', '',
'dwc:maximumdistanceabovesurfaceinmeters','',
'dwc:locationaccordingto', '',
'dwc:locationremarks', IFNULL(v_locationremarks,''),
'dwc:decimallatitude', IFNULL(v_decimallatitude,''),
'dwc:decimallongitude', IFNULL(v_decimallongitude,''),
'dwc:geodeticdatum', IFNULL(v_geodeticdatum,''),
'dwc:coordinateuncertaintyinmeters', IFNULL(v_coordinateuncertaintyinmeters,''),
'dwc:coordinateprecision', '',
'dwc:pointradiusspatialfit', '',
'dwc:verbatimcoordinates', '',
'dwc:verbatimlatitude', '',
'dwc:verbatimlongitude', '',
'dwc:verbatimcoordinatesystem', IFNULL(v_verbatimcoordinatesystem,''),
'dwc:verbatimsrs', '',
'dwc:footprintwkt', '',
'dwc:footprintsrs', '',
'dwc:footprintspatialfit', '',
'dwc:georeferencedby', IFNULL(v_georeferencedby,''),
'dwc:georeferenceddate', IFNULL(v_georeferenceddate,''),
'dwc:georeferenceprotocol', IFNULL(v_georeferenceprotocol,''),
'dwc:georeferencesources', IFNULL(v_georeferencesources,''),
'dwc:georeferenceremarks', IFNULL(v_georeferenceremarks,'')))
AS dwc_location_hash,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''), ' ',
' ', -- verticaldatum placeholder
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallatitude as NUMERIC))/10000000 AS STRING),''),' ',
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallongitude as NUMERIC))/10000000 AS STRING),''))
AS for_match_with_coords,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
'') -- verticaldatum placeholder
AS for_match,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
'') -- verticaldatum placeholder
AS for_match_sans_coords,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(v_country,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_locationremarks,''),' ',
IFNULL(v_georeferencedby,''),' ',
IFNULL(v_georeferenceddate,''),' ',
IFNULL(v_georeferencesources,''),' ',
IFNULL(v_georeferenceprotocol,''),' ',
IFNULL(v_georeferenceremarks,''), ' ',
IFNULL(interpreted_countrycode,''))
AS for_tokens,
'' AS v_highergeographyid,
'' AS v_highergeography,
'' AS v_continent,
v_waterbody,
v_islandgroup,
v_island,
v_country,
'' AS v_countrycode,
v_stateprovince,
v_county,
'' AS v_municipality,
v_locality,
v_verbatimlocality,
v_minimumelevationinmeters,
v_maximumelevationinmeters,
v_verbatimelevation,
'' AS v_verticaldatum,
'' AS v_minimumdepthinmeters,
'' AS v_maximumdepthinmeters,
'' AS v_verbatimdepth,
'' AS v_minimumdistanceabovesurfaceinmeters,
'' AS v_maximumdistanceabovesurfaceinmeters,
'' AS v_locationaccordingto,
v_locationremarks,
v_decimallatitude,
v_decimallongitude,
v_geodeticdatum,
v_coordinateuncertaintyinmeters,
'' AS v_coordinateprecision,
'' AS v_pointradiusspatialfit,
'' AS v_verbatimcoordinates,
'' AS v_verbatimlatitude,
'' AS v_verbatimlongitude,
v_verbatimcoordinatesystem,
'' AS v_verbatimsrs,
'' AS v_footprintwkt,
'' AS v_footprintsrs,
'' AS v_footprintspatialfit,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
interpreted_decimallatitude,
interpreted_decimallongitude,
interpreted_countrycode,
0 AS occcount,
'WGS84' AS u_datumstr,
source
FROM localityservice.vertnet.locations
WHERE source='MaNIS'
GROUP BY
v_waterbody,
v_islandgroup,
v_island,
v_country,
v_stateprovince,
v_county,
v_locality,
v_verbatimlocality,
v_minimumelevationinmeters,
v_maximumelevationinmeters,
v_verbatimelevation,
v_verticaldatum,
v_locationremarks,
v_decimallatitude,
v_decimallongitude,
v_geodeticdatum,
v_coordinateuncertaintyinmeters,
v_verbatimcoordinatesystem,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
interpreted_decimallatitude,
interpreted_decimallongitude,
interpreted_countrycode,
occcount,
u_datumstr,
source;
-- End table temp_locations_manis_distinct
--------------------------------------------------------------------------------
-- Make table temp_locations_herpnet_distinct (n=205,772, need not persist)
CREATE OR REPLACE TABLE localityservice.vertnet.temp_locations_herpnet_distinct
AS
SELECT
SHA256(CONCAT(
'dwc:highergeographyid', '',
'dwc:highergeography', '',
'dwc:continent', '',
'dwc:waterbody', IFNULL(v_waterbody,''),
'dwc:islandgroup', IFNULL(v_islandgroup,''),
'dwc:island', IFNULL(v_island,''),
'dwc:country', IFNULL(v_country,''),
'dwc:countrycode', '',
'dwc:stateprovince', IFNULL(v_stateprovince,''),
'dwc:county', IFNULL(v_county,''),
'dwc:municipality', '',
'dwc:locality', IFNULL(v_locality,''),
'dwc:verbatimlocality', IFNULL(v_verbatimlocality,''),
'dwc:minimumelevationinmeters', IFNULL(v_minimumelevationinmeters,''),
'dwc:maximumelevationinmeters', IFNULL(v_maximumelevationinmeters,''),
'dwc:verbatimelevation', IFNULL(v_verbatimelevation,''),
'dwc:verticaldatum', '',
'dwc:minimumdepthinmeters', '',
'dwc:maximumdepthinmeters', '',
'dwc:verbatimdepth', '',
'dwc:minimumdistanceabovesurfaceinmeters', '',
'dwc:maximumdistanceabovesurfaceinmeters','',
'dwc:locationaccordingto', '',
'dwc:locationremarks', IFNULL(v_locationremarks,''),
'dwc:decimallatitude', IFNULL(v_decimallatitude,''),
'dwc:decimallongitude', IFNULL(v_decimallongitude,''),
'dwc:geodeticdatum', IFNULL(v_geodeticdatum,''),
'dwc:coordinateuncertaintyinmeters', IFNULL(v_coordinateuncertaintyinmeters,''),
'dwc:coordinateprecision', '',
'dwc:pointradiusspatialfit', '',
'dwc:verbatimcoordinates', '',
'dwc:verbatimlatitude', '',
'dwc:verbatimlongitude', '',
'dwc:verbatimcoordinatesystem', IFNULL(v_verbatimcoordinatesystem,''),
'dwc:verbatimsrs', '',
'dwc:footprintwkt', '',
'dwc:footprintsrs', '',
'dwc:footprintspatialfit', '',
'dwc:georeferencedby', IFNULL(v_georeferencedby,''),
'dwc:georeferenceddate', IFNULL(v_georeferenceddate,''),
'dwc:georeferenceprotocol', IFNULL(v_georeferenceprotocol,''),
'dwc:georeferencesources', IFNULL(v_georeferencesources,''),
'dwc:georeferenceremarks', IFNULL(v_georeferenceremarks,'')))
AS dwc_location_hash,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''), ' ',
' ', -- verticaldatum placeholder
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallatitude as NUMERIC))/10000000 AS STRING),''),' ',
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallongitude as NUMERIC))/10000000 AS STRING),''))
AS for_match_with_coords,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
'') -- verticaldatum placeholder
AS for_match,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_locality,''),' ',
IFNULL(v_verbatimlocality,''),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
'') -- verticaldatum placeholder
AS for_match_sans_coords,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(v_country,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_locationremarks,''),' ',
IFNULL(v_georeferencedby,''),' ',
IFNULL(v_georeferenceddate,''),' ',
IFNULL(v_georeferencesources,''),' ',
IFNULL(v_georeferenceprotocol,''),' ',
IFNULL(v_georeferenceremarks,''), ' ',
IFNULL(interpreted_countrycode,''))
AS for_tokens,
'' AS v_highergeographyid,
'' AS v_highergeography,
'' AS v_continent,
v_waterbody,
v_islandgroup,
v_island,
v_country,
'' AS v_countrycode,
v_stateprovince,
v_county,
'' AS v_municipality,
v_locality,
v_verbatimlocality,
v_minimumelevationinmeters,
v_maximumelevationinmeters,
v_verbatimelevation,
'' AS v_verticaldatum,
'' AS v_minimumdepthinmeters,
'' AS v_maximumdepthinmeters,
'' AS v_verbatimdepth,
'' AS v_minimumdistanceabovesurfaceinmeters,
'' AS v_maximumdistanceabovesurfaceinmeters,
'' AS v_locationaccordingto,
v_locationremarks,
v_decimallatitude,
v_decimallongitude,
v_geodeticdatum,
v_coordinateuncertaintyinmeters,
'' AS v_coordinateprecision,
'' AS v_pointradiusspatialfit,
'' AS v_verbatimcoordinates,
'' AS v_verbatimlatitude,
'' AS v_verbatimlongitude,
v_verbatimcoordinatesystem,
'' AS v_verbatimsrs,
'' AS v_footprintwkt,
'' AS v_footprintsrs,
'' AS v_footprintspatialfit,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
interpreted_decimallatitude,
interpreted_decimallongitude,
interpreted_countrycode,
0 AS occcount,
'WGS84' AS u_datumstr,
source
FROM localityservice.vertnet.locations
WHERE source='HerpNet'
GROUP BY
v_waterbody,
v_islandgroup,
v_island,
v_country,
v_stateprovince,
v_county,
v_locality,
v_verbatimlocality,
v_minimumelevationinmeters,
v_maximumelevationinmeters,
v_verbatimelevation,
v_verticaldatum,
v_locationremarks,
v_decimallatitude,
v_decimallongitude,
v_geodeticdatum,
v_coordinateuncertaintyinmeters,
v_verbatimcoordinatesystem,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
interpreted_decimallatitude,
interpreted_decimallongitude,
interpreted_countrycode,
occcount,
u_datumstr,
source;
-- End table temp_locations_herpnet_distinct
--------------------------------------------------------------------------------
-- Make table temp_locations_ornis_distinct (n=146,316, need not persist)
CREATE OR REPLACE TABLE localityservice.vertnet.temp_locations_ornis_distinct
AS
SELECT
SHA256(CONCAT(
'dwc:highergeographyid', '',
'dwc:highergeography', '',
'dwc:continent', '',
'dwc:waterbody', IFNULL(v_waterbody,''),
'dwc:islandgroup', IFNULL(v_islandgroup,''),
'dwc:island', IFNULL(v_island,''),
'dwc:country', IFNULL(v_country,''),
'dwc:countrycode', '',
'dwc:stateprovince', IFNULL(v_stateprovince,''),
'dwc:county', IFNULL(v_county,''),
'dwc:municipality', '',
'dwc:locality', IFNULL(v_locality,''),
'dwc:verbatimlocality', IFNULL(v_verbatimlocality,''),
'dwc:minimumelevationinmeters', IFNULL(v_minimumelevationinmeters,''),
'dwc:maximumelevationinmeters', IFNULL(v_maximumelevationinmeters,''),
'dwc:verbatimelevation', IFNULL(v_verbatimelevation,''),
'dwc:verticaldatum', '',
'dwc:minimumdepthinmeters', '',
'dwc:maximumdepthinmeters', '',
'dwc:verbatimdepth', '',
'dwc:minimumdistanceabovesurfaceinmeters', '',
'dwc:maximumdistanceabovesurfaceinmeters','',
'dwc:locationaccordingto', '',
'dwc:locationremarks', IFNULL(v_locationremarks,''),
'dwc:decimallatitude', IFNULL(v_decimallatitude,''),
'dwc:decimallongitude', IFNULL(v_decimallongitude,''),
'dwc:geodeticdatum', IFNULL(v_geodeticdatum,''),
'dwc:coordinateuncertaintyinmeters', IFNULL(v_coordinateuncertaintyinmeters,''),
'dwc:coordinateprecision', '',
'dwc:pointradiusspatialfit', '',
'dwc:verbatimcoordinates', '',
'dwc:verbatimlatitude', '',
'dwc:verbatimlongitude', '',
'dwc:verbatimcoordinatesystem', IFNULL(v_verbatimcoordinatesystem,''),
'dwc:verbatimsrs', '',
'dwc:footprintwkt', '',
'dwc:footprintsrs', '',
'dwc:footprintspatialfit', '',
'dwc:georeferencedby', IFNULL(v_georeferencedby,''),
'dwc:georeferenceddate', IFNULL(v_georeferenceddate,''),
'dwc:georeferenceprotocol', IFNULL(v_georeferenceprotocol,''),
'dwc:georeferencesources', IFNULL(v_georeferencesources,''),
'dwc:georeferenceremarks', IFNULL(v_georeferenceremarks,'')))
AS dwc_location_hash,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''), ' ',
' ', -- verticaldatum placeholder
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallatitude as NUMERIC))/10000000 AS STRING),''),' ',
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallongitude as NUMERIC))/10000000 AS STRING),''))
AS for_match_with_coords,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
'') -- verticaldatum placeholder
AS for_match,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(interpreted_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
'') -- verticaldatum placeholder
AS for_match_sans_coords,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(v_country,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
 IF(v_locality IS NULL,v_verbatimlocality,
   IF(v_verbatimlocality IS NULL, v_locality,
     IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
       CONCAT(v_locality,' ',v_verbatimlocality)
     )
   )
 )
),' ',
IFNULL(v_locationremarks,''),' ',
IFNULL(v_georeferencedby,''),' ',
IFNULL(v_georeferenceddate,''),' ',
IFNULL(v_georeferencesources,''),' ',
IFNULL(v_georeferenceprotocol,''),' ',
IFNULL(v_georeferenceremarks,''), ' ',
IFNULL(interpreted_countrycode,''))
AS for_tokens,
'' AS v_highergeographyid,
'' AS v_highergeography,
'' AS v_continent,
v_waterbody,
v_islandgroup,
v_island,
v_country,
'' AS v_countrycode,
v_stateprovince,
v_county,
'' AS v_municipality,
v_locality,
v_verbatimlocality,
v_minimumelevationinmeters,
v_maximumelevationinmeters,
v_verbatimelevation,
'' AS v_verticaldatum,
'' AS v_minimumdepthinmeters,
'' AS v_maximumdepthinmeters,
'' AS v_verbatimdepth,
'' AS v_minimumdistanceabovesurfaceinmeters,
'' AS v_maximumdistanceabovesurfaceinmeters,
'' AS v_locationaccordingto,
v_locationremarks,
v_decimallatitude,
v_decimallongitude,
v_geodeticdatum,
v_coordinateuncertaintyinmeters,
'' AS v_coordinateprecision,
'' AS v_pointradiusspatialfit,
'' AS v_verbatimcoordinates,
'' AS v_verbatimlatitude,
'' AS v_verbatimlongitude,
v_verbatimcoordinatesystem,
'' AS v_verbatimsrs,
'' AS v_footprintwkt,
'' AS v_footprintsrs,
'' AS v_footprintspatialfit,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
interpreted_decimallatitude,
interpreted_decimallongitude,
interpreted_countrycode,
0 AS occcount,
'WGS84' AS u_datumstr,
source
FROM localityservice.vertnet.locations
WHERE source='ORNIS'
GROUP BY
v_waterbody,
v_islandgroup,
v_island,
v_country,
v_stateprovince,
v_county,
v_locality,
v_verbatimlocality,
v_minimumelevationinmeters,
v_maximumelevationinmeters,
v_verbatimelevation,
v_verticaldatum,
v_locationremarks,
v_decimallatitude,
v_decimallongitude,
v_geodeticdatum,
v_coordinateuncertaintyinmeters,
v_verbatimcoordinatesystem,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
interpreted_decimallatitude,
interpreted_decimallongitude,
interpreted_countrycode,
occcount,
u_datumstr,
source;
-- End table temp_locations_ornis_distinct
--------------------------------------------------------------------------------
-- Make table temp_locations_vertnet_distinct (n=503.970, need not persist)
CREATE OR REPLACE TABLE `localityservice.vertnet.temp_locations_vertnet_distinct`
AS
SELECT *  FROM `localityservice.vertnet.temp_locations_manis_distinct`;
 
INSERT INTO `localityservice.vertnet.temp_locations_vertnet_distinct`
(SELECT *  FROM `localityservice.vertnet.temp_locations_herpnet_distinct`
WHERE dwc_location_hash NOT IN
(SELECT dwc_location_hash FROM
`localityservice.vertnet.temp_locations_vertnet_distinct`)
);
 
INSERT INTO `localityservice.vertnet.temp_locations_vertnet_distinct`
(SELECT *  FROM `localityservice.vertnet.temp_locations_ornis_distinct`
WHERE dwc_location_hash NOT IN
(SELECT dwc_location_hash FROM
`localityservice.vertnet.temp_locations_vertnet_distinct`)
);
-- End table temp_locations_vertnet_distinct
--------------------------------------------------------------------------------
-- Make table locations_distinct_with_scores (n=503,970, persist)
CREATE OR REPLACE TABLE localityservice.vertnet.locations_distinct_with_scores
AS
SELECT dwc_location_hash, v_highergeographyid, v_highergeography, v_continent,
v_waterbody, v_islandgroup, v_island, v_country, v_countrycode, v_stateprovince,
v_county, v_municipality, v_locality, v_verbatimlocality, v_minimumelevationinmeters,
v_maximumelevationinmeters, v_verbatimelevation, v_verticaldatum,
v_minimumdepthinmeters, v_maximumdepthinmeters, v_verbatimdepth,
v_minimumdistanceabovesurfaceinmeters, v_maximumdistanceabovesurfaceinmeters,
v_locationaccordingto, v_locationremarks, v_decimallatitude, v_decimallongitude,
v_geodeticdatum, v_coordinateuncertaintyinmeters, v_coordinateprecision,
v_pointradiusspatialfit, v_verbatimcoordinates, v_verbatimlatitude, v_verbatimlongitude, v_verbatimcoordinatesystem, v_verbatimsrs, v_footprintwkt,
v_footprintsrs, v_footprintspatialfit, v_georeferencedby, v_georeferenceddate,
v_georeferenceprotocol, v_georeferencesources,
v_georeferenceremarks, interpreted_decimallatitude, interpreted_decimallongitude,
interpreted_countrycode, occcount, u_datumstr,
REGEXP_REPLACE(REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_tokens)),NFKC)),r"[\s]+",' '), r"^\s+|\s+$", '') AS tokens,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match_with_coords)),NFKC)),r"[\s]+",'') AS matchme_with_coords,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match)),NFKC)),r"[\s]+",'') AS matchme,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match_sans_coords)),NFKC)),r"[\s]+",'') AS matchme_sans_coords,
4326 AS epsg,
IF(v_georeferenceprotocol IS NULL,0,16) +
IF(v_georeferencesources IS NULL,0,8) +
IF(v_georeferenceddate IS NULL,0,4) +
IF(v_georeferencedby IS NULL,0,2) +
IF(v_georeferenceremarks IS NULL,0,1)
AS georef_score,
IF(interpreted_decimallatitude IS NULL or interpreted_decimallongitude IS NULL,0,128) +
64 +
IF(SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)>=1 AND
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)<20037509,32,0) +
IF(v_georeferenceprotocol IS NULL,0,16) +
IF(v_georeferencesources IS NULL,0,8) +
IF(v_georeferenceddate IS NULL,0,4) +
IF(v_georeferencedby IS NULL,0,2) +
IF(v_georeferenceremarks IS NULL,0,1)
AS coordinates_score,
source
FROM localityservice.vertnet.temp_locations_vertnet_distinct;
END;
--------------------------------------------------------------------------------
