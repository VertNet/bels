--------------------------------------------------------------------------------
-- Load GBIF - load_gbif.sql  run time ~12 m
-- Script to prepare GBIF Location data for the gazetteer.
-- Before running this script:
--   1) load the latest source data into the table gbif.occurrences in BigQuery.
--   2) reconcile and add any new values of u_datumstr with epsg codes to
--      table vocabs.datumsepsg
--   3) make sure the user defined functions saveNumbers(), removeSymbols(), and
--      simplifyDiacritics() are created.
--------------------------------------------------------------------------------
BEGIN
-- Make table temp_locations_gbif_distinct
CREATE TEMP TABLE temp_locations_gbif_distinct
AS
SELECT
SHA256(CONCAT(
'dwc:highergeographyid', IFNULL(v_highergeographyid,''),
'dwc:highergeography', IFNULL(v_highergeography,''),
'dwc:continent', IFNULL(v_continent,''),
'dwc:waterbody', IFNULL(v_waterbody,''),
'dwc:islandgroup', IFNULL(v_islandgroup,''),
'dwc:island', IFNULL(v_island,''),
'dwc:country', IFNULL(v_country,''),
'dwc:countrycode', IFNULL(v_countrycode,''),
'dwc:stateprovince', IFNULL(v_stateprovince,''),
'dwc:county', IFNULL(v_county,''),
'dwc:municipality', IFNULL(v_municipality,''),
'dwc:locality', IFNULL(v_locality,''),
'dwc:verbatimlocality', IFNULL(v_verbatimlocality,''),
'dwc:minimumelevationinmeters', IFNULL(v_minimumelevationinmeters,''),
'dwc:maximumelevationinmeters', IFNULL(v_maximumelevationinmeters,''),
'dwc:verbatimelevation', IFNULL(v_verbatimelevation,''),
'dwc:verticaldatum', IFNULL(v_verticaldatum,''),
'dwc:minimumdepthinmeters', IFNULL(v_minimumdepthinmeters,''),
'dwc:maximumdepthinmeters', IFNULL(v_maximumdepthinmeters,''),
'dwc:verbatimdepth', IFNULL(v_verbatimdepth,''),
'dwc:minimumdistanceabovesurfaceinmeters', IFNULL(v_minimumdistanceabovesurfaceinmeters,''),
'dwc:maximumdistanceabovesurfaceinmeters', IFNULL(v_maximumdistanceabovesurfaceinmeters,''),
'dwc:locationaccordingto', IFNULL(v_locationaccordingto,''),
'dwc:locationremarks', IFNULL(v_locationremarks,''),
'dwc:decimallatitude', IFNULL(v_decimallatitude,''),
'dwc:decimallongitude', IFNULL(v_decimallongitude,''),
'dwc:geodeticdatum', IFNULL(v_geodeticdatum,''),
'dwc:coordinateuncertaintyinmeters', IFNULL(v_coordinateuncertaintyinmeters,''),
'dwc:coordinateprecision', IFNULL(v_coordinateprecision,''),
'dwc:pointradiusspatialfit', IFNULL(v_pointradiusspatialfit,''),
'dwc:verbatimcoordinates', IFNULL(v_verbatimcoordinates,''),
'dwc:verbatimlatitude', IFNULL(v_verbatimlatitude,''),
'dwc:verbatimlongitude', IFNULL(v_verbatimlongitude,''),
'dwc:verbatimcoordinatesystem', IFNULL(v_verbatimcoordinatesystem,''),
'dwc:verbatimsrs', IFNULL(v_verbatimsrs,''),
'dwc:footprintwkt', IFNULL(v_footprintwkt,''),
'dwc:footprintsrs', IFNULL(v_footprintsrs,''),
'dwc:footprintspatialfit', IFNULL(v_footprintspatialfit,''),
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
IFNULL(countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_municipality,''),' ',
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
IFNULL(v_verticaldatum,''),' ',
IFNULL(v_minimumdepthinmeters,''),' ',
IFNULL(v_maximumdepthinmeters,''),' ',
IFNULL(v_verbatimdepth,''),' ',
IFNULL(v_verbatimcoordinates,''),' ',
IFNULL(v_verbatimlatitude,''),' ',
IFNULL(v_verbatimlongitude,''),' ',
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallatitude AS NUMERIC))/10000000 AS STRING),''),' ',
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallongitude AS NUMERIC))/10000000 AS STRING),''))
AS for_match_with_coords,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_municipality,''),' ',
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
IFNULL(v_verticaldatum,''),' ',
IFNULL(v_minimumdepthinmeters,''),' ',
IFNULL(v_maximumdepthinmeters,''),' ',
IFNULL(v_verbatimdepth,''),' ',
IFNULL(v_verbatimcoordinates,''),' ',
IFNULL(v_verbatimlatitude,''),' ',
IFNULL(v_verbatimlongitude,''))
AS for_match,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_municipality,''),' ',
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
IFNULL(v_verticaldatum,''),' ',
IFNULL(v_minimumdepthinmeters,''),' ',
IFNULL(v_maximumdepthinmeters,''),' ',
IFNULL(v_verbatimdepth,''))
AS for_match_sans_coords,
CONCAT(
IFNULL(v_continent,''),' ',
IFNULL(v_highergeography,''),' ',
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(v_country,''),' ',
IFNULL(v_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_municipality,''),' ',
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
IFNULL(v_georeferenceremarks,''),' ',
IFNULL(countrycode,''))
AS for_tokens,
v_highergeographyid,
v_highergeography,
v_continent,
v_waterbody,
v_islandgroup,
v_island,
v_country,
v_countrycode,
v_stateprovince,
v_county,
v_municipality,
v_locality,
v_verbatimlocality,
v_minimumelevationinmeters,
v_maximumelevationinmeters,
v_verbatimelevation,
v_verticaldatum,
v_minimumdepthinmeters,
v_maximumdepthinmeters,
v_verbatimdepth,
v_minimumdistanceabovesurfaceinmeters,
v_maximumdistanceabovesurfaceinmeters,
v_locationaccordingto,
v_locationremarks,
v_decimallatitude,
v_decimallongitude,
v_geodeticdatum,
v_coordinateuncertaintyinmeters,
v_coordinateprecision,
v_pointradiusspatialfit,
v_verbatimcoordinates,
v_verbatimlatitude,
v_verbatimlongitude,
v_verbatimcoordinatesystem,
v_verbatimsrs,
v_footprintwkt,
v_footprintsrs,
v_footprintspatialfit,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
decimallatitude AS interpreted_decimallatitude,
decimallongitude AS interpreted_decimallongitude,
countrycode AS interpreted_countrycode,
count(*) AS occcount
FROM `localityservice.gbif.occurrences` t
GROUP BY
dwc_location_hash,
for_match_with_coords,
for_match,
for_match_sans_coords,
v_highergeographyid,
v_highergeography,
v_continent,
v_waterbody,
v_islandgroup,
v_island,
v_country,
v_countrycode,
v_stateprovince,
v_county,
v_municipality,
v_locality,
v_verbatimlocality,
v_minimumelevationinmeters,
v_maximumelevationinmeters,
v_verbatimelevation,
v_verticaldatum,
v_minimumdepthinmeters,
v_maximumdepthinmeters,
v_verbatimdepth,
v_minimumdistanceabovesurfaceinmeters,
v_maximumdistanceabovesurfaceinmeters,
v_locationaccordingto,
v_locationremarks,
v_decimallatitude,
v_decimallongitude,
v_geodeticdatum,
v_coordinateuncertaintyinmeters,
v_coordinateprecision,
v_pointradiusspatialfit,
v_verbatimcoordinates,
v_verbatimlatitude,
v_verbatimlongitude,
v_verbatimcoordinatesystem,
v_verbatimsrs,
v_footprintwkt,
v_footprintsrs,
v_footprintspatialfit,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
interpreted_countrycode,
interpreted_decimallatitude,
interpreted_decimallongitude;
-- End table temp_locations_gbif_distinct
--------------------------------------------------------------------------------
-- Make table occurrence_location_lookup
CREATE OR REPLACE TABLE localityservice.gbif.occurrence_location_lookup
AS
SELECT
gbifid,
basisofrecord,
occurrencestatus,
kingdom,
phylum,
class,
`order`,
family,
genus,
species,
scientificname,
countrycode,
v_country,
v_countrycode,
year,
SUBSTR(eventdate,1,10) as shorteventdate,
publishingcountry,
publishingorgkey,
datasetkey,
institutioncode,
collectioncode,
SHA256(CONCAT(
'dwc:highergeographyid', IFNULL(v_highergeographyid,''),
'dwc:highergeography', IFNULL(v_highergeography,''),
'dwc:continent', IFNULL(v_continent,''),
'dwc:waterbody', IFNULL(v_waterbody,''),
'dwc:islandgroup', IFNULL(v_islandgroup,''),
'dwc:island', IFNULL(v_island,''),
'dwc:country', IFNULL(v_country,''),
'dwc:countrycode', IFNULL(v_countrycode,''),
'dwc:stateprovince', IFNULL(v_stateprovince,''),
'dwc:county', IFNULL(v_county,''),
'dwc:municipality', IFNULL(v_municipality,''),
'dwc:locality', IFNULL(v_locality,''),
'dwc:verbatimlocality', IFNULL(v_verbatimlocality,''),
'dwc:minimumelevationinmeters', IFNULL(v_minimumelevationinmeters,''),
'dwc:maximumelevationinmeters', IFNULL(v_maximumelevationinmeters,''),
'dwc:verbatimelevation', IFNULL(v_verbatimelevation,''),
'dwc:verticaldatum', IFNULL(v_verticaldatum,''),
'dwc:minimumdepthinmeters', IFNULL(v_minimumdepthinmeters,''),
'dwc:maximumdepthinmeters', IFNULL(v_maximumdepthinmeters,''),
'dwc:verbatimdepth', IFNULL(v_verbatimdepth,''),
'dwc:minimumdistanceabovesurfaceinmeters', IFNULL(v_minimumdistanceabovesurfaceinmeters,''),
'dwc:maximumdistanceabovesurfaceinmeters', IFNULL(v_maximumdistanceabovesurfaceinmeters,''),
'dwc:locationaccordingto', IFNULL(v_locationaccordingto,''),
'dwc:locationremarks', IFNULL(v_locationremarks,''),
'dwc:decimallatitude', IFNULL(v_decimallatitude,''),
'dwc:decimallongitude', IFNULL(v_decimallongitude,''),
'dwc:geodeticdatum', IFNULL(v_geodeticdatum,''),
'dwc:coordinateuncertaintyinmeters', IFNULL(v_coordinateuncertaintyinmeters,''),
'dwc:coordinateprecision', IFNULL(v_coordinateprecision,''),
'dwc:pointradiusspatialfit', IFNULL(v_pointradiusspatialfit,''),
'dwc:verbatimcoordinates', IFNULL(v_verbatimcoordinates,''),
'dwc:verbatimlatitude', IFNULL(v_verbatimlatitude,''),
'dwc:verbatimlongitude', IFNULL(v_verbatimlongitude,''),
'dwc:verbatimcoordinatesystem', IFNULL(v_verbatimcoordinatesystem,''),
'dwc:verbatimsrs', IFNULL(v_verbatimsrs,''),
'dwc:footprintwkt', IFNULL(v_footprintwkt,''),
'dwc:footprintsrs', IFNULL(v_footprintsrs,''),
'dwc:footprintspatialfit', IFNULL(v_footprintspatialfit,''),
'dwc:georeferencedby', IFNULL(v_georeferencedby,''),
'dwc:georeferenceddate', IFNULL(v_georeferenceddate,''),
'dwc:georeferenceprotocol', IFNULL(v_georeferenceprotocol,''),
'dwc:georeferencesources', IFNULL(v_georeferencesources,''),
'dwc:georeferenceremarks', IFNULL(v_georeferenceremarks,'')))
AS dwc_location_hash
FROM `localityservice.gbif.occurrences` t;
-- End table occurrence_location_lookup
--------------------------------------------------------------------------------
-- Make table locations_gbif_distinct
-- In this step we discard duplicates created due to distinct interpretations by GBIF
-- of interpreted_countrycode at exactly the same coordinates. It's not entirely clear
-- why this happens, but may be due records with the same location passing through the
-- processing pipeline at different times and methods or underlying data. In any case, 
-- we need to remove them.
CREATE OR REPLACE TABLE localityservice.gbif.locations_gbif_distinct
AS
SELECT dwc_location_hash, v_highergeographyid, v_highergeography, v_continent, v_waterbody, v_islandgroup, v_island, v_country, v_countrycode, v_stateprovince, v_county, v_municipality, v_locality, v_verbatimlocality, v_minimumelevationinmeters, v_maximumelevationinmeters, v_verbatimelevation, v_verticaldatum, v_minimumdepthinmeters, v_maximumdepthinmeters, v_verbatimdepth, v_minimumdistanceabovesurfaceinmeters, v_maximumdistanceabovesurfaceinmeters, v_locationaccordingto, v_locationremarks, v_decimallatitude, v_decimallongitude, v_geodeticdatum, v_coordinateuncertaintyinmeters, v_coordinateprecision, v_pointradiusspatialfit, v_verbatimcoordinates, v_verbatimlatitude, v_verbatimlongitude, v_verbatimcoordinatesystem, v_verbatimsrs, v_footprintwkt, v_footprintsrs, v_footprintspatialfit, v_georeferencedby, v_georeferenceddate, v_georeferenceprotocol, v_georeferencesources, v_georeferenceremarks, interpreted_decimallatitude, interpreted_decimallongitude, interpreted_countrycode, occcount, UPPER(v_geodeticdatum) AS u_datumstr,
REGEXP_REPLACE(REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_tokens)),NFKC)),r"[\s]+",' '), r"^\s+|\s+$", '') AS tokens,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match_with_coords)),NFKC)),r"[\s]+",'') AS matchme_with_coords,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match)),NFKC)),r"[\s]+",'') AS matchme,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match_sans_coords)),NFKC)),r"[\s]+",'') AS matchme_sans_coords
FROM temp_locations_gbif_distinct
WHERE dwc_location_hash NOT IN (SELECT dwc_location_hash
FROM temp_locations_gbif_distinct
GROUP BY dwc_location_hash
HAVING count(*)>1);
-- End table locations_gbif_distinct
--------------------------------------------------------------------------------
-- Make table locations_distinct_with_epsg
CREATE OR REPLACE TABLE localityservice.gbif.locations_distinct_with_epsg
AS
SELECT a.*, epsg
FROM `localityservice.gbif.locations_gbif_distinct` a
LEFT JOIN `localityservice.vocabs.datumsepsg` b
ON a.u_datumstr=b.u_datumstr;
-- End table locations_distinct_with_epsg
--------------------------------------------------------------------------------
-- Make table locations_distinct_with_scores
CREATE OR REPLACE TABLE localityservice.gbif.locations_distinct_with_scores
AS
SELECT *,
IF(v_georeferenceprotocol IS NULL,0,16) +
IF(v_georeferencesources IS NULL,0,8) +
IF(v_georeferenceddate IS NULL,0,4) +
IF(v_georeferencedby IS NULL,0,2) +
IF(v_georeferenceremarks IS NULL,0,1)
AS georef_score,
IF(interpreted_decimallatitude IS NULL or interpreted_decimallongitude IS NULL,0,128) +
IF(epsg IS NULL,0,64) +
IF(SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)>=1 AND
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)<20037509,32,0) +
IF(v_georeferenceprotocol IS NULL,0,16) +
IF(v_georeferencesources IS NULL,0,8) +
IF(v_georeferenceddate IS NULL,0,4) +
IF(v_georeferencedby IS NULL,0,2) +
IF(v_georeferenceremarks IS NULL,0,1)
AS coordinates_score,
'GBIF' AS source
FROM `localityservice.gbif.locations_distinct_with_epsg`;
-- End table locations_distinct_with_scores
END;
--------------------------------------------------------------------------------
