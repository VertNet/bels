--------------------------------------------------------------------------------
-- Load iDigBio - load_idigbio.sql  run time ~9 m
-- Script to prepare iDigBio Location data for the gazetteer.
-- Before running this script:
--   1) run the script load_gbif.sql
--   2) run the script load_vertnet.sql
--   3) load the latest source data into the table idigbio.occurrences in BigQuery.
--   4) reconcile and add any new values of u_datumstr with epsg codes to
--      table vocabs.datumsepsg
--   5) make sure the user defined functions saveNumbers(), removeSymbols(), and
--      simplifyDiacritics() are created.
--------------------------------------------------------------------------------
BEGIN
-- Make table occurences_iso2
CREATE OR REPLACE TABLE `localityservice.idigbio.occurrences_iso2`
AS
SELECT
a.coreid,
continent AS v_continent,
coordinatePrecision AS v_coordinateprecision,
coordinateUncertaintyInMeters AS v_coordinateuncertaintyinmeters,
country AS v_country,
countryCode AS v_countrycode,
county AS v_county,
decimalLatitude AS v_decimallatitude,
decimalLongitude AS v_decimallongitude,
footprintSRS AS v_footprintsrs,
footprintSpatialFit AS v_footprintspatialfit,
footprintWKT AS v_footprintwkt,
geodeticDatum AS v_geodeticdatum,
georeferenceProtocol AS v_georeferenceprotocol,
georeferenceRemarks AS v_georeferenceremarks,
georeferenceSources AS v_georeferencesources,
georeferenceVerificationStatus AS v_georeferenceverificationstatus,
georeferencedBy AS v_georeferencedby,
georeferencedDate AS v_georeferenceddate,
higherGeography AS v_highergeography,
higherGeographyID AS v_highergeographyid,
island AS v_island,
islandGroup AS v_islandgroup,
locality AS v_locality,
'' AS v_minimumdistanceabovesurfaceinmeters,
'' AS v_maximumdistanceabovesurfaceinmeters,
locationAccordingTo AS v_locationaccordingto,
locationID AS v_locationid,
locationRemarks AS v_locationremarks,
maximumDepthInMeters AS v_maximumdepthinmeters,
maximumElevationInMeters AS v_maximumelevationinmeters,
minimumDepthInMeters AS v_minimumdepthinmeters,
minimumElevationInMeters AS v_minimumelevationinmeters,
municipality AS v_municipality,
pointRadiusSpatialFit AS v_pointradiusspatialfit,
stateProvince AS v_stateprovince,
verbatimCoordinateSystem AS v_verbatimcoordinatesystem,
verbatimCoordinates AS v_verbatimcoordinates,
verbatimDepth AS v_verbatimdepth,
verbatimElevation AS v_verbatimelevation,
'' AS v_verticaldatum,
verbatimLatitude AS v_verbatimlatitude,
verbatimLocality AS v_verbatimlocality,
verbatimLongitude AS v_verbatimlongitude,
verbatimSRS AS v_verbatimsrs,
waterBody AS v_waterbody,
SAFE_CAST(idigbio_decimallatitude_wgs84 AS FLOAT64) AS interpreted_decimallatitude,
SAFE_CAST(idigbio_decimallongitude_wgs84 AS FLOAT64) AS interpreted_decimallongitude,
b.iso2 AS interpreted_countrycode,
upper(geodeticdatum) AS u_datumstr,
c.* EXCEPT (coreid)
FROM
`localityservice.idigbio.occurrences` a JOIN
`localityservice.idigbio_20210211.idigbio_taxonomy_20210319` c ON a.coreid=c.coreid
LEFT JOIN
`localityservice.vocabs.iso3_to_2` AS b ON a.idigbio_countrycode=b.iso3;
--------------------------------------------------------------------------------
-- Make table occurrence_location_lookup
CREATE OR REPLACE TABLE localityservice.idigbio.occurrence_location_lookup
AS
SELECT
coreid,
kingdom,
phylum,
class,
`order`,
'' AS family, -- missing in latest snapshot
genus,
specificepithet,
TRIM(CONCAT(IFNULL(genus,''),' ',IFNULL(specificepithet,''))) as species,
scientificname,
interpreted_countrycode,
v_country,
v_countrycode,
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
FROM `localityservice.idigbio.occurrences_iso2`;
-- End table occurrence_location_lookup
--------------------------------------------------------------------------------
-- Make table temp_locations_idigbio_distinct
CREATE TEMP TABLE temp_locations_idigbio_distinct
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
IFNULL(v_waterbody,"")," ",
IFNULL(v_islandgroup,"")," ",
IFNULL(v_island,"")," ",
IFNULL(interpreted_countrycode,"")," ",
IFNULL(v_stateprovince,"")," ",
IFNULL(v_county,"")," ",
IFNULL(v_municipality,"")," ",
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
  IF(v_locality IS NULL,v_verbatimlocality,
    IF(v_verbatimlocality IS NULL, v_locality,
      IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
        CONCAT(v_locality,' ',v_verbatimlocality)
      )
    )
  )
),' ',
IFNULL(v_minimumelevationinmeters,"")," ",
IFNULL(v_maximumelevationinmeters,"")," ",
IFNULL(v_verbatimelevation,"")," ",
IFNULL(v_verticaldatum,"")," ",
IFNULL(v_minimumdepthinmeters,"")," ",
IFNULL(v_maximumdepthinmeters,"")," ",
IFNULL(v_verbatimdepth,"")," ",
IFNULL(v_verbatimcoordinates,"")," ",
IFNULL(v_verbatimlatitude,"")," ",
IFNULL(v_verbatimlongitude,"")," ",
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallatitude AS NUMERIC))/10000000 AS STRING),"")," ",
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallongitude AS NUMERIC))/10000000 AS STRING),""))
AS for_match_with_coords,
CONCAT(
IFNULL(v_waterbody,"")," ",
IFNULL(v_islandgroup,"")," ",
IFNULL(v_island,"")," ",
IFNULL(interpreted_countrycode,"")," ",
IFNULL(v_stateprovince,"")," ",
IFNULL(v_county,"")," ",
IFNULL(v_municipality,"")," ",
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
  IF(v_locality IS NULL,v_verbatimlocality,
    IF(v_verbatimlocality IS NULL, v_locality,
      IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
        CONCAT(v_locality,' ',v_verbatimlocality)
      )
    )
  )
),' ',
IFNULL(v_minimumelevationinmeters,"")," ",
IFNULL(v_maximumelevationinmeters,"")," ",
IFNULL(v_verbatimelevation,"")," ",
IFNULL(v_verticaldatum,"")," ",
IFNULL(v_minimumdepthinmeters,"")," ",
IFNULL(v_maximumdepthinmeters,"")," ",
IFNULL(v_verbatimdepth,"")," ",
IFNULL(v_verbatimcoordinates,"")," ",
IFNULL(v_verbatimlatitude,"")," ",
IFNULL(v_verbatimlongitude,""))
AS for_match,
CONCAT(
IFNULL(v_waterbody,"")," ",
IFNULL(v_islandgroup,"")," ",
IFNULL(v_island,"")," ",
IFNULL(interpreted_countrycode,"")," ",
IFNULL(v_stateprovince,"")," ",
IFNULL(v_county,"")," ",
IFNULL(v_municipality,"")," ",
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
  IF(v_locality IS NULL,v_verbatimlocality,
    IF(v_verbatimlocality IS NULL, v_locality,
      IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
        CONCAT(v_locality,' ',v_verbatimlocality)
      )
    )
  )
),' ',
IFNULL(v_minimumelevationinmeters,"")," ",
IFNULL(v_maximumelevationinmeters,"")," ",
IFNULL(v_verbatimelevation,"")," ",
IFNULL(v_verticaldatum,"")," ",
IFNULL(v_minimumdepthinmeters,"")," ",
IFNULL(v_maximumdepthinmeters,"")," ",
IFNULL(v_verbatimdepth,""))
AS for_match_sans_coords,
CONCAT(
IFNULL(v_continent,"")," ",
IFNULL(v_highergeography,"")," ",
IFNULL(v_waterbody,"")," ",
IFNULL(v_islandgroup,"")," ",
IFNULL(v_island,"")," ",
IFNULL(v_country,"")," ",
IFNULL(v_countrycode,"")," ",
IFNULL(v_stateprovince,"")," ",
IFNULL(v_county,"")," ",
IFNULL(v_municipality,"")," ",
IF(v_locality IS NULL AND v_verbatimlocality IS NULL, '',
  IF(v_locality IS NULL,v_verbatimlocality,
    IF(v_verbatimlocality IS NULL, v_locality,
      IF(upper(v_locality)=upper(v_verbatimlocality),v_locality,
        CONCAT(v_locality,' ',v_verbatimlocality)
      )
    )
  )
),' ',
IFNULL(v_locationremarks,"")," ",
IFNULL(v_georeferencedby,"")," ",
IFNULL(v_georeferenceddate,"")," ",
IFNULL(v_georeferencesources,"")," ",
IFNULL(v_georeferenceprotocol,"")," ",
IFNULL(v_georeferenceremarks,"")," ",
IFNULL(interpreted_countrycode,""))
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
interpreted_decimallatitude,
interpreted_decimallongitude,
interpreted_countrycode,
u_datumstr,
count(*) AS occcount
FROM `localityservice.idigbio.occurrences_iso2`
GROUP BY
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
interpreted_decimallongitude,
u_datumstr;
-- End table temp_locations_idigbio_distinct
--------------------------------------------------------------------------------
-- Make table locations_idigbio_distinct
-- In this step we discard duplicates created due to distinct interpretations by iDigBio
-- of interpreted_coordinates at exactly the same coordinates. It seems to be that 
-- rules for coordinate precision in the interpreted coordinates changed at some point,
-- and that the same Location can have distinct interpreted coordinates. In the 2021 snapshot
-- there are 1990 dwc_location_hash values with multiple entries. These have to be removed.
CREATE OR REPLACE TABLE localityservice.idigbio.locations_idigbio_distinct
AS
SELECT dwc_location_hash, v_highergeographyid, v_highergeography, v_continent,
v_waterbody, v_islandgroup, v_island, v_country, v_countrycode, v_stateprovince,
v_county, v_municipality, v_locality, v_verbatimlocality, v_minimumelevationinmeters,
v_maximumelevationinmeters, v_verbatimelevation, v_verticaldatum,
v_minimumdepthinmeters, v_maximumdepthinmeters, v_verbatimdepth,
v_minimumdistanceabovesurfaceinmeters, v_maximumdistanceabovesurfaceinmeters,
v_locationaccordingto, v_locationremarks, v_decimallatitude, v_decimallongitude,
v_geodeticdatum, v_coordinateuncertaintyinmeters, v_coordinateprecision,
v_pointradiusspatialfit, v_verbatimcoordinates, v_verbatimlatitude,
v_verbatimlongitude, v_verbatimcoordinatesystem, v_verbatimsrs, v_footprintwkt,
v_footprintsrs, v_footprintspatialfit, v_georeferencedby, v_georeferenceddate,
v_georeferenceprotocol, v_georeferencesources, v_georeferenceremarks,
interpreted_decimallatitude, interpreted_decimallongitude, interpreted_countrycode,
occcount, u_datumstr,
REGEXP_REPLACE(REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_tokens)),NFKC)),r"[\s]+",' '), r"^\s+|\s+$", '') AS tokens,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match_with_coords)),NFKC)),r"[\s]+",'') AS matchme_with_coords,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match)),NFKC)),r"[\s]+",'') AS matchme,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(for_match_sans_coords)),NFKC)),r"[\s]+",'') AS matchme_sans_coords
FROM temp_locations_idigbio_distinct
WHERE dwc_location_hash NOT IN (SELECT dwc_location_hash
FROM temp_locations_idigbio_distinct
GROUP BY dwc_location_hash
HAVING count(*)>1);
-- End of CREATE table locations_idigbio_distinct
--------------------------------------------------------------------------------
-- Make table locations_distinct_with_epsg
CREATE OR REPLACE TABLE localityservice.idigbio.locations_distinct_with_epsg
AS
SELECT a.*, epsg
FROM `localityservice.idigbio.locations_idigbio_distinct` a
LEFT JOIN `localityservice.vocabs.datumsepsg` b
ON a.u_datumstr=b.u_datumstr;
-- End table locations_distinct_with_epsg
--------------------------------------------------------------------------------
-- Make table locations_distinct_with_scores
CREATE OR REPLACE TABLE localityservice.idigbio.locations_distinct_with_scores
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
'iDigBio' AS source
FROM `localityservice.idigbio.locations_distinct_with_epsg`
WHERE dwc_location_hash NOT IN (SELECT dwc_location_hash
FROM `localityservice.idigbio.locations_distinct_with_epsg`
GROUP BY dwc_location_hash
HAVING count(*)>1);
-- End table locations_distinct_with_scores
END;
--------------------------------------------------------------------------------
