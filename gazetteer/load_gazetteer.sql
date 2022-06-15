Gazetteer Loading Script (runtime ~35m?)
------------------------------------------ 
-- Script to prepare georeference data for the gazetteer
-- Before running this script, run the loading scripts for each of the data sources (GBIF, iDigBio, VertNet) that have been affected by any changes.
------------------------------------------ 
BEGIN
-- Make table locations_distinct_with_scores (n=184,059,656)
-- Begin by loading all GBIF distinct Locations with scores that have no more than
-- one entry for the dwc_location_hash (there should be no duplicates under normal
-- conditions.
CREATE OR REPLACE TABLE localityservice.gazetteer.locations_distinct_with_scores
AS
SELECT *
FROM `localityservice.gbif.locations_distinct_with_scores`
WHERE dwc_location_hash NOT IN 
(
SELECT dwc_location_hash
FROM `localityservice.gbif.locations_distinct_with_scores`
GROUP BY dwc_location_hash
HAVING count(*)>1
);
 
--  Continue by loading all VertNet distinct Locations with scores that can't be
--  found in the GBIF distinct Locations.
INSERT INTO `localityservice.gazetteer.locations_distinct_with_scores`
(SELECT *
FROM localityservice.vertnet.locations_distinct_with_scores
WHERE
dwc_location_hash NOT IN
(SELECT dwc_location_hash
FROM localityservice.gazetteer.locations_distinct_with_scores)
);
 
--  Continue by loading all iDigBio distinct Locations with scores that can't be
--  found in the distinct Locations from GBIF and VertNet.
INSERT INTO `localityservice.gazetteer.locations_distinct_with_scores`
(SELECT *
FROM localityservice.idigbio.locations_distinct_with_scores
WHERE
dwc_location_hash NOT IN
(SELECT dwc_location_hash
FROM localityservice.gazetteer.locations_distinct_with_scores)
);
------------------------------------------ 
-- Make table locations_with_georefs_combined (n=80,152,608)
-- This table includes only the Locations with realistic georeferences and with precision standardized to the nearest meter for coordinateUncertaintyInMeters and to seven decimals for the coordinates (following Chapman & Wieczorek 2020).
CREATE OR REPLACE TABLE localityservice.gazetteer.locations_with_georefs_combined
AS
SELECT *,
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC) AS unc_numeric,
ROUND(10000000*SAFE_CAST(interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
ROUND(SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)) AS bels_coordinateuncertaintyinmeters,
ST_GEOGPOINT(ROUND(10000000*SAFE_CAST(interpreted_decimallongitude AS NUMERIC))/10000000, ROUND(10000000*SAFE_CAST(interpreted_decimallatitude AS NUMERIC))/10000000) as center
FROM `localityservice.gazetteer.locations_distinct_with_scores`
WHERE
epsg IS NOT NULL AND
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)>=1 AND
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)<20037509 AND
interpreted_decimallatitude IS NOT NULL AND
interpreted_decimallongitude IS NOT NULL AND
interpreted_decimallatitude<>0 AND interpreted_decimallongitude<>0;
------------------------------------------
-- Make table 01_matchme_sans_coords_with_georefs_biggest (n=76,009,144)
-- Begin the separate processing for each match string type.
-- This table includes the one georeference for a matching string at a given set of
-- coordinates (rounded to 7 decimals) that has the greatest uncertainty among
-- those at those coordinates.
CREATE OR REPLACE TABLE localityservice.gazetteer.01_matchme_sans_coords_with_georefs_biggest
AS
SELECT
matchme_sans_coords,
max(bels_coordinateuncertaintyinmeters) AS big_uncertainty,
bels_decimallongitude,
bels_decimallatitude,
ST_GEOGPOINT(bels_decimallongitude, bels_decimallatitude) as center,
count(*) as georef_count
FROM `localityservice.gazetteer.locations_with_georefs_combined`
GROUP BY
matchme_sans_coords,
bels_decimallongitude,
bels_decimallatitude;
------------------------------------------
-- Make table 01_matchme_verbatim_coords_with_georefs_biggest (n=76,219,959)
-- Begin the separate processing for each match string type.
-- This table includes the one georeference for a matching string at a given set of
-- coordinates (rounded to 7 decimals) that has the greatest uncertainty among
-- those at those coordinates
CREATE OR REPLACE TABLE localityservice.gazetteer.01_matchme_verbatim_coords_with_georefs_biggest
AS
SELECT
matchme,
max(bels_coordinateuncertaintyinmeters) AS big_uncertainty,
bels_decimallongitude,
bels_decimallatitude,
ST_GEOGPOINT(bels_decimallongitude, bels_decimallatitude) as center,
count(*) as georef_count
FROM `localityservice.gazetteer.locations_with_georefs_combined`
GROUP BY
matchme,
bels_decimallongitude,
bels_decimallatitude;
------------------------------------------
-- Make table 01_matchme_with_coords_with_georefs_biggest (n=77,125,510)
-- Begin the separate processing for each match string type.
-- This table includes the one georeference for a matching string at a given set of
-- coordinates (rounded to 7 decimals) that has the greatest uncertainty among
-- the georeferences at those coordinates.
CREATE OR REPLACE TABLE localityservice.gazetteer.01_matchme_with_coords_with_georefs_biggest
AS
SELECT
matchme_with_coords,
max(bels_coordinateuncertaintyinmeters) AS big_uncertainty,
bels_decimallongitude,
bels_decimallatitude,
ST_GEOGPOINT(bels_decimallongitude, bels_decimallatitude) as center,
count(*) as georef_count
FROM `localityservice.gazetteer.locations_with_georefs_combined`
GROUP BY
matchme_with_coords,
bels_decimallongitude,
bels_decimallatitude;
------------------------------------------
-- Make table 02_matchme_sans_coords_max_uncertainty (n=17,587,059)
CREATE OR REPLACE TABLE localityservice.gazetteer.02_matchme_sans_coords_max_uncertainty
AS
SELECT
matchme_sans_coords,
max(big_uncertainty) as max_uncertainty
FROM
`localityservice.gazetteer.01_matchme_sans_coords_with_georefs_biggest`
GROUP BY
matchme_sans_coords;
------------------------------------------
-- Make table 02_matchme_verbatim_coords_max_uncertainty (n=22,872,572)
CREATE OR REPLACE TABLE localityservice.gazetteer.02_matchme_verbatim_coords_max_uncertainty
AS
SELECT
matchme,
max(big_uncertainty) as max_uncertainty
FROM
`localityservice.gazetteer.01_matchme_verbatim_coords_with_georefs_biggest`
GROUP BY
matchme;
------------------------------------------
-- Make table 02_matchme_with_coords_max_uncertainty (n=77,070,692)
CREATE OR REPLACE TABLE localityservice.gazetteer.02_matchme_with_coords_max_uncertainty
AS
SELECT
matchme_with_coords,
max(big_uncertainty) as max_uncertainty
FROM
`localityservice.gazetteer.01_matchme_with_coords_with_georefs_biggest`
GROUP BY
matchme_with_coords;
------------------------------------------
-- Make table 03_matchme_sans_coords_max_mins (n=17,587,059)
CREATE OR REPLACE TABLE localityservice.gazetteer.03_matchme_sans_coords_max_mins
AS
SELECT
matchme_sans_coords,
max(bels_decimallatitude) AS maxlat,
max(bels_decimallongitude) AS maxlong,
min(bels_decimallatitude) AS minlat,
min(bels_decimallongitude) AS minlong,
max(big_uncertainty) AS max_uncertainty
FROM
`localityservice.gazetteer.01_matchme_sans_coords_with_georefs_biggest`
GROUP BY
matchme_sans_coords;
------------------------------------------
-- Make table 03_matchme_verbatim_coords_max_mins (n=22,872,572)
CREATE OR REPLACE TABLE localityservice.gazetteer.03_matchme_verbatim_coords_max_mins
AS
SELECT
matchme,
max(bels_decimallatitude) AS maxlat,
max(bels_decimallongitude) AS maxlong,
min(bels_decimallatitude) AS minlat,
min(bels_decimallongitude) AS minlong,
max(big_uncertainty) AS max_uncertainty
FROM
`localityservice.gazetteer.01_matchme_verbatim_coords_with_georefs_biggest`
GROUP BY
matchme;
------------------------------------------
-- Make table 03_matchme_with_coords_max_mins (n=77,070,692)
CREATE OR REPLACE TABLE localityservice.gazetteer.03_matchme_with_coords_max_mins
AS
SELECT
matchme_with_coords,
max(bels_decimallatitude) AS maxlat,
max(bels_decimallongitude) AS maxlong,
min(bels_decimallatitude) AS minlat,
min(bels_decimallongitude) AS minlong,
max(big_uncertainty) AS max_uncertainty
FROM
`localityservice.gazetteer.01_matchme_with_coords_with_georefs_biggest`
GROUP BY
matchme_with_coords;
------------------------------------------
-- Make table 04_matchme_sans_coords_bb_prep (n=16,235,966)
CREATE OR REPLACE TABLE localityservice.gazetteer.04_matchme_sans_coords_bb_prep
AS
SELECT
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))
AS nsdist,
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))
AS ewdist,
max_uncertainty,
matchme_sans_coords
FROM
`localityservice.gazetteer.03_matchme_sans_coords_max_mins`
WHERE
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty;
------------------------------------------
-- Make table 04_matchme_verbatim_coords_bb_prep (n=21,778,895)
CREATE OR REPLACE TABLE localityservice.gazetteer.04_matchme_verbatim_coords_bb_prep
AS
SELECT
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))
AS nsdist,
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))
AS ewdist,
max_uncertainty,
matchme
FROM
`localityservice.gazetteer.03_matchme_verbatim_coords_max_mins`
WHERE
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty;
------------------------------------------
-- Make table 04_matchme_with_coords_bb_prep (n=77,070,479)
CREATE OR REPLACE TABLE localityservice.gazetteer.04_matchme_with_coords_bb_prep
AS
SELECT
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))
AS nsdist,
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))
AS ewdist,
max_uncertainty,
matchme_with_coords
FROM
`localityservice.gazetteer.03_matchme_with_coords_max_mins`
WHERE
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty;
------------------------------------------
-- Make table 05_matchme_sans_coords_centers (n=45,003,656)
CREATE OR REPLACE TABLE localityservice.gazetteer.05_matchme_sans_coords_centers
AS
SELECT
a.matchme_sans_coords, center
FROM
`localityservice.gazetteer.01_matchme_sans_coords_with_georefs_biggest` a,
`localityservice.gazetteer.04_matchme_sans_coords_bb_prep` b
WHERE
a.matchme_sans_coords=b.matchme_sans_coords;
------------------------------------------
-- Make table 05_matchme_verbatim_coords_centers (n=48,482,874)
CREATE OR REPLACE TABLE localityservice.gazetteer.05_matchme_verbatim_coords_centers
AS
SELECT
a.matchme, center
FROM
`localityservice.gazetteer.01_matchme_verbatim_coords_with_georefs_biggest` a,
`localityservice.gazetteer.04_matchme_verbatim_coords_bb_prep` b
WHERE
a.matchme=b.matchme;
------------------------------------------
 
-- Make table 05_matchme_with_coords_centers (n=77,125,079)
CREATE OR REPLACE TABLE localityservice.gazetteer.05_matchme_with_coords_centers
AS
SELECT
a.matchme_with_coords, center
FROM
`localityservice.gazetteer.01_matchme_with_coords_with_georefs_biggest` a,
`localityservice.gazetteer.04_matchme_with_coords_bb_prep` b
WHERE
a.matchme_with_coords=b.matchme_with_coords;
------------------------------------------
-- Make table 06_matchme_sans_coords_agg_centers (n=16,235,966)
CREATE OR REPLACE TABLE localityservice.gazetteer.06_matchme_sans_coords_agg_centers
AS
SELECT
matchme_sans_coords,
count(*) as centerscount,
ARRAY_AGG(center) as centers
FROM
`localityservice.gazetteer.05_matchme_sans_coords_centers`
GROUP BY matchme_sans_coords;
------------------------------------------
-- Make table 06_matchme_verbatim_coords_agg_centers (n=21,778,895)
CREATE OR REPLACE TABLE localityservice.gazetteer.06_matchme_verbatim_coords_agg_centers
AS
SELECT
matchme,
count(*) as centerscount,
ARRAY_AGG(center) as centers
FROM
`localityservice.gazetteer.05_matchme_verbatim_coords_centers`
GROUP BY matchme;
------------------------------------------
-- Make table 06_matchme_with_coords_agg_centers (n=77,070,479)
CREATE OR REPLACE TABLE localityservice.gazetteer.06_matchme_with_coords_agg_centers
AS
SELECT
matchme_with_coords,
count(*) as centerscount,
ARRAY_AGG(center) as centers
FROM
`localityservice.gazetteer.05_matchme_with_coords_centers`
GROUP BY matchme_with_coords;
------------------------------------------
-- Make table 07_matchme_sans_coords_centroids (n=16,235,966)
CREATE OR REPLACE TABLE localityservice.gazetteer.07_matchme_sans_coords_centroids
AS
SELECT
matchme_sans_coords,
centerscount,
(
SELECT ST_CENTROID_AGG(p) AS st_centroid_agg
FROM
(SELECT * FROM UNNEST(centers) AS p)
) as centroid
FROM `localityservice.gazetteer.06_matchme_sans_coords_agg_centers`;
------------------------------------------
-- Make table 07_matchme_verbatim_coords_centroids (n=21,778,895)
CREATE OR REPLACE TABLE localityservice.gazetteer.07_matchme_verbatim_coords_centroids
AS
SELECT
matchme,
centerscount,
(
SELECT ST_CENTROID_AGG(p) AS st_centroid_agg
FROM
(SELECT * FROM UNNEST(centers) AS p)
) as centroid
FROM `localityservice.gazetteer.06_matchme_verbatim_coords_agg_centers`;
------------------------------------------
-- Make table 07_matchme_with_coords_centroids (n=77,070,479)
CREATE OR REPLACE TABLE localityservice.gazetteer.07_matchme_with_coords_centroids
AS
SELECT
matchme_with_coords,
centerscount,
(
SELECT ST_CENTROID_AGG(p) AS st_centroid_agg
FROM
(SELECT * FROM UNNEST(centers) AS p)
) as centroid
FROM `localityservice.gazetteer.06_matchme_with_coords_agg_centers`;
------------------------------------------
-- Make table 08_matchme_sans_coords_min_centroid_dist (n=16,235,966)
CREATE OR REPLACE TABLE localityservice.gazetteer.08_matchme_sans_coords_min_centroid_dist
AS
SELECT
a.matchme_sans_coords,
min(ST_DISTANCE(b.center, centroid)) as min_centroid_dist
FROM
`localityservice.gazetteer.07_matchme_sans_coords_centroids` a,
`localityservice.gazetteer.01_matchme_sans_coords_with_georefs_biggest` b
WHERE
a.matchme_sans_coords=b.matchme_sans_coords
GROUP BY
a.matchme_sans_coords;
------------------------------------------
-- Make table 08_matchme_verbatim_coords_min_centroid_dist (n=21,778,895)
CREATE OR REPLACE TABLE localityservice.gazetteer.08_matchme_verbatim_coords_min_centroid_dist
AS
SELECT
a.matchme,
min(ST_DISTANCE(b.center, centroid)) as min_centroid_dist
FROM
`localityservice.gazetteer.07_matchme_verbatim_coords_centroids` a,
`localityservice.gazetteer.01_matchme_verbatim_coords_with_georefs_biggest` b
WHERE
a.matchme=b.matchme
GROUP BY
a.matchme;
------------------------------------------
-- Make table 08_matchme_with_coords_min_centroid_dist (n=77,070,479)
CREATE OR REPLACE TABLE localityservice.gazetteer.08_matchme_with_coords_min_centroid_dist
AS
SELECT
a.matchme_with_coords,
min(ST_DISTANCE(b.center, centroid)) as min_centroid_dist
FROM
`localityservice.gazetteer.07_matchme_with_coords_centroids` a,
`localityservice.gazetteer.01_matchme_with_coords_with_georefs_biggest` b
WHERE
a.matchme_with_coords=b.matchme_with_coords
GROUP BY
a.matchme_with_coords;
------------------------------------------
-- Make table 09_matchme_sans_coords_best_candidates (n=15,403,195)
CREATE OR REPLACE TABLE localityservice.gazetteer.09_matchme_sans_coords_best_candidates
AS
SELECT
b.*,
ST_DISTANCE(center, centroid) as centroid_dist
FROM
`localityservice.gazetteer.08_matchme_sans_coords_min_centroid_dist` a,
`localityservice.gazetteer.01_matchme_sans_coords_with_georefs_biggest` b,
`localityservice.gazetteer.07_matchme_sans_coords_centroids` c,
`localityservice.gazetteer.02_matchme_sans_coords_max_uncertainty` d
WHERE
a.matchme_sans_coords=b.matchme_sans_coords AND
a.matchme_sans_coords=c.matchme_sans_coords AND
a.matchme_sans_coords=d.matchme_sans_coords AND
ST_DISTANCE(center, centroid)=min_centroid_dist AND
max_uncertainty=big_uncertainty;
------------------------------------------
-- Make table 09_matchme_verbatim_coords_best_candidates (n=15,403,195)
CREATE OR REPLACE TABLE localityservice.gazetteer.09_matchme_verbatim_coords_best_candidates
AS
SELECT
b.*,
ST_DISTANCE(center, centroid) as centroid_dist
FROM
`localityservice.gazetteer.08_matchme_verbatim_coords_min_centroid_dist` a,
`localityservice.gazetteer.01_matchme_verbatim_coords_with_georefs_biggest` b,
`localityservice.gazetteer.07_matchme_verbatim_coords_centroids` c,
`localityservice.gazetteer.02_matchme_verbatim_coords_max_uncertainty` d
WHERE
a.matchme=b.matchme AND
a.matchme=c.matchme AND
a.matchme=d.matchme AND
ST_DISTANCE(center, centroid)=min_centroid_dist AND
max_uncertainty=big_uncertainty;
------------------------------------------
-- Make table 09_matchme_with_coords_best_candidates (n=15,403,195)
CREATE OR REPLACE TABLE localityservice.gazetteer.09_matchme_with_coords_best_candidates
AS
SELECT
b.*,
ST_DISTANCE(center, centroid) as centroid_dist
FROM
`localityservice.gazetteer.08_matchme_with_coords_min_centroid_dist` a,
`localityservice.gazetteer.01_matchme_with_coords_with_georefs_biggest` b,
`localityservice.gazetteer.07_matchme_with_coords_centroids` c,
`localityservice.gazetteer.02_matchme_with_coords_max_uncertainty` d
WHERE
a.matchme_with_coords=b.matchme_with_coords AND
a.matchme_with_coords=c.matchme_with_coords AND
a.matchme_with_coords=d.matchme_with_coords AND
ST_DISTANCE(center, centroid)=min_centroid_dist AND
max_uncertainty=big_uncertainty;
------------------------------------------
-- Make table 10_matchme_sans_coords_max_dist (n=15,396,621)
CREATE OR REPLACE TABLE localityservice.gazetteer.10_matchme_sans_coords_max_dist
AS
SELECT
a.matchme_sans_coords,
max(ST_DISTANCE(a.center, b.center)) as max_dist
FROM
`localityservice.gazetteer.09_matchme_sans_coords_best_candidates` a,
`localityservice.gazetteer.01_matchme_sans_coords_with_georefs_biggest` b
WHERE a.matchme_sans_coords=b.matchme_sans_coords
GROUP BY
matchme_sans_coords;
------------------------------------------
-- Make table 10_matchme_verbatim_coords_max_dist (n=21,081,183)
CREATE OR REPLACE TABLE localityservice.gazetteer.10_matchme_verbatim_coords_max_dist
AS
SELECT
a.matchme,
max(ST_DISTANCE(a.center, b.center)) as max_dist
FROM
`localityservice.gazetteer.09_matchme_verbatim_coords_best_candidates` a,
`localityservice.gazetteer.01_matchme_verbatim_coords_with_georefs_biggest` b
WHERE a.matchme=b.matchme
GROUP BY
matchme;
------------------------------------------
-- Make table 10_matchme_with_coords_max_dist (n=77,068,968)
CREATE OR REPLACE TABLE localityservice.gazetteer.10_matchme_with_coords_max_dist
AS
SELECT
a.matchme_with_coords,
max(ST_DISTANCE(a.center, b.center)) as max_dist
FROM
`localityservice.gazetteer.09_matchme_with_coords_best_candidates` a,
`localityservice.gazetteer.01_matchme_with_coords_with_georefs_biggest` b
WHERE a.matchme_with_coords=b.matchme_with_coords
GROUP BY
matchme_with_coords;
------------------------------------------
-- Make table 11_matchme_sans_coords_best_georef (n=15,168,681)
CREATE OR REPLACE TABLE localityservice.gazetteer.11_matchme_sans_coords_best_georef
AS
SELECT
* EXCEPT(rn)
FROM
(
SELECT
c.*, georef_count,
ROW_NUMBER() OVER(PARTITION BY a.matchme_sans_coords ORDER BY georef_score DESC) AS rn
FROM
`localityservice.gazetteer.10_matchme_sans_coords_max_dist` a,
`localityservice.gazetteer.09_matchme_sans_coords_best_candidates` b,
`localityservice.gazetteer.locations_with_georefs_combined` c
WHERE
a.matchme_sans_coords=b.matchme_sans_coords AND
a.matchme_sans_coords=c.matchme_sans_coords AND
max_dist<=big_uncertainty
)
WHERE rn = 1;
------------------------------------------
-- Make table 11_matchme_verbatim_coords_best_georef (n=20,929,195)
CREATE OR REPLACE TABLE localityservice.gazetteer.11_matchme_verbatim_coords_best_georef
AS
SELECT
* EXCEPT(rn)
FROM
(
SELECT
c.*, georef_count,
ROW_NUMBER() OVER(PARTITION BY a.matchme ORDER BY georef_score DESC) AS rn
FROM
`localityservice.gazetteer.10_matchme_verbatim_coords_max_dist` a,
`localityservice.gazetteer.09_matchme_verbatim_coords_best_candidates` b,
`localityservice.gazetteer.locations_with_georefs_combined` c
WHERE
a.matchme=b.matchme AND
a.matchme=c.matchme AND
max_dist<=big_uncertainty
)
WHERE rn = 1;
------------------------------------------
-- Make table 11_matchme_with_coords_best_georef (n=77,068,804)
CREATE OR REPLACE TABLE localityservice.gazetteer.11_matchme_with_coords_best_georef
AS
SELECT
* EXCEPT(rn)
FROM
(
SELECT
c.*, georef_count,
ROW_NUMBER() OVER(PARTITION BY a.matchme_with_coords ORDER BY georef_score DESC) AS rn
FROM
`localityservice.gazetteer.10_matchme_with_coords_max_dist` a,
`localityservice.gazetteer.09_matchme_with_coords_best_candidates` b,
`localityservice.gazetteer.locations_with_georefs_combined` c
WHERE
a.matchme_with_coords=b.matchme_with_coords AND
a.matchme_with_coords=c.matchme_with_coords AND
max_dist<=big_uncertainty
)
WHERE rn = 1;
------------------------------------------
-- Make table 12_matchme_sans_coords_only_sans_georefs in gazetteer (n=66,467,253)
CREATE OR REPLACE TABLE `localityservice.gazetteer.12_matchme_sans_coords_only_sans_georefs`
AS
(
SELECT DISTINCT
matchme_sans_coords,
ROUND(10000000*SAFE_CAST(interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude
FROM `localityservice.gazetteer.locations_distinct_with_scores`
WHERE
coordinates_score>=128 and coordinates_score<224 AND
matchme_sans_coords NOT IN (
SELECT matchme_sans_coords
FROM
`localityservice.gazetteer.11_matchme_sans_coords_best_georef`
)
);
------------------------------------------
-- Make table 12_matchme_verbatim_coords_only_sans_georefs in gazetteer (n=67,936,093)
CREATE OR REPLACE TABLE `localityservice.gazetteer.12_matchme_verbatim_coords_only_sans_georefs`
AS
(
SELECT DISTINCT
matchme,
ROUND(10000000*SAFE_CAST(interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude
FROM `localityservice.gazetteer.locations_distinct_with_scores`
WHERE
coordinates_score>=128 and coordinates_score<224 AND
matchme NOT IN (
SELECT matchme
FROM
`localityservice.gazetteer.11_matchme_verbatim_coords_best_georef`
)
);
------------------------------------------
-- Make table 12_matchme_with_coords_only_sans_georefs in gazetteer (n=68,917,245)
CREATE OR REPLACE TABLE `localityservice.gazetteer.12_matchme_with_coords_only_sans_georefs`
AS
(
SELECT DISTINCT
matchme_with_coords,
ROUND(10000000*SAFE_CAST(interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude
FROM `localityservice.gazetteer.locations_distinct_with_scores`
WHERE
coordinates_score>=128 and coordinates_score<224 AND
matchme_with_coords NOT IN (
SELECT matchme_with_coords
FROM
`localityservice.gazetteer.11_matchme_with_coords_best_georef`
)
);
------------------------------------------
-- Make table 13_matchme_sans_coords_only in gazetteer (n=31,481,155)
CREATE OR REPLACE TABLE `localityservice.gazetteer.13_matchme_sans_coords_only`
AS
(
SELECT matchme_sans_coords
FROM `localityservice.gazetteer.12_matchme_sans_coords_only_sans_georefs`
GROUP BY matchme_sans_coords
HAVING count(*)=1
);
------------------------------------------
-- Make table 13_matchme_verbatim_coords_only in gazetteer (n=37,678,786)
CREATE OR REPLACE TABLE `localityservice.gazetteer.13_matchme_verbatim_coords_only`
AS
(
SELECT matchme
FROM `localityservice.gazetteer.12_matchme_verbatim_coords_only_sans_georefs`
GROUP BY matchme
HAVING count(*)=1
);
------------------------------------------
-- Make table 13_matchme_with_coords_only in gazetteer (n=68,220,394)
CREATE OR REPLACE TABLE `localityservice.gazetteer.13_matchme_with_coords_only`
AS
(
SELECT matchme_with_coords
FROM `localityservice.gazetteer.12_matchme_with_coords_only_sans_georefs`
GROUP BY matchme_with_coords
HAVING count(*)=1
);
------------------------------------------
-- Make table 14_matchme_with_coords_best_coords_only (n=68,220,394)
CREATE OR REPLACE TABLE localityservice.gazetteer.14_matchme_with_coords_best_coords_only
AS
SELECT
 * EXCEPT(rn)
FROM
(
SELECT
b.matchme_with_coords,
0 AS unc_numeric,
ST_GEOGPOINT(interpreted_decimallongitude, interpreted_decimallatitude) AS center,
interpreted_decimallongitude,
interpreted_decimallatitude,
interpreted_countrycode,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
georef_score,
source,
NULL AS georef_count,
NULL AS max_uncertainty,
NULL AS centroid_dist,
NULL AS min_centroid_dist,
SHA256(CONCAT(
IFNULL(b.matchme_with_coords,""),
"",
IFNULL(SAFE_CAST(interpreted_decimallatitude AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallongitude AS STRING),""),
IFNULL(interpreted_countrycode,""),
IFNULL(v_georeferencedby,""),
IFNULL(v_georeferenceddate,""),
IFNULL(v_georeferenceprotocol,""),
IFNULL(v_georeferencesources,""),
IFNULL(v_georeferenceremarks,"")))
AS matchid,
ROW_NUMBER() OVER(PARTITION BY a.matchme_with_coords ORDER BY b.georef_score DESC) AS rn
FROM
`localityservice.gazetteer.13_matchme_with_coords_only` a,
`localityservice.gazetteer.locations_distinct_with_scores` b
WHERE
a.matchme_with_coords=b.matchme_with_coords
)
WHERE rn = 1;
------------------------------------------
-- Make table 14_matchme_verbatim_coords_best_coords_only (n=37,678,786)
CREATE OR REPLACE TABLE localityservice.gazetteer.14_matchme_verbatim_coords_best_coords_only
AS
SELECT
* EXCEPT(rn)
FROM
(
SELECT
b.matchme,
0 AS unc_numeric,
ST_GEOGPOINT(interpreted_decimallongitude, interpreted_decimallatitude) AS center,
interpreted_decimallongitude,
interpreted_decimallatitude,
interpreted_countrycode,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
georef_score,
source,
NULL AS georef_count,
NULL AS max_uncertainty,
NULL AS centroid_dist,
NULL AS min_centroid_dist,
SHA256(CONCAT(
IFNULL(b.matchme,""),
"",
IFNULL(SAFE_CAST(interpreted_decimallatitude AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallongitude AS STRING),""),
IFNULL(interpreted_countrycode,""),
IFNULL(v_georeferencedby,""),
IFNULL(v_georeferenceddate,""),
IFNULL(v_georeferenceprotocol,""),
IFNULL(v_georeferencesources,""),
IFNULL(v_georeferenceremarks,"")))
AS matchid,
ROW_NUMBER() OVER(PARTITION BY a.matchme ORDER BY b.georef_score DESC) AS rn
FROM
`localityservice.gazetteer.13_matchme_verbatim_coords_only` a,
`localityservice.gazetteer.locations_distinct_with_scores` b
WHERE
a.matchme=b.matchme
)
WHERE rn = 1;
------------------------------------------
-- Make table 14_matchme_sans_coords_best_coords_only (n=31,481,155)
CREATE OR REPLACE TABLE localityservice.gazetteer.14_matchme_sans_coords_best_coords_only
AS
SELECT
* EXCEPT(rn)
FROM
(
SELECT
b.matchme_sans_coords,
0 AS unc_numeric,
ST_GEOGPOINT(interpreted_decimallongitude, interpreted_decimallatitude) AS center,
interpreted_decimallongitude,
interpreted_decimallatitude,
interpreted_countrycode,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
georef_score,
source,
NULL AS georef_count,
NULL AS max_uncertainty,
NULL AS centroid_dist,
NULL AS min_centroid_dist,
SHA256(CONCAT(
IFNULL(b.matchme_sans_coords,""),
"",
IFNULL(SAFE_CAST(interpreted_decimallatitude AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallongitude AS STRING),""),
IFNULL(interpreted_countrycode,""),
IFNULL(v_georeferencedby,""),
IFNULL(v_georeferenceddate,""),
IFNULL(v_georeferenceprotocol,""),
IFNULL(v_georeferencesources,""),
IFNULL(v_georeferenceremarks,"")))
AS matchid,
ROW_NUMBER() OVER(PARTITION BY a.matchme_sans_coords ORDER BY b.georef_score DESC) AS rn
FROM
`localityservice.gazetteer.13_matchme_sans_coords_only` a,
`localityservice.gazetteer.locations_distinct_with_scores` b
WHERE
a.matchme_sans_coords=b.matchme_sans_coords
)
WHERE rn = 1;
------------------------------------------
-- Check bels_match_type counts in table georef.
SELECT bels_match_type, count(*) as reps
FROM `localityservice.gazetteer.georefs`
GROUP BY bels_match_type
HAVING count(*)>1
ORDER BY reps DESC;
Row
bels_match_type
reps
 
1
original georeference
80155644
 
2
coords only match on entire Location
70093355
 
3
match using verbatim coords
2400584
 
4
match sans coords
1237108
 
5
match using verbatim coords - coords only
997060
 
6
match using coords
711277
 
7
match sans coords - coords only
307964
 
8
match using coords - coords only
77734
 

 
-- Make table georefs (n=155,980,726)
-- This is meant to be a table containing the single best georeference
-- (coordinates_score>=224) from the gazetteer for every distinct Location 
-- in the gazetteer. For Locations where a single best georeference can not be 
-- determined, the original coordinate-related values are used.
-- Start with Locations that already have original georeferences.
CREATE OR REPLACE TABLE `localityservice.gazetteer.georefs`
AS
(SELECT
dwc_location_hash,
ROUND(10000000*SAFE_CAST(interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
'epsg:4326' AS bels_geodeticdatum,
ROUND(SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)) AS bels_coordinateuncertaintyinmeters,
v_georeferencedby AS bels_georeferencedby,
v_georeferenceddate AS bels_georeferenceddate,
v_georeferenceprotocol AS bels_georeferenceprotocol,
v_georeferencesources AS bels_georeferencesources,
v_georeferenceremarks AS bels_georeferenceremarks,
coordinates_score AS bels_coordinates_score,
georef_score AS bels_georeference_score,
source AS bels_georeference_source,
1 AS bels_best_of_n_georeferences,
'original georeference' as bels_match_type
FROM
`localityservice.gazetteer.locations_distinct_with_scores`
WHERE coordinates_score>=224
);
------------------------------------------
-- APPEND to table georefs from matchme_with_coords (n=711,277)
-- For any remaining Locations not already assigned a best georeference, add the 
-- best one based on matching with coordinates
INSERT INTO `localityservice.gazetteer.georefs`
(SELECT
a.dwc_location_hash,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
'epsg:4326' AS bels_geodeticdatum,
ROUND(unc_numeric) AS bels_coordinateuncertaintyinmeters,
b.v_georeferencedby AS bels_georeferencedby,
b.v_georeferenceddate AS bels_georeferenceddate,
b.v_georeferenceprotocol AS bels_georeferenceprotocol,
b.v_georeferencesources AS bels_georeferencesources,
b.v_georeferenceremarks AS bels_georeferenceremarks,
224+b.georef_score AS bels_coordinates_score,
b.georef_score AS bels_georeference_score,
b.source AS bels_georeference_source,
b.georef_count AS bels_best_of_n_georeferences,
'match using coords' AS bels_match_type
FROM
`localityservice.gazetteer.locations_distinct_with_scores` a,
`localityservice.gazetteer.11_matchme_with_coords_best_georef` b
WHERE
a.matchme_with_coords=b.matchme_with_coords AND
a.dwc_location_hash NOT IN
(SELECT dwc_location_hash
FROM
`localityservice.gazetteer.georefs`
)
);
------------------------------------------
-- APPEND to table georefs from matchme (n=2,400,584)
-- For any remaining Locations not already assigned a best georeference, add the 
-- best one based on matching with verbatim coordinates
INSERT INTO `localityservice.gazetteer.georefs`
(SELECT
a.dwc_location_hash,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
'epsg:4326' AS bels_geodeticdatum,
ROUND(unc_numeric) AS bels_coordinateuncertaintyinmeters,
b.v_georeferencedby AS bels_georeferencedby,
b.v_georeferenceddate AS bels_georeferenceddate,
b.v_georeferenceprotocol AS bels_georeferenceprotocol,
b.v_georeferencesources AS bels_georeferencesources,
b.v_georeferenceremarks AS bels_georeferenceremarks,
224+b.georef_score AS bels_coordinates_score,
b.georef_score AS bels_georeference_score,
b.source AS bels_georeference_source,
b.georef_count AS bels_best_of_n_georeferences,
'match using verbatim coords' AS bels_match_type
FROM
`localityservice.gazetteer.locations_distinct_with_scores` a,
`localityservice.gazetteer.11_matchme_verbatim_coords_best_georef` b
WHERE
a.matchme=b.matchme AND
a.dwc_location_hash NOT IN
(SELECT dwc_location_hash
FROM
`localityservice.gazetteer.georefs`
)
);
------------------------------------------
-- APPEND to table georefs from matchme_sans_coords (n=1,237,108)
-- For any remaining Locations not already assigned a best georeference, add the 
-- best one based on matching sans coordinates
INSERT INTO `localityservice.gazetteer.georefs`
(SELECT
a.dwc_location_hash,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
'epsg:4326' AS bels_geodeticdatum,
ROUND(unc_numeric) AS bels_coordinateuncertaintyinmeters,
b.v_georeferencedby AS bels_georeferencedby,
b.v_georeferenceddate AS bels_georeferenceddate,
b.v_georeferenceprotocol AS bels_georeferenceprotocol,
b.v_georeferencesources AS bels_georeferencesources,
b.v_georeferenceremarks AS bels_georeferenceremarks,
224+b.georef_score AS bels_coordinates_score,
b.georef_score AS bels_georeference_score,
b.source AS bels_georeference_source,
b.georef_count AS bels_best_of_n_georeferences,
'match sans coords' AS bels_match_type
FROM
`localityservice.gazetteer.locations_distinct_with_scores` a,
`localityservice.gazetteer.11_matchme_sans_coords_best_georef` b
WHERE
a.matchme_sans_coords=b.matchme_sans_coords AND
a.dwc_location_hash NOT IN
(SELECT dwc_location_hash
FROM
`localityservice.gazetteer.georefs`
)
);
------------------------------------------
-- APPEND to table georefs coords-only (n=70,093,355)
INSERT INTO `localityservice.gazetteer.georefs`
(
 SELECT
 dwc_location_hash,
 ROUND(10000000*SAFE_CAST(interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
 ROUND(10000000*SAFE_CAST(interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
 'epsg:4326' AS bels_geodeticdatum,
 ROUND(SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)) AS bels_coordinateuncertaintyinmeters,
 v_georeferencedby AS bels_georeferencedby,
 v_georeferenceddate AS bels_georeferenceddate,
 v_georeferenceprotocol AS bels_georeferenceprotocol,
 v_georeferencesources AS bels_georeferencesources,
 v_georeferenceremarks AS bels_georeferenceremarks,
 coordinates_score AS bels_coordinates_score,
 georef_score AS bels_georeference_score,
 source AS bels_georeference_source,
 1 AS bels_best_of_n_georeferences,
 'original coordinates only' AS bels_match_type
 FROM
 `localityservice.gazetteer.locations_distinct_with_scores`
 WHERE
 coordinates_score>=128 AND
 dwc_location_hash NOT IN
 (
   SELECT dwc_location_hash
   FROM
   `localityservice.gazetteer.georefs`
 )
);
------------------------------------------
-- APPEND to table georefs coords-only records from matchme_with_coords (n=77,734)
INSERT INTO `localityservice.gazetteer.georefs`
SELECT
dwc_location_hash,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
'epsg:4326' AS bels_geodeticdatum,
0 as bels_coordinateuncertaintyinmeters,
b.v_georeferencedby AS bels_georeferencedby,
b.v_georeferenceddate AS bels_georeferenceddate,
b.v_georeferenceprotocol AS bels_georeferenceprotocol,
b.v_georeferencesources AS bels_georeferencesources,
b.v_georeferenceremarks AS bels_georeferenceremarks,
coordinates_score AS bels_coordinates_score,
b.georef_score AS bels_georeference_score,
b.source AS bels_georeference_source,
1 AS bels_best_of_n_georeferences,
'match using coords - coords only' AS bels_match_type
FROM
`localityservice.gazetteer.14_matchme_with_coords_best_coords_only` a,
`localityservice.gazetteer.locations_distinct_with_scores` b
WHERE
a.matchme_with_coords=b.matchme_with_coords
AND dwc_location_hash NOT IN
(
SELECT dwc_location_hash
FROM
`localityservice.gazetteer.georefs`
);
------------------------------------------
-- APPEND to table georefs coords-only records from matchme (n=997,060)
INSERT INTO `localityservice.gazetteer.georefs`
SELECT
dwc_location_hash,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
'epsg:4326' AS bels_geodeticdatum,
0 as bels_coordinateuncertaintyinmeters,
b.v_georeferencedby AS bels_georeferencedby,
b.v_georeferenceddate AS bels_georeferenceddate,
b.v_georeferenceprotocol AS bels_georeferenceprotocol,
b.v_georeferencesources AS bels_georeferencesources,
b.v_georeferenceremarks AS bels_georeferenceremarks,
coordinates_score AS bels_coordinates_score,
b.georef_score AS bels_georeference_score,
b.source AS bels_georeference_source,
1 AS bels_best_of_n_georeferences,
'match using verbatim coords - coords only' AS bels_match_type
FROM
`localityservice.gazetteer.14_matchme_verbatim_coords_best_coords_only` a,
`localityservice.gazetteer.locations_distinct_with_scores` b
WHERE
a.matchme=b.matchme
AND dwc_location_hash NOT IN
(
SELECT dwc_location_hash
FROM
`localityservice.gazetteer.georefs`
);
------------------------------------------
-- APPEND to table georefs coords-only records from matchme_sans_coords (n=307,964)
INSERT INTO `localityservice.gazetteer.georefs`
SELECT
dwc_location_hash,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallatitude AS NUMERIC))/10000000 AS bels_decimallatitude,
ROUND(10000000*SAFE_CAST(b.interpreted_decimallongitude AS NUMERIC))/10000000 AS bels_decimallongitude,
'epsg:4326' AS bels_geodeticdatum,
0 as bels_coordinateuncertaintyinmeters,
b.v_georeferencedby AS bels_georeferencedby,
b.v_georeferenceddate AS bels_georeferenceddate,
b.v_georeferenceprotocol AS bels_georeferenceprotocol,
b.v_georeferencesources AS bels_georeferencesources,
b.v_georeferenceremarks AS bels_georeferenceremarks,
coordinates_score AS bels_coordinates_score,
b.georef_score AS bels_georeference_score,
b.source AS bels_georeference_source,
1 AS bels_best_of_n_georeferences,
'match sans coords - coords only' AS bels_match_type
FROM
`localityservice.gazetteer.14_matchme_sans_coords_best_coords_only` a,
`localityservice.gazetteer.locations_distinct_with_scores` b
WHERE
a.matchme_sans_coords=b.matchme_sans_coords
AND dwc_location_hash NOT IN
(
SELECT dwc_location_hash
FROM
`localityservice.gazetteer.georefs`
);
------------------------------------------
-- Check to see that no duplicate dwc_location_hash was added (n=0)
SELECT dwc_location_hash, count(*) as reps
FROM `localityservice.gazetteer.georefs`
GROUP BY dwc_location_hash
HAVING count(*)>1
ORDER BY reps DESC
LIMIT 10;
END;