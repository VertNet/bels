--------------------------------------------------------------------------------
-- BELS runtime structures, PostgreSQL DDL
--
-- Instantiates the tables that the BELS georeference-matching API reads, as
-- they exist in BigQuery project `localityservice`.
-- Derived from VertNet/bels @ 454b792 and confirmed with the project owner:
--   * the three runtime best-georef tables are the 11_* tables produced by
--     gazetteer/load_gazetteer.sql, renamed in BigQuery after creation
--   * epsg is an integer
--   * data/tests CSVs are not authoritative and were not used
--
-- Requires PostgreSQL 14+ (the snapshot integrity check uses bit_xor) and the
-- PostGIS extension (for column center).
--
-- Run with psql. The gazetteer tables are created in the schema named by the
-- psql variable gazetteer_schema, default gazetteer. The snapshot loader (plan
-- step O4) creates them in gazetteer_next, then swaps schemas in O7:
--   psql -d localityservice -v ON_ERROR_STOP=1 -f bels_runtime_ddl_postgresql.sql
--   psql -d localityservice -v ON_ERROR_STOP=1 -v gazetteer_schema=gazetteer_next \
--        -f bels_runtime_ddl_postgresql.sql
--
-- Scope: structures only. Population, query porting, and the UDFs are covered
-- by notes, not statements. See "Not included" at the end.
--
-- Type mapping applied, BigQuery -> PostgreSQL:
--   STRING    -> text
--   BYTES     -> bytea
--   INT64     -> bigint
--   FLOAT64   -> double precision
--   NUMERIC   -> numeric(38,9)          BigQuery NUMERIC is fixed at P=38, S=9
--   GEOGRAPHY -> geography(Point, 4326) every value comes from ST_GEOGPOINT(),
--                                        so Point is grounded in the lineage
--
-- Naming: bels/bels_query.py builds three-part names such as
-- localityservice.gazetteer.matchme_sans_coords_best_georef. PostgreSQL
-- accepts database.schema.table only for the current database, so these names
-- keep working if this script runs in a database named localityservice.
--------------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS vocabs;
\if :{?gazetteer_schema}
\else
  \set gazetteer_schema gazetteer
\endif
CREATE SCHEMA IF NOT EXISTS :"gazetteer_schema";

--------------------------------------------------------------------------------
-- vocabs.countrycode_lookup
-- Read in full by BELS_Client.populate() at App Engine startup to build the
-- u_country -> countrycode map used by /api/bestgeoref.
-- Schema source: data/countrycode_lookup.csv header. In the repository copy
-- (13,400 rows) u_country values are unique and all upper case, one row has an
-- empty u_country, and 2,560 rows have an empty countrycode. BigQuery loads
-- empty CSV fields as NULL, and so does COPY ... (FORMAT csv), so:
--   * u_country is UNIQUE rather than PRIMARY KEY, because one key is NULL
--   * countrycode is unconstrained text, because the file also holds values
--     that are not 2-letter codes (for example 'test', 'new',
--     'Argentina, Uruguay')
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vocabs.countrycode_lookup
(
  u_country   text,  -- upper-cased country name or code variant
  countrycode text,  -- interpreted country code, NULL when undeterminable
  CONSTRAINT countrycode_lookup_u_country_key UNIQUE (u_country)
);

--------------------------------------------------------------------------------
-- Best-georeference tables, one per match type
--
-- /api/bestgeoref (bels/resources.py) tries these in order and returns the
-- first hit, each by equality on one match-string column:
--   1. matchme_with_coords_best_georef    WHERE matchme_with_coords = $1
--      only when the input has decimal coordinates
--   2. matchme_verbatimcoords_best_georef WHERE matchme = $1
--      only when the input has verbatim coordinates
--   3. matchme_sans_coords_best_georef    WHERE matchme_sans_coords = $1
--
-- All three share one schema. Each 11_* table selects c.* from
-- gazetteer.locations_with_georefs_combined plus georef_count, so each carries
-- all three match-string columns even though only one is its lookup key.
-- Column order follows the BigQuery lineage:
--   gbif.locations_distinct_with_scores        (gazetteer/load_gbif.sql)
--   -> gazetteer.locations_distinct_with_scores (SELECT *)
--   -> gazetteer.locations_with_georefs_combined (* + unc_numeric,
--        bels_decimallatitude, bels_decimallongitude,
--        bels_coordinateuncertaintyinmeters, center)
--   -> gazetteer.11_*_best_georef               (c.* + georef_count)
--
-- Columns the API reads: interpreted_countrycode, the lookup key,
-- interpreted_decimallatitude, interpreted_decimallongitude, unc_numeric,
-- v_georeferencedby, v_georeferenceddate, v_georeferenceprotocol,
-- v_georeferencesources, v_georeferenceremarks, georef_score, source,
-- georef_count. The remaining columns are kept so the structures match
-- BigQuery.
--------------------------------------------------------------------------------

-- BigQuery source: localityservice.gazetteer.matchme_with_coords_best_georef
--   (renamed from 11_matchme_with_coords_best_georef)
-- Lookup key: matchme_with_coords
CREATE TABLE IF NOT EXISTS :"gazetteer_schema".matchme_with_coords_best_georef
(
  dwc_location_hash                     bytea,                      -- SHA256 Location identifier
  v_highergeographyid                   text,
  v_highergeography                     text,
  v_continent                           text,
  v_waterbody                           text,
  v_islandgroup                         text,
  v_island                              text,
  v_country                             text,
  v_countrycode                         text,
  v_stateprovince                       text,
  v_county                              text,
  v_municipality                        text,
  v_locality                            text,
  v_verbatimlocality                    text,
  v_minimumelevationinmeters            text,
  v_maximumelevationinmeters            text,
  v_verbatimelevation                   text,
  v_verticaldatum                       text,
  v_minimumdepthinmeters                text,
  v_maximumdepthinmeters                text,
  v_verbatimdepth                       text,
  v_minimumdistanceabovesurfaceinmeters text,
  v_maximumdistanceabovesurfaceinmeters text,
  v_locationaccordingto                 text,
  v_locationremarks                     text,
  v_decimallatitude                     text,
  v_decimallongitude                    text,
  v_geodeticdatum                       text,
  v_coordinateuncertaintyinmeters       text,
  v_coordinateprecision                 text,
  v_pointradiusspatialfit               text,
  v_verbatimcoordinates                 text,
  v_verbatimlatitude                    text,
  v_verbatimlongitude                   text,
  v_verbatimcoordinatesystem            text,
  v_verbatimsrs                         text,
  v_footprintwkt                        text,
  v_footprintsrs                        text,
  v_footprintspatialfit                 text,
  v_georeferencedby                     text,
  v_georeferenceddate                   text,
  v_georeferenceprotocol                text,
  v_georeferencesources                 text,
  v_georeferenceremarks                 text,
  interpreted_decimallatitude           double precision,           -- read by API
  interpreted_decimallongitude          double precision,           -- read by API
  interpreted_countrycode               text,                       -- read by API, as bels_countrycode
  occcount                              bigint,
  u_datumstr                            text,
  tokens                                text,
  matchme_with_coords                   text,                       -- match key, with coords
  matchme                               text,                       -- match key, verbatim coords
  matchme_sans_coords                   text,                       -- match key, sans coords
  epsg                                  bigint,                     -- integer EPSG code
  georef_score                          bigint,                     -- read by API
  coordinates_score                     bigint,
  source                                text,                       -- read by API: GBIF, VertNet, iDigBio
  unc_numeric                           numeric(38,9),              -- read by API
  bels_decimallatitude                  numeric(38,9),
  bels_decimallongitude                 numeric(38,9),
  bels_coordinateuncertaintyinmeters    numeric(38,9),
  center                                geography(Point, 4326),     -- ST_GEOGPOINT() in BigQuery
  georef_count                          bigint                      -- read by API, as bels_best_of_n_georeferences
);

-- BigQuery source: localityservice.gazetteer.matchme_verbatimcoords_best_georef
--   (renamed from 11_matchme_verbatim_coords_best_georef)
-- Lookup key: matchme
CREATE TABLE IF NOT EXISTS :"gazetteer_schema".matchme_verbatimcoords_best_georef
(
  dwc_location_hash                     bytea,                      -- SHA256 Location identifier
  v_highergeographyid                   text,
  v_highergeography                     text,
  v_continent                           text,
  v_waterbody                           text,
  v_islandgroup                         text,
  v_island                              text,
  v_country                             text,
  v_countrycode                         text,
  v_stateprovince                       text,
  v_county                              text,
  v_municipality                        text,
  v_locality                            text,
  v_verbatimlocality                    text,
  v_minimumelevationinmeters            text,
  v_maximumelevationinmeters            text,
  v_verbatimelevation                   text,
  v_verticaldatum                       text,
  v_minimumdepthinmeters                text,
  v_maximumdepthinmeters                text,
  v_verbatimdepth                       text,
  v_minimumdistanceabovesurfaceinmeters text,
  v_maximumdistanceabovesurfaceinmeters text,
  v_locationaccordingto                 text,
  v_locationremarks                     text,
  v_decimallatitude                     text,
  v_decimallongitude                    text,
  v_geodeticdatum                       text,
  v_coordinateuncertaintyinmeters       text,
  v_coordinateprecision                 text,
  v_pointradiusspatialfit               text,
  v_verbatimcoordinates                 text,
  v_verbatimlatitude                    text,
  v_verbatimlongitude                   text,
  v_verbatimcoordinatesystem            text,
  v_verbatimsrs                         text,
  v_footprintwkt                        text,
  v_footprintsrs                        text,
  v_footprintspatialfit                 text,
  v_georeferencedby                     text,
  v_georeferenceddate                   text,
  v_georeferenceprotocol                text,
  v_georeferencesources                 text,
  v_georeferenceremarks                 text,
  interpreted_decimallatitude           double precision,           -- read by API
  interpreted_decimallongitude          double precision,           -- read by API
  interpreted_countrycode               text,                       -- read by API, as bels_countrycode
  occcount                              bigint,
  u_datumstr                            text,
  tokens                                text,
  matchme_with_coords                   text,                       -- match key, with coords
  matchme                               text,                       -- match key, verbatim coords
  matchme_sans_coords                   text,                       -- match key, sans coords
  epsg                                  bigint,                     -- integer EPSG code
  georef_score                          bigint,                     -- read by API
  coordinates_score                     bigint,
  source                                text,                       -- read by API: GBIF, VertNet, iDigBio
  unc_numeric                           numeric(38,9),              -- read by API
  bels_decimallatitude                  numeric(38,9),
  bels_decimallongitude                 numeric(38,9),
  bels_coordinateuncertaintyinmeters    numeric(38,9),
  center                                geography(Point, 4326),     -- ST_GEOGPOINT() in BigQuery
  georef_count                          bigint                      -- read by API, as bels_best_of_n_georeferences
);

-- BigQuery source: localityservice.gazetteer.matchme_sans_coords_best_georef
--   (renamed from 11_matchme_sans_coords_best_georef)
-- Lookup key: matchme_sans_coords
CREATE TABLE IF NOT EXISTS :"gazetteer_schema".matchme_sans_coords_best_georef
(
  dwc_location_hash                     bytea,                      -- SHA256 Location identifier
  v_highergeographyid                   text,
  v_highergeography                     text,
  v_continent                           text,
  v_waterbody                           text,
  v_islandgroup                         text,
  v_island                              text,
  v_country                             text,
  v_countrycode                         text,
  v_stateprovince                       text,
  v_county                              text,
  v_municipality                        text,
  v_locality                            text,
  v_verbatimlocality                    text,
  v_minimumelevationinmeters            text,
  v_maximumelevationinmeters            text,
  v_verbatimelevation                   text,
  v_verticaldatum                       text,
  v_minimumdepthinmeters                text,
  v_maximumdepthinmeters                text,
  v_verbatimdepth                       text,
  v_minimumdistanceabovesurfaceinmeters text,
  v_maximumdistanceabovesurfaceinmeters text,
  v_locationaccordingto                 text,
  v_locationremarks                     text,
  v_decimallatitude                     text,
  v_decimallongitude                    text,
  v_geodeticdatum                       text,
  v_coordinateuncertaintyinmeters       text,
  v_coordinateprecision                 text,
  v_pointradiusspatialfit               text,
  v_verbatimcoordinates                 text,
  v_verbatimlatitude                    text,
  v_verbatimlongitude                   text,
  v_verbatimcoordinatesystem            text,
  v_verbatimsrs                         text,
  v_footprintwkt                        text,
  v_footprintsrs                        text,
  v_footprintspatialfit                 text,
  v_georeferencedby                     text,
  v_georeferenceddate                   text,
  v_georeferenceprotocol                text,
  v_georeferencesources                 text,
  v_georeferenceremarks                 text,
  interpreted_decimallatitude           double precision,           -- read by API
  interpreted_decimallongitude          double precision,           -- read by API
  interpreted_countrycode               text,                       -- read by API, as bels_countrycode
  occcount                              bigint,
  u_datumstr                            text,
  tokens                                text,
  matchme_with_coords                   text,                       -- match key, with coords
  matchme                               text,                       -- match key, verbatim coords
  matchme_sans_coords                   text,                       -- match key, sans coords
  epsg                                  bigint,                     -- integer EPSG code
  georef_score                          bigint,                     -- read by API
  coordinates_score                     bigint,
  source                                text,                       -- read by API: GBIF, VertNet, iDigBio
  unc_numeric                           numeric(38,9),              -- read by API
  bels_decimallatitude                  numeric(38,9),
  bels_decimallongitude                 numeric(38,9),
  bels_coordinateuncertaintyinmeters    numeric(38,9),
  center                                geography(Point, 4326),     -- ST_GEOGPOINT() in BigQuery
  georef_count                          bigint                      -- read by API, as bels_best_of_n_georeferences
);

--------------------------------------------------------------------------------
-- Lookup indexes
-- BigQuery has no indexes, so these are NOT part of the BigQuery structure.
-- They are included because every API lookup is a single equality predicate
-- on one text column, and without an index each request would scan a table of
-- tens of millions of rows (load_gazetteer.sql records n=81,252,804 for the
-- with-coords table). They do not change query results.
--
-- Hash, not B-tree: the match strings are unbounded concatenations of verbatim
-- Darwin Core fields, and a B-tree entry fails above roughly 2.7 kB, which would
-- abort the load on the first long locality. Hash indexes have no such limit
-- and support exactly the = operator the API uses.
--
-- Uniqueness: each 11_* table keeps ROW_NUMBER() ... rn = 1 per match string,
-- so the lookup key is unique by construction. A hash index cannot enforce
-- UNIQUE, so that property is documented here, not enforced. If you confirm no
-- key exceeds the B-tree limit, a UNIQUE constraint can replace the hash index.
--
-- Build these after loading data; building on an empty table and then loading
-- is much slower.
--------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS matchme_with_coords_best_georef_key_hidx
  ON :"gazetteer_schema".matchme_with_coords_best_georef USING hash (matchme_with_coords);

CREATE INDEX IF NOT EXISTS matchme_verbatimcoords_best_georef_key_hidx
  ON :"gazetteer_schema".matchme_verbatimcoords_best_georef USING hash (matchme);

CREATE INDEX IF NOT EXISTS matchme_sans_coords_best_georef_key_hidx
  ON :"gazetteer_schema".matchme_sans_coords_best_georef USING hash (matchme_sans_coords);

--------------------------------------------------------------------------------
-- Not included
--
-- 1. UDFs matchString, simplifyDiacritics, removeSymbols, saveNumbers, and
--    the datasets belsapi and results. These serve only the bulk CSV path
--    (/api/bels_csv -> Pub/Sub -> Cloud Function -> process_import_table),
--    which is built on BigQuery load and extract jobs and would need
--    rewriting for PostgreSQL. /api/bestgeoref builds match strings in Python
--    (id_utils.location_match_str, super_simplify) and needs none of them.
--    Porting gap if the bulk path is moved: process_import_table also calls
--    BigQuery's NORMALIZE_AND_CASEFOLD(..., NFKC), which has no exact
--    PostgreSQL equivalent; normalize(lower(x), NFKC) is close but lower() is
--    not full Unicode case folding.
--
-- 2. Loading. Snapshots arrive as Parquet files from BigQuery extract jobs
--    (plan O1 to O4). How extract jobs encode GEOGRAPHY, BYTES and NUMERIC in
--    Parquet is not stated in the BigQuery documentation consulted; it is
--    confirmed in plan step D3, and the loader converts accordingly. Do not
--    load via CSV: BigQuery writes BYTES to CSV as base64, which COPY would
--    store into bytea as the base64 characters WITHOUT raising an error.
--
-- 4. vocabs.countrycode_lookup is not covered by the gazetteer_schema
--    variable. Per the plan, it arrives as CSV each run and is replaced in
--    the same transaction as the gazetteer swap:
--      O4:  CREATE TABLE vocabs.countrycode_lookup_next
--             (LIKE vocabs.countrycode_lookup INCLUDING ALL);
--           \copy vocabs.countrycode_lookup_next FROM '<local csv>'
--             WITH (FORMAT csv, HEADER true)
--      O7:  BEGIN;
--             ALTER SCHEMA gazetteer RENAME TO gazetteer_prev;
--             ALTER SCHEMA gazetteer_next RENAME TO gazetteer;
--             DELETE FROM vocabs.countrycode_lookup;
--             INSERT INTO vocabs.countrycode_lookup
--               SELECT * FROM vocabs.countrycode_lookup_next;
--             DROP TABLE vocabs.countrycode_lookup_next;
--           COMMIT;
--
-- 3. Query porting. The SQL in bels/bels_query.py is BigQuery dialect and
--    will not run unchanged here. For example, SAFE_CAST(... AS INT64) has no
--    PostgreSQL form, and the match string is interpolated into the SQL text
--    rather than bound as a parameter.
--------------------------------------------------------------------------------
