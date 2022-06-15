-- Detect any values of v_geodeticDatum that have not already been added to the 
-- datumsepsg table so that mappings can be added for these before loading the 
-- gazetteer. The script below creates the list. This needs to be reconciled with 
-- the vocabulary managed in 
-- https://github.com/VertNet/DwCVocabs/blob/master/vocabs/datumepsg.csv. 
-- Append the new values to the vocabulary, add all the epsg codes that can be mapped, 
-- and replace the datumsepsg table with the updated data.

-- Replace table vocabs.newdatums
BEGIN
CREATE TEMP TABLE datums AS
SELECT DISTINCT UPPER(v_geodeticdatum) AS u_datumstr
FROM localityservice.gbif.occurrences;
CREATE OR REPLACE TABLE localityservice.vocabs.newdatums
AS
SELECT u_datumstr
FROM datums
WHERE u_datumstr NOT IN
(SELECT u_datumstr FROM localityservice.vocabs.datumsepsg);
 
INSERT localityservice.vocabs.newdatums (u_datumstr)
((SELECT DISTINCT upper(geodeticdatum) AS u_datumstr
FROM localityservice.idigbio.occurrences
WHERE
upper(geodeticdatum) NOT IN (SELECT u_datumstr FROM localityservice.vocabs.datumsepsg) AND
upper(geodeticdatum) NOT IN (SELECT u_datumstr FROM localityservice.vocabs.newdatums))
);
END;
 
-- Reconcile new u_datumstrs to epsg integer values (e.g., WGS84 -> 4326)
-- load csv results to newdatums_gbif_reconciled with fields u_datumstr and epsg, 
-- detecting schema and not skipping any header rows.
 
INSERT localityservice.vocabs.datumsepsg (u_datumstr, epsg)
(SELECT u_datumstr, epsg
FROM localityservice.vocabs.newdatums_gbif_reconciled
);

-- Replace 
-- https://github.com/VertNet/DwCVocabs/blob/master/vocabs/datumepsg.csv 
-- with export of localityservice.vocabs.datumsepsg.