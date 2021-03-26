-- GBIF Loading Script
-- Script to prepare Location data for the gazetteer
-- Before running this script, load the latest source data into tables in BigQuery
--  gbif.occurrences
--  idigbio.occurrences
--  vertnet.locations (static)
-- Then update the table vocabs.datums.epsg based on new values in the source tables
BEGIN
-- Make function saveNumbers()
CREATE TEMP FUNCTION saveNumbers (str STRING)
RETURNS STRING
LANGUAGE js AS
"""
{
// Replace any of , . /  - and +,  except in the middle of digits, with space
var changed = str.replace(/(?<!\\d)[.,\\-\\/\\+](?!\\d)/g,' ');
return changed;
}
""";
-- End function saveNumbers()
 
-- Make function removeSymbols()
CREATE TEMP FUNCTION removeSymbols (str STRING)
RETURNS STRING
LANGUAGE js AS
"""
{
// Removes most punctuation and symbols. Does not remove . , / - or +
var cleaned = str.replace(/[’'<>:‒–—―…!«»-‐?‘’“”;⁄␠·&@*•^¤¢$€£¥₩₪†‡°¡¿¬#№%‰‱¶′§~¨_|¦⁂☞∴‽※}{\\]\\[\\"\\)\\(]/g,'');
return cleaned;
}
""";
-- End function removeSymbols()
 
-- Make function simplifyDiacritics()
CREATE TEMP FUNCTION simplifyDiacritics (str STRING)
RETURNS STRING
LANGUAGE js AS """
{
// Normalizes unicode, lowercases, and changes diacritics to ASCII "equivalents"
var defaultDiacriticsRemovalMap = [ {'base':'A', 'letters':/[\u0041\u24B6\uFF21\u00C0\u00C1\u00C2\u1EA6\u1EA4\u1EAA\u1EA8\u00C3\u0100\u0102\u1EB0\u1EAE\u1EB4\u1EB2\u0226\u01E0\u00C4\u01DE\u1EA2\u00C5\u01FA\u01CD\u0200\u0202\u1EA0\u1EAC\u1EB6\u1E00\u0104\u023A\u2C6F]/g}, {'base':'AA','letters':/[\uA732]/g}, {'base':'AE','letters':/[\u00C6\u01FC\u01E2]/g}, {'base':'AO','letters':/[\uA734]/g}, {'base':'AU','letters':/[\uA736]/g}, {'base':'AV','letters':/[\uA738\uA73A]/g}, {'base':'AY','letters':/[\uA73C]/g}, {'base':'B', 'letters':/[\u0042\u24B7\uFF22\u1E02\u1E04\u1E06\u0243\u0182\u0181]/g}, {'base':'C', 'letters':/[\u0043\u24B8\uFF23\u0106\u0108\u010A\u010C\u00C7\u1E08\u0187\u023B\uA73E]/g}, {'base':'D', 'letters':/[\u0044\u24B9\uFF24\u1E0A\u010E\u1E0C\u1E10\u1E12\u1E0E\u0110\u018B\u018A\u0189\uA779]/g}, {'base':'DZ','letters':/[\u01F1\u01C4]/g}, {'base':'Dz','letters':/[\u01F2\u01C5]/g}, {'base':'E', 'letters':/[\u0045\u24BA\uFF25\u00C8\u00C9\u00CA\u1EC0\u1EBE\u1EC4\u1EC2\u1EBC\u0112\u1E14\u1E16\u0114\u0116\u00CB\u1EBA\u011A\u0204\u0206\u1EB8\u1EC6\u0228\u1E1C\u0118\u1E18\u1E1A\u0190\u018E]/g}, {'base':'F', 'letters':/[\u0046\u24BB\uFF26\u1E1E\u0191\uA77B]/g}, {'base':'G', 'letters':/[\u0047\u24BC\uFF27\u01F4\u011C\u1E20\u011E\u0120\u01E6\u0122\u01E4\u0193\uA7A0\uA77D\uA77E]/g}, {'base':'H', 'letters':/[\u0048\u24BD\uFF28\u0124\u1E22\u1E26\u021E\u1E24\u1E28\u1E2A\u0126\u2C67\u2C75\uA78D]/g}, {'base':'I', 'letters':/[\u0049\u24BE\uFF29\u00CC\u00CD\u00CE\u0128\u012A\u012C\u0130\u00CF\u1E2E\u1EC8\u01CF\u0208\u020A\u1ECA\u012E\u1E2C\u0197]/g}, {'base':'J', 'letters':/[\u004A\u24BF\uFF2A\u0134\u0248]/g}, {'base':'K', 'letters':/[\u004B\u24C0\uFF2B\u1E30\u01E8\u1E32\u0136\u1E34\u0198\u2C69\uA740\uA742\uA744\uA7A2]/g}, {'base':'L', 'letters':/[\u004C\u24C1\uFF2C\u013F\u0139\u013D\u1E36\u1E38\u013B\u1E3C\u1E3A\u0141\u023D\u2C62\u2C60\uA748\uA746\uA780]/g}, {'base':'LJ','letters':/[\u01C7]/g}, {'base':'Lj','letters':/[\u01C8]/g}, {'base':'M', 'letters':/[\u004D\u24C2\uFF2D\u1E3E\u1E40\u1E42\u2C6E\u019C]/g}, {'base':'N', 'letters':/[\u004E\u24C3\uFF2E\u01F8\u0143\u00D1\u1E44\u0147\u1E46\u0145\u1E4A\u1E48\u0220\u019D\uA790\uA7A4]/g}, {'base':'NJ','letters':/[\u01CA]/g}, {'base':'Nj','letters':/[\u01CB]/g}, {'base':'O', 'letters':/[\u004F\u24C4\uFF2F\u00D2\u00D3\u00D4\u1ED2\u1ED0\u1ED6\u1ED4\u00D5\u1E4C\u022C\u1E4E\u014C\u1E50\u1E52\u014E\u022E\u0230\u00D6\u022A\u1ECE\u0150\u01D1\u020C\u020E\u01A0\u1EDC\u1EDA\u1EE0\u1EDE\u1EE2\u1ECC\u1ED8\u01EA\u01EC\u00D8\u01FE\u0186\u019F\uA74A\uA74C]/g}, {'base':'OI','letters':/[\u01A2]/g}, {'base':'OO','letters':/[\uA74E]/g}, {'base':'OU','letters':/[\u0222]/g}, {'base':'P', 'letters':/[\u0050\u24C5\uFF30\u1E54\u1E56\u01A4\u2C63\uA750\uA752\uA754]/g}, {'base':'Q', 'letters':/[\u0051\u24C6\uFF31\uA756\uA758\u024A]/g}, {'base':'R', 'letters':/[\u0052\u24C7\uFF32\u0154\u1E58\u0158\u0210\u0212\u1E5A\u1E5C\u0156\u1E5E\u024C\u2C64\uA75A\uA7A6\uA782]/g}, {'base':'S', 'letters':/[\u0053\u24C8\uFF33\u1E9E\u015A\u1E64\u015C\u1E60\u0160\u1E66\u1E62\u1E68\u0218\u015E\u2C7E\uA7A8\uA784]/g}, {'base':'SS', 'letters':/[\u1E9E]/g}, {'base':'T', 'letters':/[\u0054\u24C9\uFF34\u1E6A\u0164\u1E6C\u021A\u0162\u1E70\u1E6E\u0166\u01AC\u01AE\u023E\uA786]/g}, {'base':'TZ','letters':/[\uA728]/g}, {'base':'U', 'letters':/[\u0055\u24CA\uFF35\u00D9\u00DA\u00DB\u0168\u1E78\u016A\u1E7A\u016C\u00DC\u01DB\u01D7\u01D5\u01D9\u1EE6\u016E\u0170\u01D3\u0214\u0216\u01AF\u1EEA\u1EE8\u1EEE\u1EEC\u1EF0\u1EE4\u1E72\u0172\u1E76\u1E74\u0244]/g}, {'base':'V', 'letters':/[\u0056\u24CB\uFF36\u1E7C\u1E7E\u01B2\uA75E\u0245]/g}, {'base':'VY','letters':/[\uA760]/g}, {'base':'W', 'letters':/[\u0057\u24CC\uFF37\u1E80\u1E82\u0174\u1E86\u1E84\u1E88\u2C72]/g}, {'base':'X', 'letters':/[\u0058\u24CD\uFF38\u1E8A\u1E8C]/g}, {'base':'Y', 'letters':/[\u0059\u24CE\uFF39\u1EF2\u00DD\u0176\u1EF8\u0232\u1E8E\u0178\u1EF6\u1EF4\u01B3\u024E\u1EFE]/g}, {'base':'Z', 'letters':/[\u005A\u24CF\uFF3A\u0179\u1E90\u017B\u017D\u1E92\u1E94\u01B5\u0224\u2C7F\u2C6B\uA762]/g}, {'base':'a', 'letters':/[\u0061\u24D0\uFF41\u1E9A\u00E0\u00E1\u00E2\u1EA7\u1EA5\u1EAB\u1EA9\u00E3\u0101\u0103\u1EB1\u1EAF\u1EB5\u1EB3\u0227\u01E1\u00E4\u01DF\u1EA3\u00E5\u01FB\u01CE\u0201\u0203\u1EA1\u1EAD\u1EB7\u1E01\u0105\u2C65\u0250]/g}, {'base':'aa','letters':/[\uA733]/g}, {'base':'ae','letters':/[\u00E6\u01FD\u01E3]/g}, {'base':'ao','letters':/[\uA735]/g}, {'base':'au','letters':/[\uA737]/g}, {'base':'av','letters':/[\uA739\uA73B]/g}, {'base':'ay','letters':/[\uA73D]/g}, {'base':'b', 'letters':/[\u0062\u24D1\uFF42\u1E03\u1E05\u1E07\u0180\u0183\u0253]/g}, {'base':'c', 'letters':/[\u0063\u24D2\uFF43\u0107\u0109\u010B\u010D\u00E7\u1E09\u0188\u023C\uA73F\u2184]/g}, {'base':'d', 'letters':/[\u0064\u24D3\uFF44\u1E0B\u010F\u1E0D\u1E11\u1E13\u1E0F\u0111\u018C\u0256\u0257\uA77A]/g}, {'base':'dz','letters':/[\u01F3\u01C6]/g}, {'base':'e', 'letters':/[\u0065\u24D4\uFF45\u00E8\u00E9\u00EA\u1EC1\u1EBF\u1EC5\u1EC3\u1EBD\u0113\u1E15\u1E17\u0115\u0117\u00EB\u1EBB\u011B\u0205\u0207\u1EB9\u1EC7\u0229\u1E1D\u0119\u1E19\u1E1B\u0247\u025B\u01DD]/g}, {'base':'f', 'letters':/[\u0066\u24D5\uFF46\u1E1F\u0192\uA77C]/g}, {'base':'g', 'letters':/[\u0067\u24D6\uFF47\u01F5\u011D\u1E21\u011F\u0121\u01E7\u0123\u01E5\u0260\uA7A1\u1D79\uA77F]/g}, {'base':'h', 'letters':/[\u0068\u24D7\uFF48\u0125\u1E23\u1E27\u021F\u1E25\u1E29\u1E2B\u1E96\u0127\u2C68\u2C76\u0265]/g}, {'base':'hv','letters':/[\u0195]/g}, {'base':'i', 'letters':/[\u0069\u24D8\uFF49\u00EC\u00ED\u00EE\u0129\u012B\u012D\u00EF\u1E2F\u1EC9\u01D0\u0209\u020B\u1ECB\u012F\u1E2D\u0268\u0131]/g}, {'base':'j', 'letters':/[\u006A\u24D9\uFF4A\u0135\u01F0\u0249]/g}, {'base':'k', 'letters':/[\u006B\u24DA\uFF4B\u1E31\u01E9\u1E33\u0137\u1E35\u0199\u2C6A\uA741\uA743\uA745\uA7A3]/g}, {'base':'l', 'letters':/[\u006C\u24DB\uFF4C\u0140\u013A\u013E\u1E37\u1E39\u013C\u1E3D\u1E3B\u017F\u0142\u019A\u026B\u2C61\uA749\uA781\uA747]/g}, {'base':'lj','letters':/[\u01C9]/g}, {'base':'m', 'letters':/[\u006D\u24DC\uFF4D\u1E3F\u1E41\u1E43\u0271\u026F]/g}, {'base':'n', 'letters':/[\u006E\u24DD\uFF4E\u01F9\u0144\u00F1\u1E45\u0148\u1E47\u0146\u1E4B\u1E49\u019E\u0272\u0149\uA791\uA7A5]/g}, {'base':'nj','letters':/[\u01CC]/g}, {'base':'o', 'letters':/[\u006F\u24DE\uFF4F\u00F2\u00F3\u00F4\u1ED3\u1ED1\u1ED7\u1ED5\u00F5\u1E4D\u022D\u1E4F\u014D\u1E51\u1E53\u014F\u022F\u0231\u00F6\u022B\u1ECF\u0151\u01D2\u020D\u020F\u01A1\u1EDD\u1EDB\u1EE1\u1EDF\u1EE3\u1ECD\u1ED9\u01EB\u01ED\u00F8\u01FF\u0254\uA74B\uA74D\u0275]/g}, {'base':'oi','letters':/[\u01A3]/g}, {'base':'ou','letters':/[\u0223]/g}, {'base':'oo','letters':/[\uA74F]/g}, {'base':'p','letters':/[\u0070\u24DF\uFF50\u1E55\u1E57\u01A5\u1D7D\uA751\uA753\uA755]/g}, {'base':'q','letters':/[\u0071\u24E0\uFF51\u024B\uA757\uA759]/g}, {'base':'r','letters':/[\u0072\u24E1\uFF52\u0155\u1E59\u0159\u0211\u0213\u1E5B\u1E5D\u0157\u1E5F\u024D\u027D\uA75B\uA7A7\uA783]/g}, {'base':'s','letters':/[\u0073\u24E2\uFF53\u015B\u1E65\u015D\u1E61\u0161\u1E67\u1E63\u1E69\u0219\u015F\u023F\uA7A9\uA785\u1E9B]/g}, {'base':'ss','letters':/[\u00DF]/g}, {'base':'t','letters':/[\u0074\u24E3\uFF54\u1E6B\u1E97\u0165\u1E6D\u021B\u0163\u1E71\u1E6F\u0167\u01AD\u0288\u2C66\uA787]/g}, {'base':'tz','letters':/[\uA729]/g}, {'base':'u','letters':/[\u0075\u24E4\uFF55\u00F9\u00FA\u00FB\u0169\u1E79\u016B\u1E7B\u016D\u00FC\u01DC\u01D8\u01D6\u01DA\u1EE7\u016F\u0171\u01D4\u0215\u0217\u01B0\u1EEB\u1EE9\u1EEF\u1EED\u1EF1\u1EE5\u1E73\u0173\u1E77\u1E75\u0289]/g}, {'base':'v','letters':/[\u0076\u24E5\uFF56\u1E7D\u1E7F\u028B\uA75F\u028C]/g}, {'base':'vy','letters':/[\uA761]/g}, {'base':'w','letters':/[\u0077\u24E6\uFF57\u1E81\u1E83\u0175\u1E87\u1E85\u1E98\u1E89\u2C73]/g}, {'base':'x','letters':/[\u0078\u24E7\uFF58\u1E8B\u1E8D]/g}, {'base':'y','letters':/[\u0079\u24E8\uFF59\u1EF3\u00FD\u0177\u1EF9\u0233\u1E8F\u00FF\u1EF7\u1E99\u1EF5\u01B4\u024F\u1EFF]/g}, {'base':'z','letters':/[\u007A\u24E9\uFF5A\u017A\u1E91\u017C\u017E\u1E93\u1E95\u01B6\u0225\u0240\u2C6C\uA763]/g} ];
for(var i=0; i<defaultDiacriticsRemovalMap.length; i++) {
str = str.replace(defaultDiacriticsRemovalMap[i].letters, defaultDiacriticsRemovalMap[i].base);
}
return str;
}""";
-- End function simplifyDiacritics()
 
-- Process GBIF
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
'dwc:georeferenceverificationstatus', IFNULL(v_georeferenceverificationstatus,''),
'dwc:georeferenceremarks', IFNULL(v_georeferenceremarks,''))) AS dwc_location_hash,
CONCAT(
IFNULL(v_continent,''),' ',
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_municipality,''),' ',
IFNULL(v_locality,''),' ',
IFNULL(v_verbatimlocality,''),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
IFNULL(v_minimumdepthinmeters,''),' ',
IFNULL(v_maximumdepthinmeters,''),' ',
IFNULL(v_verbatimdepth,''),' ',
IFNULL(v_verbatimcoordinates,''),' ',
IFNULL(v_verbatimlatitude,''),' ',
IFNULL(v_verbatimlongitude,''),' ',
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallatitude AS NUMERIC))/10000000 AS STRING),''),' ',
IFNULL(SAFE_CAST(round(10000000*safe_cast(v_decimallongitude AS NUMERIC))/10000000 AS STRING),'')) AS for_match_with_coords,
CONCAT( IFNULL(v_continent,''),' ',
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_municipality,''),' ',
IFNULL(v_locality,''),' ',
IFNULL(v_verbatimlocality,''),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
IFNULL(v_minimumdepthinmeters,''),' ',
IFNULL(v_maximumdepthinmeters,''),' ',
IFNULL(v_verbatimdepth,''),' ',
IFNULL(v_verbatimcoordinates,''),' ',
IFNULL(v_verbatimlatitude,''),' ',
IFNULL(v_verbatimlongitude,''))
AS for_match,
CONCAT( IFNULL(v_continent,''),' ',
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_municipality,''),' ',
IFNULL(v_locality,''),' ',
IFNULL(v_verbatimlocality,''),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''),' ',
IFNULL(v_minimumdepthinmeters,''),' ',
IFNULL(v_maximumdepthinmeters,''),' ',
IFNULL(v_verbatimdepth,''))
AS for_match_sans_coords,
CONCAT( IFNULL(v_continent,''),' ',
IFNULL(v_highergeography,''),' ',
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(v_country,''),' ',
IFNULL(v_countrycode,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_municipality,''),' ',
IFNULL(v_locality,''),' ',
IFNULL(v_verbatimlocality,''),' ',
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
v_georeferenceverificationstatus,
v_georeferenceremarks,
decimallatitude AS interpreted_decimallatitude,
decimallongitude AS interpreted_decimallongitude,
countrycode AS interpreted_countrycode,
count(*) AS occcount
FROM localityservice.gbif.occurrences
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
v_georeferenceverificationstatus,
v_georeferenceremarks,
interpreted_countrycode,
interpreted_decimallatitude,
interpreted_decimallongitude;
-- End table temp_locations_gbif_distinct
 
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
issue,
SHA256(CONCAT(
"dwc:highergeographyid",
IFNULL(v_highergeographyid,""),
"dwc:highergeography",
IFNULL(v_highergeography,""),
"dwc:continent",
IFNULL(v_continent,""),
"dwc:waterbody",
IFNULL(v_waterbody,""),
"dwc:islandgroup",
IFNULL(v_islandgroup,""),
"dwc:island",
IFNULL(v_island,""),
"dwc:country",
IFNULL(v_country,""),
"dwc:countrycode",
IFNULL(v_countrycode,""),
"dwc:stateprovince",
IFNULL(v_stateprovince,""),
"dwc:county",
IFNULL(v_county,""),
"dwc:municipality",
IFNULL(v_municipality,""),
"dwc:locality",
IFNULL(v_locality,""),
"dwc:verbatimlocality",
IFNULL(v_verbatimlocality,""),
"dwc:minimumelevationinmeters",
IFNULL(v_minimumelevationinmeters,""),
"dwc:maximumelevationinmeters",
IFNULL(v_maximumelevationinmeters,""),
"dwc:verbatimelevation",
IFNULL(v_verbatimelevation,""),
"dwc:minimumdepthinmeters",
IFNULL(v_minimumdepthinmeters,""),
"dwc:maximumdepthinmeters",
IFNULL(v_maximumdepthinmeters,""),
"dwc:verbatimdepth",
IFNULL(v_verbatimdepth,""),
"dwc:minimumdistanceabovesurfaceinmeters",
IFNULL(v_minimumdistanceabovesurfaceinmeters,""),
"dwc:maximumdistanceabovesurfaceinmeters",
IFNULL(v_maximumdistanceabovesurfaceinmeters,""),
"dwc:locationaccordingto",
IFNULL(v_locationaccordingto,""),
"dwc:locationremarks",
IFNULL(v_locationremarks,""),
"dwc:decimallatitude",
IFNULL(v_decimallatitude,""),
"dwc:decimallongitude",
IFNULL(v_decimallongitude,""),
"dwc:geodeticdatum",
IFNULL(v_geodeticdatum,""),
"dwc:coordinateuncertaintyinmeters",
IFNULL(v_coordinateuncertaintyinmeters,""),
"dwc:coordinateprecision",
IFNULL(v_coordinateprecision,""),
"dwc:pointradiusspatialfit",
IFNULL(v_pointradiusspatialfit,""),
"dwc:verbatimcoordinates",
IFNULL(v_verbatimcoordinates,""),
"dwc:verbatimlatitude",
IFNULL(v_verbatimlatitude,""),
"dwc:verbatimlongitude",
IFNULL(v_verbatimlongitude,""),
"dwc:verbatimcoordinatesystem",
IFNULL(v_verbatimcoordinatesystem,""),
"dwc:verbatimsrs",
IFNULL(v_verbatimsrs,""),
"dwc:footprintwkt",
IFNULL(v_footprintwkt,""),
"dwc:footprintsrs",
IFNULL(v_footprintsrs,""),
"dwc:footprintspatialfit",
IFNULL(v_footprintspatialfit,""),
"dwc:georeferencedby",
IFNULL(v_georeferencedby,""),
"dwc:georeferenceddate",
IFNULL(v_georeferenceddate,""),
"dwc:georeferenceprotocol",
IFNULL(v_georeferenceprotocol,""),
"dwc:georeferencesources",
IFNULL(v_georeferencesources,""),
"dwc:georeferenceverificationstatus",
IFNULL(v_georeferenceverificationstatus,""),
"dwc:georeferenceremarks",
IFNULL(v_georeferenceremarks,""))) as dwc_location_hash
FROM `localityservice.gbif.occurrences`;
-- End table occurrence_location_lookup
 
-- Make table locations_gbif_distinct
CREATE OR REPLACE TABLE localityservice.gbif.locations_gbif_distinct
AS
SELECT dwc_location_hash, v_highergeographyid, v_highergeography, v_continent, v_waterbody, v_islandgroup, v_island, v_country, v_countrycode, v_stateprovince, v_county, v_municipality, v_locality, v_verbatimlocality, v_minimumelevationinmeters, v_maximumelevationinmeters, v_verbatimelevation, v_minimumdepthinmeters, v_maximumdepthinmeters, v_verbatimdepth, v_minimumdistanceabovesurfaceinmeters, v_maximumdistanceabovesurfaceinmeters, v_locationaccordingto, v_locationremarks, v_decimallatitude, v_decimallongitude, v_geodeticdatum, v_coordinateuncertaintyinmeters, v_coordinateprecision, v_pointradiusspatialfit, v_verbatimcoordinates, v_verbatimlatitude, v_verbatimlongitude, v_verbatimcoordinatesystem, v_verbatimsrs, v_footprintwkt, v_footprintsrs, v_footprintspatialfit, v_georeferencedby, v_georeferenceddate, v_georeferenceprotocol, v_georeferencesources, v_georeferenceverificationstatus, v_georeferenceremarks, interpreted_decimallatitude, interpreted_decimallongitude, interpreted_countrycode, occcount, UPPER(v_geodeticdatum) AS u_datumstr,
REGEXP_REPLACE(REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_tokens)),NFKC)),r"[\s]+",' '), r"^\s+|\s+$", '') AS tokens,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match_with_coords)),NFKC)),r"[\s]+",'') AS matchme_with_coords,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match)),NFKC)),r"[\s]+",'') AS matchme,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match_sans_coords)),NFKC)),r"[\s]+",'') AS matchme_sans_coords
FROM temp_locations_gbif_distinct;
-- End table locations_gbif_distinct
 
-- Make table locations_distinct_with_epsg
CREATE OR REPLACE TABLE localityservice.gbif.locations_distinct_with_epsg
AS
SELECT a.*, epsg
FROM `localityservice.gbif.locations_gbif_distinct` a
LEFT JOIN `localityservice.vocabs.datumsepsg` b
ON a.u_datumstr=b.u_datumstr;
-- End table locations_distinct_with_epsg
 
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
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)<20037509,0,32) +
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

-- iDigBio Loading Script
-- Script to prepare Location data for the gazetteer
-- Before running this script, load the latest source data into tables in BigQuery
--  gbif.occurrences
--  idigbio.occurrences
--  vertnet.locations (static)
-- Then update the table vocabs.datums.epsg based on new values in the source tables
-- Process iDigBio
BEGIN
CREATE TEMP FUNCTION saveNumbers (str STRING)
RETURNS STRING
LANGUAGE js AS
"""
{
// Replace any of , . /  - and +,  except in the middle of digits, with space
var changed = str.replace(/(?<!\\d)[.,\\-\\/\\+](?!\\d)/g,' ');
return changed;
}
""";
-- End function saveNumbers()
 
-- Make function removeSymbols()
CREATE TEMP FUNCTION removeSymbols (str STRING)
RETURNS STRING
LANGUAGE js AS
"""
{
// Removes most punctuation and symbols. Does not remove . , / - or +
var cleaned = str.replace(/[’'<>:‒–—―…!«»-‐?‘’“”;⁄␠·&@*•^¤¢$€£¥₩₪†‡°¡¿¬#№%‰‱¶′§~¨_|¦⁂☞∴‽※}{\\]\\[\\"\\)\\(]/g,'');
return cleaned;
}
""";
-- End function removeSymbols()
 
-- Make function simplifyDiacritics()
CREATE TEMP FUNCTION simplifyDiacritics (str STRING)
RETURNS STRING
LANGUAGE js AS """
{
// Normalizes unicode, lowercases, and changes diacritics to ASCII "equivalents"
var defaultDiacriticsRemovalMap = [ {'base':'A', 'letters':/[\u0041\u24B6\uFF21\u00C0\u00C1\u00C2\u1EA6\u1EA4\u1EAA\u1EA8\u00C3\u0100\u0102\u1EB0\u1EAE\u1EB4\u1EB2\u0226\u01E0\u00C4\u01DE\u1EA2\u00C5\u01FA\u01CD\u0200\u0202\u1EA0\u1EAC\u1EB6\u1E00\u0104\u023A\u2C6F]/g}, {'base':'AA','letters':/[\uA732]/g}, {'base':'AE','letters':/[\u00C6\u01FC\u01E2]/g}, {'base':'AO','letters':/[\uA734]/g}, {'base':'AU','letters':/[\uA736]/g}, {'base':'AV','letters':/[\uA738\uA73A]/g}, {'base':'AY','letters':/[\uA73C]/g}, {'base':'B', 'letters':/[\u0042\u24B7\uFF22\u1E02\u1E04\u1E06\u0243\u0182\u0181]/g}, {'base':'C', 'letters':/[\u0043\u24B8\uFF23\u0106\u0108\u010A\u010C\u00C7\u1E08\u0187\u023B\uA73E]/g}, {'base':'D', 'letters':/[\u0044\u24B9\uFF24\u1E0A\u010E\u1E0C\u1E10\u1E12\u1E0E\u0110\u018B\u018A\u0189\uA779]/g}, {'base':'DZ','letters':/[\u01F1\u01C4]/g}, {'base':'Dz','letters':/[\u01F2\u01C5]/g}, {'base':'E', 'letters':/[\u0045\u24BA\uFF25\u00C8\u00C9\u00CA\u1EC0\u1EBE\u1EC4\u1EC2\u1EBC\u0112\u1E14\u1E16\u0114\u0116\u00CB\u1EBA\u011A\u0204\u0206\u1EB8\u1EC6\u0228\u1E1C\u0118\u1E18\u1E1A\u0190\u018E]/g}, {'base':'F', 'letters':/[\u0046\u24BB\uFF26\u1E1E\u0191\uA77B]/g}, {'base':'G', 'letters':/[\u0047\u24BC\uFF27\u01F4\u011C\u1E20\u011E\u0120\u01E6\u0122\u01E4\u0193\uA7A0\uA77D\uA77E]/g}, {'base':'H', 'letters':/[\u0048\u24BD\uFF28\u0124\u1E22\u1E26\u021E\u1E24\u1E28\u1E2A\u0126\u2C67\u2C75\uA78D]/g}, {'base':'I', 'letters':/[\u0049\u24BE\uFF29\u00CC\u00CD\u00CE\u0128\u012A\u012C\u0130\u00CF\u1E2E\u1EC8\u01CF\u0208\u020A\u1ECA\u012E\u1E2C\u0197]/g}, {'base':'J', 'letters':/[\u004A\u24BF\uFF2A\u0134\u0248]/g}, {'base':'K', 'letters':/[\u004B\u24C0\uFF2B\u1E30\u01E8\u1E32\u0136\u1E34\u0198\u2C69\uA740\uA742\uA744\uA7A2]/g}, {'base':'L', 'letters':/[\u004C\u24C1\uFF2C\u013F\u0139\u013D\u1E36\u1E38\u013B\u1E3C\u1E3A\u0141\u023D\u2C62\u2C60\uA748\uA746\uA780]/g}, {'base':'LJ','letters':/[\u01C7]/g}, {'base':'Lj','letters':/[\u01C8]/g}, {'base':'M', 'letters':/[\u004D\u24C2\uFF2D\u1E3E\u1E40\u1E42\u2C6E\u019C]/g}, {'base':'N', 'letters':/[\u004E\u24C3\uFF2E\u01F8\u0143\u00D1\u1E44\u0147\u1E46\u0145\u1E4A\u1E48\u0220\u019D\uA790\uA7A4]/g}, {'base':'NJ','letters':/[\u01CA]/g}, {'base':'Nj','letters':/[\u01CB]/g}, {'base':'O', 'letters':/[\u004F\u24C4\uFF2F\u00D2\u00D3\u00D4\u1ED2\u1ED0\u1ED6\u1ED4\u00D5\u1E4C\u022C\u1E4E\u014C\u1E50\u1E52\u014E\u022E\u0230\u00D6\u022A\u1ECE\u0150\u01D1\u020C\u020E\u01A0\u1EDC\u1EDA\u1EE0\u1EDE\u1EE2\u1ECC\u1ED8\u01EA\u01EC\u00D8\u01FE\u0186\u019F\uA74A\uA74C]/g}, {'base':'OI','letters':/[\u01A2]/g}, {'base':'OO','letters':/[\uA74E]/g}, {'base':'OU','letters':/[\u0222]/g}, {'base':'P', 'letters':/[\u0050\u24C5\uFF30\u1E54\u1E56\u01A4\u2C63\uA750\uA752\uA754]/g}, {'base':'Q', 'letters':/[\u0051\u24C6\uFF31\uA756\uA758\u024A]/g}, {'base':'R', 'letters':/[\u0052\u24C7\uFF32\u0154\u1E58\u0158\u0210\u0212\u1E5A\u1E5C\u0156\u1E5E\u024C\u2C64\uA75A\uA7A6\uA782]/g}, {'base':'S', 'letters':/[\u0053\u24C8\uFF33\u1E9E\u015A\u1E64\u015C\u1E60\u0160\u1E66\u1E62\u1E68\u0218\u015E\u2C7E\uA7A8\uA784]/g}, {'base':'SS', 'letters':/[\u1E9E]/g}, {'base':'T', 'letters':/[\u0054\u24C9\uFF34\u1E6A\u0164\u1E6C\u021A\u0162\u1E70\u1E6E\u0166\u01AC\u01AE\u023E\uA786]/g}, {'base':'TZ','letters':/[\uA728]/g}, {'base':'U', 'letters':/[\u0055\u24CA\uFF35\u00D9\u00DA\u00DB\u0168\u1E78\u016A\u1E7A\u016C\u00DC\u01DB\u01D7\u01D5\u01D9\u1EE6\u016E\u0170\u01D3\u0214\u0216\u01AF\u1EEA\u1EE8\u1EEE\u1EEC\u1EF0\u1EE4\u1E72\u0172\u1E76\u1E74\u0244]/g}, {'base':'V', 'letters':/[\u0056\u24CB\uFF36\u1E7C\u1E7E\u01B2\uA75E\u0245]/g}, {'base':'VY','letters':/[\uA760]/g}, {'base':'W', 'letters':/[\u0057\u24CC\uFF37\u1E80\u1E82\u0174\u1E86\u1E84\u1E88\u2C72]/g}, {'base':'X', 'letters':/[\u0058\u24CD\uFF38\u1E8A\u1E8C]/g}, {'base':'Y', 'letters':/[\u0059\u24CE\uFF39\u1EF2\u00DD\u0176\u1EF8\u0232\u1E8E\u0178\u1EF6\u1EF4\u01B3\u024E\u1EFE]/g}, {'base':'Z', 'letters':/[\u005A\u24CF\uFF3A\u0179\u1E90\u017B\u017D\u1E92\u1E94\u01B5\u0224\u2C7F\u2C6B\uA762]/g}, {'base':'a', 'letters':/[\u0061\u24D0\uFF41\u1E9A\u00E0\u00E1\u00E2\u1EA7\u1EA5\u1EAB\u1EA9\u00E3\u0101\u0103\u1EB1\u1EAF\u1EB5\u1EB3\u0227\u01E1\u00E4\u01DF\u1EA3\u00E5\u01FB\u01CE\u0201\u0203\u1EA1\u1EAD\u1EB7\u1E01\u0105\u2C65\u0250]/g}, {'base':'aa','letters':/[\uA733]/g}, {'base':'ae','letters':/[\u00E6\u01FD\u01E3]/g}, {'base':'ao','letters':/[\uA735]/g}, {'base':'au','letters':/[\uA737]/g}, {'base':'av','letters':/[\uA739\uA73B]/g}, {'base':'ay','letters':/[\uA73D]/g}, {'base':'b', 'letters':/[\u0062\u24D1\uFF42\u1E03\u1E05\u1E07\u0180\u0183\u0253]/g}, {'base':'c', 'letters':/[\u0063\u24D2\uFF43\u0107\u0109\u010B\u010D\u00E7\u1E09\u0188\u023C\uA73F\u2184]/g}, {'base':'d', 'letters':/[\u0064\u24D3\uFF44\u1E0B\u010F\u1E0D\u1E11\u1E13\u1E0F\u0111\u018C\u0256\u0257\uA77A]/g}, {'base':'dz','letters':/[\u01F3\u01C6]/g}, {'base':'e', 'letters':/[\u0065\u24D4\uFF45\u00E8\u00E9\u00EA\u1EC1\u1EBF\u1EC5\u1EC3\u1EBD\u0113\u1E15\u1E17\u0115\u0117\u00EB\u1EBB\u011B\u0205\u0207\u1EB9\u1EC7\u0229\u1E1D\u0119\u1E19\u1E1B\u0247\u025B\u01DD]/g}, {'base':'f', 'letters':/[\u0066\u24D5\uFF46\u1E1F\u0192\uA77C]/g}, {'base':'g', 'letters':/[\u0067\u24D6\uFF47\u01F5\u011D\u1E21\u011F\u0121\u01E7\u0123\u01E5\u0260\uA7A1\u1D79\uA77F]/g}, {'base':'h', 'letters':/[\u0068\u24D7\uFF48\u0125\u1E23\u1E27\u021F\u1E25\u1E29\u1E2B\u1E96\u0127\u2C68\u2C76\u0265]/g}, {'base':'hv','letters':/[\u0195]/g}, {'base':'i', 'letters':/[\u0069\u24D8\uFF49\u00EC\u00ED\u00EE\u0129\u012B\u012D\u00EF\u1E2F\u1EC9\u01D0\u0209\u020B\u1ECB\u012F\u1E2D\u0268\u0131]/g}, {'base':'j', 'letters':/[\u006A\u24D9\uFF4A\u0135\u01F0\u0249]/g}, {'base':'k', 'letters':/[\u006B\u24DA\uFF4B\u1E31\u01E9\u1E33\u0137\u1E35\u0199\u2C6A\uA741\uA743\uA745\uA7A3]/g}, {'base':'l', 'letters':/[\u006C\u24DB\uFF4C\u0140\u013A\u013E\u1E37\u1E39\u013C\u1E3D\u1E3B\u017F\u0142\u019A\u026B\u2C61\uA749\uA781\uA747]/g}, {'base':'lj','letters':/[\u01C9]/g}, {'base':'m', 'letters':/[\u006D\u24DC\uFF4D\u1E3F\u1E41\u1E43\u0271\u026F]/g}, {'base':'n', 'letters':/[\u006E\u24DD\uFF4E\u01F9\u0144\u00F1\u1E45\u0148\u1E47\u0146\u1E4B\u1E49\u019E\u0272\u0149\uA791\uA7A5]/g}, {'base':'nj','letters':/[\u01CC]/g}, {'base':'o', 'letters':/[\u006F\u24DE\uFF4F\u00F2\u00F3\u00F4\u1ED3\u1ED1\u1ED7\u1ED5\u00F5\u1E4D\u022D\u1E4F\u014D\u1E51\u1E53\u014F\u022F\u0231\u00F6\u022B\u1ECF\u0151\u01D2\u020D\u020F\u01A1\u1EDD\u1EDB\u1EE1\u1EDF\u1EE3\u1ECD\u1ED9\u01EB\u01ED\u00F8\u01FF\u0254\uA74B\uA74D\u0275]/g}, {'base':'oi','letters':/[\u01A3]/g}, {'base':'ou','letters':/[\u0223]/g}, {'base':'oo','letters':/[\uA74F]/g}, {'base':'p','letters':/[\u0070\u24DF\uFF50\u1E55\u1E57\u01A5\u1D7D\uA751\uA753\uA755]/g}, {'base':'q','letters':/[\u0071\u24E0\uFF51\u024B\uA757\uA759]/g}, {'base':'r','letters':/[\u0072\u24E1\uFF52\u0155\u1E59\u0159\u0211\u0213\u1E5B\u1E5D\u0157\u1E5F\u024D\u027D\uA75B\uA7A7\uA783]/g}, {'base':'s','letters':/[\u0073\u24E2\uFF53\u015B\u1E65\u015D\u1E61\u0161\u1E67\u1E63\u1E69\u0219\u015F\u023F\uA7A9\uA785\u1E9B]/g}, {'base':'ss','letters':/[\u00DF]/g}, {'base':'t','letters':/[\u0074\u24E3\uFF54\u1E6B\u1E97\u0165\u1E6D\u021B\u0163\u1E71\u1E6F\u0167\u01AD\u0288\u2C66\uA787]/g}, {'base':'tz','letters':/[\uA729]/g}, {'base':'u','letters':/[\u0075\u24E4\uFF55\u00F9\u00FA\u00FB\u0169\u1E79\u016B\u1E7B\u016D\u00FC\u01DC\u01D8\u01D6\u01DA\u1EE7\u016F\u0171\u01D4\u0215\u0217\u01B0\u1EEB\u1EE9\u1EEF\u1EED\u1EF1\u1EE5\u1E73\u0173\u1E77\u1E75\u0289]/g}, {'base':'v','letters':/[\u0076\u24E5\uFF56\u1E7D\u1E7F\u028B\uA75F\u028C]/g}, {'base':'vy','letters':/[\uA761]/g}, {'base':'w','letters':/[\u0077\u24E6\uFF57\u1E81\u1E83\u0175\u1E87\u1E85\u1E98\u1E89\u2C73]/g}, {'base':'x','letters':/[\u0078\u24E7\uFF58\u1E8B\u1E8D]/g}, {'base':'y','letters':/[\u0079\u24E8\uFF59\u1EF3\u00FD\u0177\u1EF9\u0233\u1E8F\u00FF\u1EF7\u1E99\u1EF5\u01B4\u024F\u1EFF]/g}, {'base':'z','letters':/[\u007A\u24E9\uFF5A\u017A\u1E91\u017C\u017E\u1E93\u1E95\u01B6\u0225\u0240\u2C6C\uA763]/g} ];
for(var i=0; i<defaultDiacriticsRemovalMap.length; i++) {
str = str.replace(defaultDiacriticsRemovalMap[i].letters, defaultDiacriticsRemovalMap[i].base);
}
return str;
}""";
-- End function simplifyDiacritics()
 
-- Make table occurences_iso2
CREATE OR REPLACE TABLE `localityservice.idigbio.occurrences_iso2`
AS
SELECT
coreid,
continent AS v_continent,
coordinatePrecision AS v_coordinateprecision,
coordinateUncertaintyInMeters AS v_coordinateuncertaintyinmeters,
country AS v_country,
countrycode AS v_countrycode,
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
verbatimLatitude AS v_verbatimlatitude,
verbatimLocality AS v_verbatimlocality,
verbatimLongitude AS v_verbatimlongitude,
verbatimSRS AS v_verbatimsrs,
waterBody AS v_waterbody,
SAFE_CAST(idigbio_decimallatitude_wgs84 AS FLOAT64) AS interpreted_decimallatitude,
SAFE_CAST(idigbio_decimallongitude_wgs84 AS FLOAT64) AS interpreted_decimallongitude,
b.iso2 AS interpreted_countrycode,
upper(geodeticdatum) AS u_datumstr
FROM `localityservice.idigbio.occurrences` AS a LEFT JOIN
`localityservice.vocabs.iso3_to_2` AS b ON a.idigbio_countrycode=b.iso3;
-- End table occurences_iso2
 
-- Make table temp_locations_idigbio_distinct
CREATE TEMP TABLE temp_locations_idigbio_distinct
AS
SELECT
SHA256(CONCAT(
"dwc:highergeographyid",
IFNULL(v_highergeographyid,""),
"dwc:highergeography",
IFNULL(v_highergeography,""),
"dwc:continent",
IFNULL(v_continent,""),
"dwc:waterbody",
IFNULL(v_waterbody,""),
"dwc:islandgroup",
IFNULL(v_islandgroup,""),
"dwc:island",
IFNULL(v_island,""),
"dwc:country",
IFNULL(v_country,""),
"dwc:countrycode",
IFNULL(v_countrycode,""),
"dwc:stateprovince",
IFNULL(v_stateprovince,""),
"dwc:county",
IFNULL(v_county,""),
"dwc:municipality",
IFNULL(v_municipality,""),
"dwc:locality",
IFNULL(v_locality,""),
"dwc:verbatimlocality",
IFNULL(v_verbatimlocality,""),
"dwc:minimumelevationinmeters",
IFNULL(v_minimumelevationinmeters,""),
"dwc:maximumelevationinmeters",
IFNULL(v_maximumelevationinmeters,""),
"dwc:verbatimelevation",
IFNULL(v_verbatimelevation,""),
"dwc:minimumdepthinmeters",
IFNULL(v_minimumdepthinmeters,""),
"dwc:maximumdepthinmeters",
IFNULL(v_maximumdepthinmeters,""),
"dwc:verbatimdepth",
IFNULL(v_verbatimdepth,""),
"dwc:minimumdistanceabovesurfaceinmeters",
IFNULL(v_minimumdistanceabovesurfaceinmeters,""),
"dwc:maximumdistanceabovesurfaceinmeters",
IFNULL(v_maximumdistanceabovesurfaceinmeters,""),
"dwc:locationaccordingto",
IFNULL(v_locationaccordingto,""),
"dwc:locationremarks",
IFNULL(v_locationremarks,""),
"dwc:decimallatitude",
IFNULL(v_decimallatitude,""),
"dwc:decimallongitude",
IFNULL(v_decimallongitude,""),
"dwc:geodeticdatum",
IFNULL(v_geodeticdatum,""),
"dwc:coordinateuncertaintyinmeters",
IFNULL(v_coordinateuncertaintyinmeters,""),
"dwc:coordinateprecision",
IFNULL(v_coordinateprecision,""),
"dwc:pointradiusspatialfit",
IFNULL(v_pointradiusspatialfit,""),
"dwc:verbatimcoordinates",
IFNULL(v_verbatimcoordinates,""),
"dwc:verbatimlatitude",
IFNULL(v_verbatimlatitude,""),
"dwc:verbatimlongitude",
IFNULL(v_verbatimlongitude,""),
"dwc:verbatimcoordinatesystem",
IFNULL(v_verbatimcoordinatesystem,""),
"dwc:verbatimsrs",
IFNULL(v_verbatimsrs,""),
"dwc:footprintwkt",
IFNULL(v_footprintwkt,""),
"dwc:footprintsrs",
IFNULL(v_footprintsrs,""),
"dwc:footprintspatialfit",
IFNULL(v_footprintspatialfit,""),
"dwc:georeferencedby",
IFNULL(v_georeferencedby,""),
"dwc:georeferenceddate",
IFNULL(v_georeferenceddate,""),
"dwc:georeferenceprotocol",
IFNULL(v_georeferenceprotocol,""),
"dwc:georeferencesources",
IFNULL(v_georeferencesources,""),
"dwc:georeferenceverificationstatus",
IFNULL(v_georeferenceverificationstatus,""),
"dwc:georeferenceremarks",
IFNULL(v_georeferenceremarks,"")))
AS dwc_location_hash,
CONCAT(
IFNULL(v_continent,"")," ",
IFNULL(v_waterbody,"")," ",
IFNULL(v_islandgroup,"")," ",
IFNULL(v_island,"")," ",
IFNULL(interpreted_countrycode,"")," ",
IFNULL(v_stateprovince,"")," ",
IFNULL(v_county,"")," ",
IFNULL(v_municipality,"")," ",
IFNULL(v_locality,"")," ",
IFNULL(v_verbatimlocality,"")," ",
IFNULL(v_minimumelevationinmeters,"")," ",
IFNULL(v_maximumelevationinmeters,"")," ",
IFNULL(v_verbatimelevation,"")," ",
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
IFNULL(v_continent,"")," ",
IFNULL(v_waterbody,"")," ",
IFNULL(v_islandgroup,"")," ",
IFNULL(v_island,"")," ",
IFNULL(interpreted_countrycode,"")," ",
IFNULL(v_stateprovince,"")," ",
IFNULL(v_county,"")," ",
IFNULL(v_municipality,"")," ",
IFNULL(v_locality,"")," ",
IFNULL(v_verbatimlocality,"")," ",
IFNULL(v_minimumelevationinmeters,"")," ",
IFNULL(v_maximumelevationinmeters,"")," ",
IFNULL(v_verbatimelevation,"")," ",
IFNULL(v_minimumdepthinmeters,"")," ",
IFNULL(v_maximumdepthinmeters,"")," ",
IFNULL(v_verbatimdepth,"")," ",
IFNULL(v_verbatimcoordinates,"")," ",
IFNULL(v_verbatimlatitude,"")," ",
IFNULL(v_verbatimlongitude,""))
AS for_match,
CONCAT(
IFNULL(v_continent,"")," ",
IFNULL(v_waterbody,"")," ",
IFNULL(v_islandgroup,"")," ",
IFNULL(v_island,"")," ",
IFNULL(interpreted_countrycode,"")," ",
IFNULL(v_stateprovince,"")," ",
IFNULL(v_county,"")," ",
IFNULL(v_municipality,"")," ",
IFNULL(v_locality,"")," ",
IFNULL(v_verbatimlocality,"")," ",
IFNULL(v_minimumelevationinmeters,"")," ",
IFNULL(v_maximumelevationinmeters,"")," ",
IFNULL(v_verbatimelevation,"")," ",
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
IFNULL(v_locality,"")," ",
IFNULL(v_verbatimlocality,"")," ",
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
v_georeferenceverificationstatus,
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
v_georeferenceverificationstatus,
v_georeferenceremarks,
interpreted_countrycode,
interpreted_decimallatitude,
interpreted_decimallongitude,
u_datumstr;
-- End table temp_locations_idigbio_distinct
 
-- Make table locations_idigbio_distinct
CREATE OR REPLACE TABLE localityservice.idigbio.locations_idigbio_distinct
AS
SELECT dwc_location_hash, v_highergeographyid, v_highergeography, v_continent, v_waterbody, v_islandgroup,
v_island, v_country, v_countrycode, v_stateprovince, v_county, v_municipality, v_locality, v_verbatimlocality,
v_minimumelevationinmeters, v_maximumelevationinmeters, v_verbatimelevation, v_minimumdepthinmeters,
v_maximumdepthinmeters, v_verbatimdepth, v_minimumdistanceabovesurfaceinmeters,
v_maximumdistanceabovesurfaceinmeters, v_locationaccordingto, v_locationremarks,
v_decimallatitude, v_decimallongitude, v_geodeticdatum, v_coordinateuncertaintyinmeters, v_coordinateprecision,
v_pointradiusspatialfit, v_verbatimcoordinates, v_verbatimlatitude, v_verbatimlongitude, v_verbatimcoordinatesystem,
v_verbatimsrs, v_footprintwkt, v_footprintsrs, v_footprintspatialfit, v_georeferencedby, v_georeferenceddate,
v_georeferenceprotocol, v_georeferencesources, v_georeferenceverificationstatus, v_georeferenceremarks,
interpreted_decimallatitude, interpreted_decimallongitude, interpreted_countrycode, occcount, u_datumstr,
REGEXP_REPLACE(REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_tokens)),NFKC)),r"[\s]+",' '), r"^\s+|\s+$", '') AS tokens,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match_with_coords)),NFKC)),r"[\s]+",'') AS matchme_with_coords,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match)),NFKC)),r"[\s]+",'') AS matchme,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match_sans_coords)),NFKC)),r"[\s]+",'') AS matchme_sans_coords
FROM temp_locations_idigbio_distinct;
-- End of CREATE table locations_idigbio_distinct
 
-- Make table locations_distinct_with_epsg
CREATE OR REPLACE TABLE localityservice.idigbio.locations_distinct_with_epsg
AS
SELECT a.*, epsg
FROM `localityservice.idigbio.locations_idigbio_distinct` a
LEFT JOIN `localityservice.vocabs.datumsepsg` b
ON a.u_datumstr=b.u_datumstr;
-- End table locations_distinct_with_epsg
 
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
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)<20037509,0,32) +
IF(v_georeferenceprotocol IS NULL,0,16) +
IF(v_georeferencesources IS NULL,0,8) +
IF(v_georeferenceddate IS NULL,0,4) +
IF(v_georeferencedby IS NULL,0,2) +
IF(v_georeferenceremarks IS NULL,0,1)
AS coordinates_score,
'iDigBio' AS source
FROM `localityservice.idigbio.locations_distinct_with_epsg`;
-- End table locations_distinct_with_scores
END;

-- VertNet Loading Script
-- Script to prepare georeference data for the gazetteer
-- Before running this script, run the loading scripts for all the data sources
BEGIN
CREATE TEMP FUNCTION saveNumbers (str STRING)
RETURNS STRING
LANGUAGE js AS
"""
{
// Replace any of , . /  - and +,  except in the middle of digits, with space
var changed = str.replace(/(?<!\\d)[.,\\-\\/\\+](?!\\d)/g,' ');
return changed;
}
""";
-- End function saveNumbers()
 
-- Make function removeSymbols()
CREATE TEMP FUNCTION removeSymbols (str STRING)
RETURNS STRING
LANGUAGE js AS
"""
{
// Removes most punctuation and symbols. Does not remove . , / - or +
var cleaned = str.replace(/[’'<>:‒–—―…!«»-‐?‘’“”;⁄␠·&@*•^¤¢$€£¥₩₪†‡°¡¿¬#№%‰‱¶′§~¨_|¦⁂☞∴‽※}{\\]\\[\\"\\)\\(]/g,'');
return cleaned;
}
""";
-- End function removeSymbols()
 
-- Make function simplifyDiacritics()
CREATE TEMP FUNCTION simplifyDiacritics (str STRING)
RETURNS STRING
LANGUAGE js AS """
{
// Normalizes unicode, lowercases, and changes diacritics to ASCII "equivalents"
var defaultDiacriticsRemovalMap = [ {'base':'A', 'letters':/[\u0041\u24B6\uFF21\u00C0\u00C1\u00C2\u1EA6\u1EA4\u1EAA\u1EA8\u00C3\u0100\u0102\u1EB0\u1EAE\u1EB4\u1EB2\u0226\u01E0\u00C4\u01DE\u1EA2\u00C5\u01FA\u01CD\u0200\u0202\u1EA0\u1EAC\u1EB6\u1E00\u0104\u023A\u2C6F]/g}, {'base':'AA','letters':/[\uA732]/g}, {'base':'AE','letters':/[\u00C6\u01FC\u01E2]/g}, {'base':'AO','letters':/[\uA734]/g}, {'base':'AU','letters':/[\uA736]/g}, {'base':'AV','letters':/[\uA738\uA73A]/g}, {'base':'AY','letters':/[\uA73C]/g}, {'base':'B', 'letters':/[\u0042\u24B7\uFF22\u1E02\u1E04\u1E06\u0243\u0182\u0181]/g}, {'base':'C', 'letters':/[\u0043\u24B8\uFF23\u0106\u0108\u010A\u010C\u00C7\u1E08\u0187\u023B\uA73E]/g}, {'base':'D', 'letters':/[\u0044\u24B9\uFF24\u1E0A\u010E\u1E0C\u1E10\u1E12\u1E0E\u0110\u018B\u018A\u0189\uA779]/g}, {'base':'DZ','letters':/[\u01F1\u01C4]/g}, {'base':'Dz','letters':/[\u01F2\u01C5]/g}, {'base':'E', 'letters':/[\u0045\u24BA\uFF25\u00C8\u00C9\u00CA\u1EC0\u1EBE\u1EC4\u1EC2\u1EBC\u0112\u1E14\u1E16\u0114\u0116\u00CB\u1EBA\u011A\u0204\u0206\u1EB8\u1EC6\u0228\u1E1C\u0118\u1E18\u1E1A\u0190\u018E]/g}, {'base':'F', 'letters':/[\u0046\u24BB\uFF26\u1E1E\u0191\uA77B]/g}, {'base':'G', 'letters':/[\u0047\u24BC\uFF27\u01F4\u011C\u1E20\u011E\u0120\u01E6\u0122\u01E4\u0193\uA7A0\uA77D\uA77E]/g}, {'base':'H', 'letters':/[\u0048\u24BD\uFF28\u0124\u1E22\u1E26\u021E\u1E24\u1E28\u1E2A\u0126\u2C67\u2C75\uA78D]/g}, {'base':'I', 'letters':/[\u0049\u24BE\uFF29\u00CC\u00CD\u00CE\u0128\u012A\u012C\u0130\u00CF\u1E2E\u1EC8\u01CF\u0208\u020A\u1ECA\u012E\u1E2C\u0197]/g}, {'base':'J', 'letters':/[\u004A\u24BF\uFF2A\u0134\u0248]/g}, {'base':'K', 'letters':/[\u004B\u24C0\uFF2B\u1E30\u01E8\u1E32\u0136\u1E34\u0198\u2C69\uA740\uA742\uA744\uA7A2]/g}, {'base':'L', 'letters':/[\u004C\u24C1\uFF2C\u013F\u0139\u013D\u1E36\u1E38\u013B\u1E3C\u1E3A\u0141\u023D\u2C62\u2C60\uA748\uA746\uA780]/g}, {'base':'LJ','letters':/[\u01C7]/g}, {'base':'Lj','letters':/[\u01C8]/g}, {'base':'M', 'letters':/[\u004D\u24C2\uFF2D\u1E3E\u1E40\u1E42\u2C6E\u019C]/g}, {'base':'N', 'letters':/[\u004E\u24C3\uFF2E\u01F8\u0143\u00D1\u1E44\u0147\u1E46\u0145\u1E4A\u1E48\u0220\u019D\uA790\uA7A4]/g}, {'base':'NJ','letters':/[\u01CA]/g}, {'base':'Nj','letters':/[\u01CB]/g}, {'base':'O', 'letters':/[\u004F\u24C4\uFF2F\u00D2\u00D3\u00D4\u1ED2\u1ED0\u1ED6\u1ED4\u00D5\u1E4C\u022C\u1E4E\u014C\u1E50\u1E52\u014E\u022E\u0230\u00D6\u022A\u1ECE\u0150\u01D1\u020C\u020E\u01A0\u1EDC\u1EDA\u1EE0\u1EDE\u1EE2\u1ECC\u1ED8\u01EA\u01EC\u00D8\u01FE\u0186\u019F\uA74A\uA74C]/g}, {'base':'OI','letters':/[\u01A2]/g}, {'base':'OO','letters':/[\uA74E]/g}, {'base':'OU','letters':/[\u0222]/g}, {'base':'P', 'letters':/[\u0050\u24C5\uFF30\u1E54\u1E56\u01A4\u2C63\uA750\uA752\uA754]/g}, {'base':'Q', 'letters':/[\u0051\u24C6\uFF31\uA756\uA758\u024A]/g}, {'base':'R', 'letters':/[\u0052\u24C7\uFF32\u0154\u1E58\u0158\u0210\u0212\u1E5A\u1E5C\u0156\u1E5E\u024C\u2C64\uA75A\uA7A6\uA782]/g}, {'base':'S', 'letters':/[\u0053\u24C8\uFF33\u1E9E\u015A\u1E64\u015C\u1E60\u0160\u1E66\u1E62\u1E68\u0218\u015E\u2C7E\uA7A8\uA784]/g}, {'base':'SS', 'letters':/[\u1E9E]/g}, {'base':'T', 'letters':/[\u0054\u24C9\uFF34\u1E6A\u0164\u1E6C\u021A\u0162\u1E70\u1E6E\u0166\u01AC\u01AE\u023E\uA786]/g}, {'base':'TZ','letters':/[\uA728]/g}, {'base':'U', 'letters':/[\u0055\u24CA\uFF35\u00D9\u00DA\u00DB\u0168\u1E78\u016A\u1E7A\u016C\u00DC\u01DB\u01D7\u01D5\u01D9\u1EE6\u016E\u0170\u01D3\u0214\u0216\u01AF\u1EEA\u1EE8\u1EEE\u1EEC\u1EF0\u1EE4\u1E72\u0172\u1E76\u1E74\u0244]/g}, {'base':'V', 'letters':/[\u0056\u24CB\uFF36\u1E7C\u1E7E\u01B2\uA75E\u0245]/g}, {'base':'VY','letters':/[\uA760]/g}, {'base':'W', 'letters':/[\u0057\u24CC\uFF37\u1E80\u1E82\u0174\u1E86\u1E84\u1E88\u2C72]/g}, {'base':'X', 'letters':/[\u0058\u24CD\uFF38\u1E8A\u1E8C]/g}, {'base':'Y', 'letters':/[\u0059\u24CE\uFF39\u1EF2\u00DD\u0176\u1EF8\u0232\u1E8E\u0178\u1EF6\u1EF4\u01B3\u024E\u1EFE]/g}, {'base':'Z', 'letters':/[\u005A\u24CF\uFF3A\u0179\u1E90\u017B\u017D\u1E92\u1E94\u01B5\u0224\u2C7F\u2C6B\uA762]/g}, {'base':'a', 'letters':/[\u0061\u24D0\uFF41\u1E9A\u00E0\u00E1\u00E2\u1EA7\u1EA5\u1EAB\u1EA9\u00E3\u0101\u0103\u1EB1\u1EAF\u1EB5\u1EB3\u0227\u01E1\u00E4\u01DF\u1EA3\u00E5\u01FB\u01CE\u0201\u0203\u1EA1\u1EAD\u1EB7\u1E01\u0105\u2C65\u0250]/g}, {'base':'aa','letters':/[\uA733]/g}, {'base':'ae','letters':/[\u00E6\u01FD\u01E3]/g}, {'base':'ao','letters':/[\uA735]/g}, {'base':'au','letters':/[\uA737]/g}, {'base':'av','letters':/[\uA739\uA73B]/g}, {'base':'ay','letters':/[\uA73D]/g}, {'base':'b', 'letters':/[\u0062\u24D1\uFF42\u1E03\u1E05\u1E07\u0180\u0183\u0253]/g}, {'base':'c', 'letters':/[\u0063\u24D2\uFF43\u0107\u0109\u010B\u010D\u00E7\u1E09\u0188\u023C\uA73F\u2184]/g}, {'base':'d', 'letters':/[\u0064\u24D3\uFF44\u1E0B\u010F\u1E0D\u1E11\u1E13\u1E0F\u0111\u018C\u0256\u0257\uA77A]/g}, {'base':'dz','letters':/[\u01F3\u01C6]/g}, {'base':'e', 'letters':/[\u0065\u24D4\uFF45\u00E8\u00E9\u00EA\u1EC1\u1EBF\u1EC5\u1EC3\u1EBD\u0113\u1E15\u1E17\u0115\u0117\u00EB\u1EBB\u011B\u0205\u0207\u1EB9\u1EC7\u0229\u1E1D\u0119\u1E19\u1E1B\u0247\u025B\u01DD]/g}, {'base':'f', 'letters':/[\u0066\u24D5\uFF46\u1E1F\u0192\uA77C]/g}, {'base':'g', 'letters':/[\u0067\u24D6\uFF47\u01F5\u011D\u1E21\u011F\u0121\u01E7\u0123\u01E5\u0260\uA7A1\u1D79\uA77F]/g}, {'base':'h', 'letters':/[\u0068\u24D7\uFF48\u0125\u1E23\u1E27\u021F\u1E25\u1E29\u1E2B\u1E96\u0127\u2C68\u2C76\u0265]/g}, {'base':'hv','letters':/[\u0195]/g}, {'base':'i', 'letters':/[\u0069\u24D8\uFF49\u00EC\u00ED\u00EE\u0129\u012B\u012D\u00EF\u1E2F\u1EC9\u01D0\u0209\u020B\u1ECB\u012F\u1E2D\u0268\u0131]/g}, {'base':'j', 'letters':/[\u006A\u24D9\uFF4A\u0135\u01F0\u0249]/g}, {'base':'k', 'letters':/[\u006B\u24DA\uFF4B\u1E31\u01E9\u1E33\u0137\u1E35\u0199\u2C6A\uA741\uA743\uA745\uA7A3]/g}, {'base':'l', 'letters':/[\u006C\u24DB\uFF4C\u0140\u013A\u013E\u1E37\u1E39\u013C\u1E3D\u1E3B\u017F\u0142\u019A\u026B\u2C61\uA749\uA781\uA747]/g}, {'base':'lj','letters':/[\u01C9]/g}, {'base':'m', 'letters':/[\u006D\u24DC\uFF4D\u1E3F\u1E41\u1E43\u0271\u026F]/g}, {'base':'n', 'letters':/[\u006E\u24DD\uFF4E\u01F9\u0144\u00F1\u1E45\u0148\u1E47\u0146\u1E4B\u1E49\u019E\u0272\u0149\uA791\uA7A5]/g}, {'base':'nj','letters':/[\u01CC]/g}, {'base':'o', 'letters':/[\u006F\u24DE\uFF4F\u00F2\u00F3\u00F4\u1ED3\u1ED1\u1ED7\u1ED5\u00F5\u1E4D\u022D\u1E4F\u014D\u1E51\u1E53\u014F\u022F\u0231\u00F6\u022B\u1ECF\u0151\u01D2\u020D\u020F\u01A1\u1EDD\u1EDB\u1EE1\u1EDF\u1EE3\u1ECD\u1ED9\u01EB\u01ED\u00F8\u01FF\u0254\uA74B\uA74D\u0275]/g}, {'base':'oi','letters':/[\u01A3]/g}, {'base':'ou','letters':/[\u0223]/g}, {'base':'oo','letters':/[\uA74F]/g}, {'base':'p','letters':/[\u0070\u24DF\uFF50\u1E55\u1E57\u01A5\u1D7D\uA751\uA753\uA755]/g}, {'base':'q','letters':/[\u0071\u24E0\uFF51\u024B\uA757\uA759]/g}, {'base':'r','letters':/[\u0072\u24E1\uFF52\u0155\u1E59\u0159\u0211\u0213\u1E5B\u1E5D\u0157\u1E5F\u024D\u027D\uA75B\uA7A7\uA783]/g}, {'base':'s','letters':/[\u0073\u24E2\uFF53\u015B\u1E65\u015D\u1E61\u0161\u1E67\u1E63\u1E69\u0219\u015F\u023F\uA7A9\uA785\u1E9B]/g}, {'base':'ss','letters':/[\u00DF]/g}, {'base':'t','letters':/[\u0074\u24E3\uFF54\u1E6B\u1E97\u0165\u1E6D\u021B\u0163\u1E71\u1E6F\u0167\u01AD\u0288\u2C66\uA787]/g}, {'base':'tz','letters':/[\uA729]/g}, {'base':'u','letters':/[\u0075\u24E4\uFF55\u00F9\u00FA\u00FB\u0169\u1E79\u016B\u1E7B\u016D\u00FC\u01DC\u01D8\u01D6\u01DA\u1EE7\u016F\u0171\u01D4\u0215\u0217\u01B0\u1EEB\u1EE9\u1EEF\u1EED\u1EF1\u1EE5\u1E73\u0173\u1E77\u1E75\u0289]/g}, {'base':'v','letters':/[\u0076\u24E5\uFF56\u1E7D\u1E7F\u028B\uA75F\u028C]/g}, {'base':'vy','letters':/[\uA761]/g}, {'base':'w','letters':/[\u0077\u24E6\uFF57\u1E81\u1E83\u0175\u1E87\u1E85\u1E98\u1E89\u2C73]/g}, {'base':'x','letters':/[\u0078\u24E7\uFF58\u1E8B\u1E8D]/g}, {'base':'y','letters':/[\u0079\u24E8\uFF59\u1EF3\u00FD\u0177\u1EF9\u0233\u1E8F\u00FF\u1EF7\u1E99\u1EF5\u01B4\u024F\u1EFF]/g}, {'base':'z','letters':/[\u007A\u24E9\uFF5A\u017A\u1E91\u017C\u017E\u1E93\u1E95\u01B6\u0225\u0240\u2C6C\uA763]/g} ];
for(var i=0; i<defaultDiacriticsRemovalMap.length; i++) {
str = str.replace(defaultDiacriticsRemovalMap[i].letters, defaultDiacriticsRemovalMap[i].base);
}
return str;
}""";
-- End function simplifyDiacritics()
 
-- Make table locations (need not persist)
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
VerificationStatus AS v_georeferenceverificationstatus,
verbatimCoordinateSystem AS v_verbatimcoordinatesystem
FROM `localityservice.vertnet.MaNISORNISHerpNETBestPracticeGeorefs`;
-- End table locations
 
-- Make table temp_locations_vertnet_distinct (need not persist)
CREATE OR REPLACE TABLE localityservice.vertnet.temp_locations_vertnet_distinct
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
'dwc:georeferenceverificationstatus', IFNULL(v_georeferenceverificationstatus,''),
'dwc:georeferenceremarks', IFNULL(v_georeferenceremarks,'')))
AS dwc_location_hash,
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
IFNULL(v_verbatimelevation,''), ' ',
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
IFNULL(v_locality,''),' ',
IFNULL(v_verbatimlocality,''),' ',
IFNULL(v_minimumelevationinmeters,''),' ',
IFNULL(v_maximumelevationinmeters,''),' ',
IFNULL(v_verbatimelevation,''))
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
IFNULL(v_verbatimelevation,''))
AS for_match_sans_coords,
CONCAT(
IFNULL(v_waterbody,''),' ',
IFNULL(v_islandgroup,''),' ',
IFNULL(v_island,''),' ',
IFNULL(v_country,''),' ',
IFNULL(v_stateprovince,''),' ',
IFNULL(v_county,''),' ',
IFNULL(v_locality,''),' ',
IFNULL(v_verbatimlocality,''),' ',
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
'' AS v_minimumdepthinmeters,
'' AS v_maximumdepthinmeters,
'' AS v_verbatimdepth,
'' AS v_minimumdistanceabovesurfaceinmeters,
'' AS v_maximumdistanceabovesurfaceinmeters,
'' AS v_locationaccordingto,
v_locationremarks,
'' AS v_decimallatitude,
'' AS v_decimallongitude,
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
v_georeferenceverificationstatus,
v_georeferenceremarks,
interpreted_decimallatitude,
interpreted_decimallongitude,
interpreted_countrycode,
0 AS occcount,
'WGS84' AS u_datumstr,
source
FROM localityservice.vertnet.locations;
-- End table temp_locations_vertnet_distinct
 
-- Make table locations_distinct_with_scores (persist)
CREATE OR REPLACE TABLE localityservice.vertnet.locations_distinct_with_scores
AS
SELECT dwc_location_hash, v_highergeographyid, v_highergeography, v_continent, v_waterbody, v_islandgroup,
v_island, v_country, v_countrycode, v_stateprovince, v_county, v_municipality, v_locality, v_verbatimlocality,
v_minimumelevationinmeters, v_maximumelevationinmeters, v_verbatimelevation, v_minimumdepthinmeters,
v_maximumdepthinmeters, v_verbatimdepth, v_minimumdistanceabovesurfaceinmeters,
v_maximumdistanceabovesurfaceinmeters, v_locationaccordingto, v_locationremarks,
v_decimallatitude, v_decimallongitude, v_geodeticdatum, v_coordinateuncertaintyinmeters, v_coordinateprecision,
v_pointradiusspatialfit, v_verbatimcoordinates, v_verbatimlatitude, v_verbatimlongitude, v_verbatimcoordinatesystem,
v_verbatimsrs, v_footprintwkt, v_footprintsrs, v_footprintspatialfit, v_georeferencedby, v_georeferenceddate,
v_georeferenceprotocol, v_georeferencesources, v_georeferenceverificationstatus, v_georeferenceremarks,
interpreted_decimallatitude, interpreted_decimallongitude, interpreted_countrycode, occcount, u_datumstr,
REGEXP_REPLACE(REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_tokens)),NFKC)),r"[\s]+",' '), r"^\s+|\s+$", '') AS tokens,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match_with_coords)),NFKC)),r"[\s]+",'') AS matchme_with_coords,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match)),NFKC)),r"[\s]+",'') AS matchme,
REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(for_match_sans_coords)),NFKC)),r"[\s]+",'') AS matchme_sans_coords,
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
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)<20037509,0,32) +
IF(v_georeferenceprotocol IS NULL,0,16) +
IF(v_georeferencesources IS NULL,0,8) +
IF(v_georeferenceddate IS NULL,0,4) +
IF(v_georeferencedby IS NULL,0,2) +
IF(v_georeferenceremarks IS NULL,0,1)
AS coordinates_score,
source
FROM localityservice.vertnet.temp_locations_vertnet_distinct;
END;

-- Gazetteer Loading Script
-- Script to prepare georeference data for the gazetteer
-- Before running this script, run the loading scripts for all the data sources
BEGIN
-- Make table locations_distinct_with_scores
CREATE OR REPLACE TABLE localityservice.gazetteer.locations_distinct_with_scores
AS
SELECT *
FROM `localityservice.gbif.locations_distinct_with_scores`;
INSERT INTO `localityservice.gazetteer.locations_distinct_with_scores`
(SELECT *
FROM localityservice.idigbio.locations_distinct_with_scores
WHERE
dwc_location_hash NOT IN
(SELECT DISTINCT dwc_location_hash
FROM localityservice.gazetteer.locations_distinct_with_scores)
);
INSERT INTO `localityservice.gazetteer.locations_distinct_with_scores`
(SELECT *
FROM localityservice.vertnet.locations_distinct_with_scores
WHERE
dwc_location_hash NOT IN
(SELECT DISTINCT dwc_location_hash
FROM localityservice.gazetteer.locations_distinct_with_scores)
);
 
-- Make table locations_with_georefs_combined
CREATE OR REPLACE TABLE localityservice.gazetteer.locations_with_georefs_combined
AS
SELECT *,
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC) AS unc_numeric,
FROM `localityservice.gazetteer.locations_distinct_with_scores`
WHERE
epsg IS NOT NULL AND
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)>=1 AND
SAFE_CAST(v_coordinateuncertaintyinmeters AS NUMERIC)<20037509 AND
interpreted_decimallatitude IS NOT NULL AND
interpreted_decimallongitude IS NOT NULL AND
interpreted_decimallatitude<>0 AND interpreted_decimallongitude<>0;
 
-- Make table matchme_sans_coords_with_georefs
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_with_georefs
AS
SELECT
matchme_sans_coords,
unc_numeric,
ST_GEOGPOINT(interpreted_decimallongitude, interpreted_decimallatitude) as center,
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
count(*) as georef_count
FROM `localityservice.gazetteer.locations_with_georefs_combined`
GROUP BY
matchme_sans_coords,
unc_numeric,
interpreted_decimallongitude,
interpreted_decimallatitude,
interpreted_countrycode,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
georef_score,
source;
 
-- Make table matchme_verbatimcoords_with_georefs
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_with_georefs
AS
SELECT
matchme,
unc_numeric,
ST_GEOGPOINT(interpreted_decimallongitude, interpreted_decimallatitude) as center,
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
count(*) as georef_count
FROM `localityservice.gazetteer.locations_with_georefs_combined`
GROUP BY
matchme,
unc_numeric,
interpreted_decimallongitude,
interpreted_decimallatitude,
interpreted_countrycode,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
georef_score,
source;
 
-- Make table matchme_with_coords_with_georefs
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_with_georefs
AS
SELECT
matchme_with_coords,
unc_numeric,
ST_GEOGPOINT(interpreted_decimallongitude, interpreted_decimallatitude) as center, interpreted_decimallongitude,
interpreted_decimallatitude,
interpreted_countrycode,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
georef_score,
source,
count(*) as georef_count
FROM `localityservice.gazetteer.locations_with_georefs_combined`
GROUP BY
matchme_with_coords,
unc_numeric,
interpreted_decimallongitude,
interpreted_decimallatitude,
interpreted_countrycode,
v_georeferencedby,
v_georeferenceddate,
v_georeferenceprotocol,
v_georeferencesources,
v_georeferenceremarks,
georef_score,
source;
 
-- Make table matchme_sans_coords_max_mins
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_max_mins
AS
SELECT
matchme_sans_coords,
max(unc_numeric) as max_uncertainty,
max(interpreted_decimallatitude) AS maxlat,
max(interpreted_decimallongitude) AS maxlong,
min(interpreted_decimallatitude) AS minlat,
min(interpreted_decimallongitude) AS minlong
FROM `localityservice.gazetteer.matchme_sans_coords_with_georefs`
GROUP BY matchme_sans_coords;
 
-- Make table matchme_verbatimcoords_max_mins
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_max_mins
AS
SELECT
matchme,
max(unc_numeric) as max_uncertainty,
max(interpreted_decimallatitude) AS maxlat,
max(interpreted_decimallongitude) AS maxlong,
min(interpreted_decimallatitude) AS minlat,
min(interpreted_decimallongitude) AS minlong
FROM `localityservice.gazetteer.matchme_verbatimcoords_with_georefs`
GROUP BY matchme;
 
-- Make table matchme_with_coords_max_mins
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_max_mins
AS
SELECT
matchme_with_coords,
max(unc_numeric) as max_uncertainty,
max(interpreted_decimallatitude) AS maxlat,
max(interpreted_decimallongitude) AS maxlong,
min(interpreted_decimallatitude) AS minlat,
min(interpreted_decimallongitude) AS minlong
FROM `localityservice.gazetteer.matchme_with_coords_with_georefs`
GROUP BY matchme_with_coords;
 
-- Make table matchme_sans_coords_bb_filtered
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_bb_filtered
AS
SELECT
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))
AS nsdist,
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))
AS ewdist,
max_uncertainty,
center,
a.matchme_sans_coords
FROM
`localityservice.gazetteer.matchme_sans_coords_max_mins` a,
`localityservice.gazetteer.matchme_sans_coords_with_georefs` b
WHERE a.matchme_sans_coords=b.matchme_sans_coords
AND unc_numeric=max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty;
 
-- Make table matchme_verbatimcoords_bb_filtered
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_bb_filtered
AS
SELECT
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))
AS nsdist,
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))
AS ewdist,
max_uncertainty,
center,
a.matchme
FROM
`localityservice.gazetteer.matchme_verbatimcoords_max_mins` a,
`localityservice.gazetteer.matchme_verbatimcoords_with_georefs` b
WHERE a.matchme=b.matchme
AND unc_numeric=max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty;
 
-- Make table matchme_with_coords_bb_filtered
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_bb_filtered
AS
SELECT
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))
AS nsdist,
ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))
AS ewdist,
max_uncertainty,
center,
a.matchme_with_coords
FROM
`localityservice.gazetteer.matchme_with_coords_max_mins` a,
`localityservice.gazetteer.matchme_with_coords_with_georefs` b
WHERE a.matchme_with_coords=b.matchme_with_coords
AND unc_numeric=max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(minlong,maxlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,minlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty
AND ST_DISTANCE(ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(maxlong,maxlat),ST_GEOGPOINT(maxlong,minlat))),ST_CENTROID(ST_MAKELINE(ST_GEOGPOINT(minlong,maxlat),ST_GEOGPOINT(minlong,minlat))))<=2*max_uncertainty;
 
-- Make table matchme_sans_coords_agg_centers
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_agg_centers
AS
SELECT
a.matchme_sans_coords,
max_uncertainty,
count(*) as georefcount,
ARRAY_AGG(a.center) as centers
FROM
`localityservice.gazetteer.matchme_sans_coords_with_georefs` a,
`localityservice.gazetteer.matchme_sans_coords_bb_filtered` b
WHERE a.matchme_sans_coords=b.matchme_sans_coords
GROUP BY a.matchme_sans_coords, max_uncertainty;
 
-- Make table matchme_verbatimcoords_agg_centers
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_agg_centers
AS
SELECT
a.matchme,
max_uncertainty,
count(*) as georefcount,
ARRAY_AGG(a.center) as centers
FROM
`localityservice.gazetteer.matchme_verbatimcoords_with_georefs` a,
`localityservice.gazetteer.matchme_verbatimcoords_bb_filtered` b
WHERE a.matchme=b.matchme
GROUP BY a.matchme, max_uncertainty;
 
-- Make table matchme_with_coords_agg_centers
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_agg_centers
AS
SELECT
a.matchme_with_coords,
max_uncertainty,
count(*) as georefcount,
ARRAY_AGG(a.center) as centers
FROM
`localityservice.gazetteer.matchme_with_coords_with_georefs` a,
`localityservice.gazetteer.matchme_with_coords_bb_filtered` b
WHERE a.matchme_with_coords=b.matchme_with_coords
GROUP BY a.matchme_with_coords, max_uncertainty;
 
-- Make table matchme_sans_coords_bb_filtered_centroids
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_bb_filtered_centroids
AS
SELECT
matchme_sans_coords,
max_uncertainty,
georefcount,
(
SELECT ST_CENTROID_AGG(p) AS st_centroid_agg
FROM
(SELECT * FROM UNNEST(centers) AS p)
) as centroid
FROM `localityservice.gazetteer.matchme_sans_coords_agg_centers`;
 
-- Make table matchme_verbatimcoords_bb_filtered_centroids
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_bb_filtered_centroids
AS
SELECT
matchme,
max_uncertainty,
georefcount,
(
SELECT ST_CENTROID_AGG(p) AS st_centroid_agg
FROM
(SELECT * FROM UNNEST(centers) AS p)
) as centroid
FROM `localityservice.gazetteer.matchme_verbatimcoords_agg_centers`;
 
-- Make table matchme_with_coords_bb_filtered_centroids
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_bb_filtered_centroids
AS
SELECT
matchme_with_coords,
max_uncertainty,
georefcount,
(
SELECT ST_CENTROID_AGG(p) AS st_centroid_agg
FROM
(SELECT * FROM UNNEST(centers) AS p)
) as centroid
FROM `localityservice.gazetteer.matchme_with_coords_agg_centers`;
 
-- Make table matchme_sans_coords_min_centroid_dist
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_min_centroid_dist
AS
SELECT
a.matchme_sans_coords,
ST_DISTANCE(b.center, centroid) as centroid_dist,
min(ST_DISTANCE(b.center, centroid)) as min_centroid_dist,
max_uncertainty
FROM
`localityservice.gazetteer.matchme_sans_coords_bb_filtered_centroids` a,
`localityservice.gazetteer.matchme_sans_coords_with_georefs` b
WHERE
a.matchme_sans_coords=b.matchme_sans_coords AND
b.unc_numeric=max_uncertainty
GROUP BY
matchme_sans_coords,
centroid_dist,
max_uncertainty;
 
-- Make table matchme_verbatimcoords_min_centroid_dist
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_min_centroid_dist
AS
SELECT
a.matchme,
ST_DISTANCE(b.center, centroid) as centroid_dist,
min(ST_DISTANCE(b.center, centroid)) as min_centroid_dist,
max_uncertainty
FROM
`localityservice.gazetteer.matchme_verbatimcoords_bb_filtered_centroids` a,
`localityservice.gazetteer.matchme_verbatimcoords_with_georefs` b
WHERE
a.matchme=b.matchme AND
b.unc_numeric=max_uncertainty
GROUP BY
matchme,
centroid_dist,
max_uncertainty;
 
-- Make table matchme_with_coords_min_centroid_dist
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_min_centroid_dist
AS
SELECT a.matchme_with_coords,
ST_DISTANCE(b.center, centroid) as centroid_dist,
min(ST_DISTANCE(b.center, centroid)) as min_centroid_dist,
max_uncertainty
FROM
`localityservice.gazetteer.matchme_with_coords_bb_filtered_centroids` a,
`localityservice.gazetteer.matchme_with_coords_with_georefs` b
WHERE
a.matchme_with_coords=b.matchme_with_coords AND
b.unc_numeric=max_uncertainty
GROUP BY
matchme_with_coords,
centroid_dist,
max_uncertainty;
 
-- Make table matchme_sans_coords_best_georef_candidates
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_best_georef_candidates
AS
SELECT
b.*,
max_uncertainty,
centroid_dist,
min_centroid_dist,
SHA256(CONCAT(
IFNULL(a.matchme_sans_coords,""),
IFNULL(SAFE_CAST(unc_numeric AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallatitude AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallongitude AS STRING),""),
IFNULL(interpreted_countrycode,""),
IFNULL(v_georeferencedby,""),
IFNULL(v_georeferenceddate,""),
IFNULL(v_georeferenceprotocol,""),
IFNULL(v_georeferencesources,""),
IFNULL(v_georeferenceremarks,"")))
AS matchid
FROM
`localityservice.gazetteer.matchme_sans_coords_min_centroid_dist` a,
`localityservice.gazetteer.matchme_sans_coords_with_georefs` b
WHERE
a.matchme_sans_coords=b.matchme_sans_coords AND
centroid_dist=min_centroid_dist AND
unc_numeric=max_uncertainty;
 
-- Make table matchme_verbatimcoords_best_georef_candidates
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_best_georef_candidates
AS
SELECT
b.*,
max_uncertainty,
centroid_dist,
min_centroid_dist,
SHA256(CONCAT(
IFNULL(a.matchme,""),
IFNULL(SAFE_CAST(unc_numeric AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallatitude AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallongitude AS STRING),""),
IFNULL(interpreted_countrycode,""),
IFNULL(v_georeferencedby,""),
IFNULL(v_georeferenceddate,""),
IFNULL(v_georeferenceprotocol,""),
IFNULL(v_georeferencesources,""),
IFNULL(v_georeferenceremarks,""))) as matchid
FROM
`localityservice.gazetteer.matchme_verbatimcoords_min_centroid_dist` a,
`localityservice.gazetteer.matchme_verbatimcoords_with_georefs` b
WHERE
a.matchme=b.matchme AND
centroid_dist=min_centroid_dist AND
unc_numeric=max_uncertainty;
 
-- Make table matchme_with_coords_best_georef_candidates
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_best_georef_candidates
AS
SELECT
b.*,
max_uncertainty,
centroid_dist,
min_centroid_dist,
SHA256(CONCAT(
IFNULL(a.matchme_with_coords,""),
IFNULL(SAFE_CAST(unc_numeric AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallatitude AS STRING),""),
IFNULL(SAFE_CAST(interpreted_decimallongitude AS STRING),""),
IFNULL(interpreted_countrycode,""),
IFNULL(v_georeferencedby,""),
IFNULL(v_georeferenceddate,""),
IFNULL(v_georeferenceprotocol,""),
IFNULL(v_georeferencesources,""),
IFNULL(v_georeferenceremarks,""))) as matchid
FROM
`localityservice.gazetteer.matchme_with_coords_min_centroid_dist` a,
`localityservice.gazetteer.matchme_with_coords_with_georefs` b
WHERE
a.matchme_with_coords=b.matchme_with_coords AND
centroid_dist=min_centroid_dist AND
unc_numeric=max_uncertainty;
 
-- Make table matchme_sans_coords_max_dist
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_max_dist
AS
SELECT
matchid,
a.matchme_sans_coords,
max(ST_DISTANCE(a.center, b.center)) as max_dist
FROM
`localityservice.gazetteer.matchme_sans_coords_best_georef_candidates` a,
`localityservice.gazetteer.matchme_sans_coords_with_georefs` b
WHERE a.matchme_sans_coords=b.matchme_sans_coords
GROUP BY
matchid,
matchme_sans_coords;
 
-- Make table matchme_verbatimcoords_max_dist
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_max_dist
AS
SELECT
matchid,
a.matchme,
max(ST_DISTANCE(a.center, b.center)) as max_dist
FROM
`localityservice.gazetteer.matchme_verbatimcoords_best_georef_candidates` a,
`localityservice.gazetteer.matchme_verbatimcoords_with_georefs` b
WHERE a.matchme=b.matchme
GROUP BY
matchid,
matchme;
 
-- Make table matchme_with_coords_max_dist
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_max_dist
AS
SELECT
matchid,
a.matchme_with_coords,
max(ST_DISTANCE(a.center, b.center)) as max_dist
FROM
`localityservice.gazetteer.matchme_with_coords_best_georef_candidates` a,
`localityservice.gazetteer.matchme_with_coords_with_georefs` b
WHERE a.matchme_with_coords=b.matchme_with_coords
GROUP BY
matchid,
matchme_with_coords;
 
-- Make table matchme_sans_coords_best_georef
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_sans_coords_best_georef
AS
SELECT
  * EXCEPT(rn)
FROM
(
SELECT
b.*,
ROW_NUMBER() OVER(PARTITION BY a.matchme_sans_coords ORDER BY b.georef_score DESC) AS rn
FROM
`localityservice.gazetteer.matchme_sans_coords_max_dist` a,
`localityservice.gazetteer.matchme_sans_coords_best_georef_candidates` b
WHERE
a.matchme_sans_coords=b.matchme_sans_coords AND
max_dist<=max_uncertainty
)
WHERE rn = 1;
 
-- Make table matchme_verbatimcoords_best_georef
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_verbatimcoords_best_georef
AS
SELECT
  * EXCEPT(rn)
FROM
(
SELECT
b.*,
ROW_NUMBER() OVER(PARTITION BY a.matchme ORDER BY b.georef_score DESC) AS rn
FROM
`localityservice.gazetteer.matchme_verbatimcoords_max_dist` a,
`localityservice.gazetteer.matchme_verbatimcoords_best_georef_candidates` b
WHERE
a.matchme=b.matchme AND
max_dist<=max_uncertainty
)
WHERE rn = 1;
 
-- Make table matchme_with_coords_best_georef
CREATE OR REPLACE TABLE localityservice.gazetteer.matchme_with_coords_best_georef
AS
SELECT
  * EXCEPT(rn)
FROM
(
SELECT
b.*,
ROW_NUMBER() OVER(PARTITION BY a.matchme_with_coords ORDER BY b.georef_score DESC) AS rn
FROM
`localityservice.gazetteer.matchme_with_coords_max_dist` a,
`localityservice.gazetteer.matchme_with_coords_best_georef_candidates` b
WHERE
a.matchme_with_coords=b.matchme_with_coords AND
max_dist<=max_uncertainty
)
WHERE rn = 1;
 
END;
 

