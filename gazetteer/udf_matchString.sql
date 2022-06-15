-- Note: This function is to be used via the BELS Georef Matcher API to construct 
-- matching strings from Darwinized input data. 
-- New: In this version, continent was omitted, verticalDatum was added, and decimal 
-- coordinates were rounded to seven digits.
CREATE OR REPLACE FUNCTION `localityservice.functions.matchString`(json_row STRING, matchtype STRING) RETURNS STRING LANGUAGE js AS R"""
 var geogfields = {"waterbody":"", "islandgroup":"", "island":"", "interpreted_countrycode":"", "stateprovince":"", "county":"", "municipality":""};
 var elevdepthfields = {"minimumelevationinmeters":"", "maximumelevationinmeters":"", "verbatimelevation":"", "verticaldatum":"", "minimumdepthinmeters":"", "maximumdepthinmeters":"", "verbatimdepth":""};
 var vcoordsfields = {"verbatimcoordinates":"", "verbatimlatitude":"", "verbatimlongitude":""};
 function isNumber(str) {
   // coerce str to be a string, just in case
   str = ""+str
   return !isNaN(str) && !isNaN(parseFloat(str))
 }
   function roundToSeven(num) {
   // get num rounded to seven digits
   return +(Math.round(num + "e+7")  + "e-7");
 }
 function matchrow(obj, matchtype) {
   var match ="";
 
   for (var field in geogfields) {
     lfield = field.toLowerCase()
     if (obj.hasOwnProperty(lfield) && obj[lfield] != null && obj[lfield].trim() != "") {
       match += obj[lfield];
     }
   }
   var loc = "locality";
   var vloc = "verbatimlocality";
   var locval = ""
   var vlocval = ""
   var loccombo = "";
   if (obj.hasOwnProperty(loc) && obj[loc] != null && obj[loc].trim() != "") {
     locval = obj[loc].trim().toLowerCase();
   }
   if (obj.hasOwnProperty(vloc) && obj[vloc] != null && obj[vloc].trim() != "") {
     vlocval = obj[vloc].trim().toLowerCase();
   }
   if (locval === vlocval) {
     loccombo = locval;
   } else {
     loccombo = (locval+vlocval).trim();
   }
   match += loccombo;
   for (var field in elevdepthfields) {
     lfield = field.toLowerCase()
     if (obj.hasOwnProperty(lfield) && obj[lfield] != null && obj[lfield].trim() != "") {
       match += obj[lfield];
     }
   }
   if (matchtype === "verbatimcoords" || matchtype === "withcoords") {
     for (var field in vcoordsfields) {
       lfield = field.toLowerCase()
       if (obj.hasOwnProperty(lfield) && obj[lfield] != null && obj[lfield].trim() != "") {
         match += obj[lfield];
       }
     }
   }
   if (matchtype === "withcoords") {
     var vlat = "decimallatitude"
     var vlng = "decimallongitude"
     if (obj.hasOwnProperty(vlat) && obj[vlat] != null && obj[vlat].trim() != "") {
       if (isNumber(obj[vlat])) {
         if (obj.hasOwnProperty(vlng) && obj[vlng] != null && obj[vlng].trim() != "") {
           if (isNumber(obj[vlng])) {
             // decimallatitude and decimallongitude are both numbers, round them to seven digits
             ilat = +roundToSeven(obj[vlat])
             ilng = +roundToSeven(obj[vlng])
             match += ilat
             match += ilng
           }
         } 
       }
     }
   }
   return match;
 }
 var row = JSON.parse(json_row);
 return matchrow(row, matchtype);
""";
