CREATE OR REPLACE FUNCTION `localityservice.functions.saveNumbers`(str STRING) RETURNS STRING LANGUAGE js AS R"""
{
// Replace any of , . /  - and +,  except in the middle of digits, with space
var changed = str.replace(/(?<!\d)[.,\-\/\+](?!\d)/g,' ');
return changed;
}
""";
