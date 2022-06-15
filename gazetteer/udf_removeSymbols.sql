CREATE OR REPLACE FUNCTION `localityservice.functions.removeSymbols`(str STRING) RETURNS STRING LANGUAGE js AS R"""
{
// Removes most punctuation and symbols. Does not remove . , / - or +
var cleaned = str.replace(/[’'<>:‒–—―…!«»-‐?‘’“”;⁄␠·&@*•^¤¢$€£¥₩₪†‡°¡¿¬#№%‰‱¶′§~¨_|¦⁂☞∴‽※}{\]\[\"\)\(]/g,'');
return cleaned;
}
""";
