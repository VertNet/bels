# Biodiversity Enhanced Location Services (BELS)
Biodiversity Enhanced Location Services, affectionately known as "BELS", is envisioned to be a suite of services to facilitate the identification, persistence, management, sameAs assertions, standardizations, representations, georeferencing, and sharing of location-based data with a target scope of biodiversity. The work here stems from concepts presented and discussed in the community forum "[Imagining a global gazetteer of georeferences](https://www.idigbio.org/content/darwin-core-hour-2-bbqs-imagining-global-gazetteer-georeferences)" as well as in "[Quality issues in georeferencing: From physical collections to digital data repositories for ecological research](https://doi.org/10.1111/ddi.13208)".

To date, the services include: 
* A base data store of verbatim data shared via [VertNet](http://vertnet.org), [iDigBio](https://www.idigbio.org/) and [GBIF](https://www.gbif.org/)
* An extract of distinct combinations of values of [Darwin Core Location terms](https://dwc.tdwg.org/terms/#location) found in the sources (the "Gazetteer"), currently as of March 2022
* Location identifiers based on a SHA256 hash of a formula based on the content of the Location terms of Darwin Core
* Lookups from Location identifiers to all Occurrences from the same Location
* Standardization of coordinate reference systems mentioned in verbatim data to [EPSG codes](http://epsg.io/)
* Location matching strings based on several levels and types of string simplification, used to assert that two Locations are the same place even if there are variations in how they were captured textually. Examples of simplifications include:
* * a) the normalization of distinct Unicode characters that have the same meaning to a single common value, 
* * b) removal of punctuation that does not affect numbers, 
* * c) setting all matching strings to lowercase
* * d) the translation of accented characters to ASCII equivalents.
* Georeferences derived from Location data along with weighted metadata scores to assess georeference quality based on a) their compliance with georeferencing best practices ([Chapman & Wieczorek 2020](https://doi.org/10.15468/doc-gg7h-s853), [Zermoglio et al. 2020](https://doi.org/10.35035/e09p-h128)), b) their ability to represent the whole set of possible spatial representations for that string, and c) a metadata score. The weighted values of metadata terms that are summed to make the metadata score are:
* * 1 - georeferenceRemarks
* * 2 - georeferencedBy
* * 4 - georeferencedDate
* * 8 - georeferenceSources
* * 16 - georeferenceProtocol
* * 32 - interpretable realistic coordinate uncertainty
* * 64 - interpretable epsg code
* * 128 - interpreted decimal coordinates
* Preprocessed single best matching georeferences for each matching string, if available, for three types of matches:
* * Using no coordinate information
* * Using verbatim coordinate information, but not decimalLatitude and decimalLongitude
* * Using all coordinate information
* Scripts to support reading input Locations from CSV files, determine the Location hash identifier and matching strings in exactly the same way they were made within the gazetteer, and find the best georefrence for each of the three types of matching strings.
* Using the web interface (temporarily at https://localityservice.uc.r.appspot.com/, but eventually destined for [georeferencing.org](http://georeferencing.org/)), one can upload a file with Locations (and any additional data fields) and provide an email address. When the file has been uploaded and processed, the results, which include all of the input fields plus fields for the best georeference available, are saved in a CSV file and the link to the file is sent to the email provided by the requester.

Next steps include a REST API and the incorporation of higher geography term standardization based on [VertNet vocabularies]( https://github.com/VertNet/DwCVocabs/blob/master/vocabs/Geography.csv), [Kurator workflows](https://github.com/kurator-org/kurator-validation/blob/master/packages/kurator_dwca/workflows/dwca_geography_cleaner.yaml), and verbatim data shared via VertNet, iDigBio, and GBIF. 
