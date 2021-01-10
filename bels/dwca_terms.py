#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__author__ = "John Wieczorek"
__copyright__ = "Copyright 2021 Rauthiflor LLC"
__version__ = "dwca_terms.py 2021-01-09T15:28-03:00"
__adapted_from__ = "https://github.com/kurator-org/kurator-validation/blob/master/packages/kurator_dwca/dwca_terms.py"

# This file contains definitions of standard sets of Darwin Core terms.

# Terms that make up the current Simple Darwin Core
simpledwctermlist = [
    'type','modified','language','license','rightsHolder','accessRights',
    'bibliographicCitation','references','institutionID','collectionID','datasetID',
    'institutionCode','collectionCode','datasetName','ownerInstitutionCode',
    'basisOfRecord','informationWithheld','dataGeneralizations','dynamicProperties',
    'occurrenceID','catalogNumber','recordNumber','recordedBy','individualCount',
    'organismQuantity','organismQuantityType','sex','lifeStage','reproductiveCondition',
    'behavior','establishmentMeans','degreeOfEstablishment','pathway','occurrenceStatus',
    'preparations','disposition','associatedMedia','associatedReferences',
    'associatedSequences','associatedTaxa','otherCatalogNumbers','occurrenceRemarks',
    'organismID','organismName','organismScope','associatedOccurrences',
    'associatedOrganisms','previousIdentifications','organismRemarks','materialSampleID',
    'eventID','parentEventID','fieldNumber','eventDate','eventTime','startDayOfYear',
    'endDayOfYear','year','month','day','verbatimEventDate','habitat','samplingProtocol',
    'samplingEffort','sampleSizeValue','sampleSizeUnit','fieldNotes','eventRemarks',
    'locationID','higherGeographyID','higherGeography','continent','waterBody',
    'islandGroup','island','country','countryCode','stateProvince','county',
    'municipality','locality','verbatimLocality','minimumElevationInMeters',
    'maximumElevationInMeters','verbatimElevation','minimumDepthInMeters',
    'maximumDepthInMeters','verbatimDepth','minimumDistanceAboveSurfaceInMeters',
    'maximumDistanceAboveSurfaceInMeters','locationAccordingTo','locationRemarks',
    'decimalLatitude','decimalLongitude','geodeticDatum','coordinateUncertaintyInMeters',
    'coordinatePrecision','pointRadiusSpatialFit','verbatimCoordinates',
    'verbatimLatitude','verbatimLongitude','verbatimCoordinateSystem','verbatimSRS',
    'footprintWKT','footprintSRS','footprintSpatialFit','georeferencedBy',
    'georeferencedDate','georeferenceProtocol','georeferenceSources',
    'georeferenceVerificationStatus','georeferenceRemarks',
    'geologicalContextID','earliestEonOrLowestEonothem','latestEonOrHighestEonothem',
    'earliestEraOrLowestErathem','latestEraOrHighestErathem',
    'earliestPeriodOrLowestSystem','latestPeriodOrHighestSystem',
    'earliestEpochOrLowestSeries','latestEpochOrHighestSeries','earliestAgeOrLowestStage',
    'latestAgeOrHighestStage','lowestBiostratigraphicZone','highestBiostratigraphicZone',
    'lithostratigraphicTerms','group','formation','member','bed',
    'identificationID','identificationQualifier','typeStatus','identifiedBy',
    'dateIdentified','identificationReferences','identificationVerificationStatus',
    'identificationRemarks',
    'taxonID','scientificNameID','acceptedNameUsageID','parentNameUsageID',
    'originalNameUsageID','nameAccordingToID','namePublishedInID','taxonConceptID',
    'scientificName','acceptedNameUsage','parentNameUsage','originalNameUsage',
    'nameAccordingTo','namePublishedIn','namePublishedInYear','higherClassification',
    'kingdom','phylum','class','order','family','genus','subgenus','specificEpithet',
    'infraspecificEpithet','taxonRank','verbatimTaxonRank','scientificNameAuthorship',
    'vernacularName','nomenclaturalCode','taxonomicStatus','nomenclaturalStatus',
    'taxonRemarks']

# Terms that make up a distinct geography combination
geogkeytermlist = [
    'continent', 'country', 'countryCode', 'stateProvince', 'county', 'municipality', 
    'waterBody', 'islandGroup', 'island']

# Terms that make up a distinct Location
locationkeytermlist = [
    'dwc:highergeographyid', 'dwc:highergeography', 'dwc:continent', 'dwc:waterbody', 
    'dwc:islandgroup', 'dwc:island', 'dwc:country', 'dwc:countrycode', 
    'dwc:stateprovince', 'dwc:county', 'dwc:municipality', 'dwc:locality', 
    'dwc:verbatimlocality', 'dwc:minimumelevationinmeters','dwc:maximumelevationinmeters',
    'dwc:verbatimelevation', 'dwc:minimumdepthinmeters', 'dwc:maximumdepthinmeters', 
    'dwc:verbatimdepth', 'dwc:minimumdistanceabovesurfaceinmeters', 
    'dwc:maximumdistanceabovesurfaceinmeters', 'dwc:locationaccordingto', 
    'dwc:locationremarks', 'dwc:decimallatitude', 'dwc:decimallongitude', 
    'dwc:geodeticdatum', 'dwc:coordinateuncertaintyinmeters', 'dwc:coordinateprecision',
    'dwc:pointradiusspatialfit', 'dwc:verbatimcoordinates', 'dwc:verbatimlatitude', 
    'dwc:verbatimlongitude', 'dwc:verbatimcoordinatesystem', 'dwc:verbatimsrs',
    'dwc:footprintwkt', 'dwc:footprintsrs', 'dwc:footprintspatialfit', 
    'dwc:georeferencedby', 'dwc:georeferenceddate', 'dwc:georeferenceprotocol',
    'dwc:georeferencesources', 'dwc:georeferenceverificationstatus', 
    'dwc:georeferenceremarks'
    ]

# Terms to use to match locations for a georeference search on table 
# matchme_with_coords_best_georef
locationmatchwithcoordstermlist = [
    'continent', 'waterbody', 'islandgroup', 'island', 'countrycode', 'stateprovince', 
    'county', 'municipality', 'locality', 'verbatimlocality', 'minimumelevationinmeters',
    'maximumelevationinmeters', 'verbatimelevation', 'minimumdepthinmeters', 
    'maximumdepthinmeters', 'verbatimdepth', 'verbatimcoordinates', 'verbatimlatitude', 
    'verbatimlongitude', 'decimallatitude', 'decimallongitude'
    ]

# Terms to use to match locations for a georeference search on table 
# matchme_with_coords_best_georef where the input has interpreted_countrycode
gbiflocationmatchwithcoordstermlist = [
    'continent', 'waterbody', 'islandgroup', 'island', 'interpreted_countrycode', 
    'stateprovince', 'county', 'municipality', 'locality', 'verbatimlocality', 
    'minimumelevationinmeters', 'maximumelevationinmeters', 'verbatimelevation', 
    'minimumdepthinmeters', 'maximumdepthinmeters', 'verbatimdepth', 
    'verbatimcoordinates', 'verbatimlatitude', 'verbatimlongitude', 'decimallatitude', 
    'decimallongitude'
    ]

# Terms to use to match locations for a georeference search on table 
# matchme_verbatimcoords_best_georef
locationmatchverbatimcoordstermlist = [
    'continent', 'waterbody', 'islandgroup', 'island', 'countrycode', 'stateprovince', 
    'county', 'municipality', 'locality', 'verbatimlocality', 'minimumelevationinmeters',
    'maximumelevationinmeters', 'verbatimelevation', 'minimumdepthinmeters', 
    'maximumdepthinmeters', 'verbatimdepth', 'verbatimcoordinates', 'verbatimlatitude', 
    'verbatimlongitude'
    ]

# Terms to use to match locations for a georeference search on table 
# matchme_verbatimcoords_best_georef where the input has interpreted_countrycode
gbiflocationmatchverbatimcoordstermlist = [
    'continent', 'waterbody', 'islandgroup', 'island', 'interpreted_countrycode', 
    'stateprovince', 'county', 'municipality', 'locality', 'verbatimlocality', 
    'minimumelevationinmeters', 'maximumelevationinmeters', 'verbatimelevation', 
    'minimumdepthinmeters', 'maximumdepthinmeters', 'verbatimdepth', 
    'verbatimcoordinates', 'verbatimlatitude', 'verbatimlongitude'
    ]

# Terms to use to match locations for a georeference search on table 
# matchme_sans_coords_best_georef
locationmatchsanscoordstermlist = [
    'continent', 'waterbody', 'islandgroup', 'island', 'countrycode', 'stateprovince', 
    'county', 'municipality', 'locality', 'verbatimlocality', 'minimumelevationinmeters',
    'maximumelevationinmeters', 'verbatimelevation', 'minimumdepthinmeters', 
    'maximumdepthinmeters', 'verbatimdepth'
    ]

# Terms to use to match locations for a georeference search on table 
# matchme_sans_coords_best_georef where the input has interpreted_countrycode
gbiflocationmatchsanscoordstermlist = [
    'continent', 'waterbody', 'islandgroup', 'island', 'interpreted_countrycode', 
    'stateprovince', 'county', 'municipality', 'locality', 'verbatimlocality', 
    'minimumelevationinmeters', 'maximumelevationinmeters', 'verbatimelevation', 
    'minimumdepthinmeters', 'maximumdepthinmeters', 'verbatimdepth'
    ]

# The taxonkeytermlist contains the terms that make up a distinct taxon name combination
taxonkeytermlist = [
    'kingdom', 'genus', 'subgenus', 'specificEpithet', 'infraspecificEpithet', 
    'scientificNameAuthorship', 'scientificName']

# Terms that make up a distinct event date combination
eventkeytermlist = [
    'eventdate', 'verbatimEventDate','year','month','day']

# Terms that make up a distinct coordinates combination
coordinateskeytermlist = [
    'decimalLatitude', 'decimalLongitude', 'verbatimLatitude','verbatimLongitude',
    'verbatimCoordinates']

# Terms that are recommended comply with a controlled vocabulary
controlledtermlist = [
    'type', 'language', 'license', 'basisOfRecord', 'sex', 'lifeStage', 
    'reproductiveCondition', 'establishmentMeans', 'degreeOfEstablishment','pathway',
    'occurrenceStatus', 'preparations', 'disposition', 'organismScope', 'month', 'day', 
    'geodeticDatum', 'georeferenceVerificationStatus', 'identificationQualifier', 
    'identificationVerificationStatus', 'taxonRank', 'nomenclaturalCode', 
    'taxonomicStatus', 'nomenclaturalStatus']	

# Standard fields in the header of a vocabulary lookup file
vocabfieldlist = ['standard', 'vetted']

# Extra fields in the header of a geography vocabulary lookup file
geogvocabaddedfieldlist = ['error', 'misplaced', 'unresolved', 'source', 'comment']

# Extra fields in the header of a Darwin Cloud vocabulary lookup file
darwincloudvocabaddedfieldlist = ['namespace', 'source', 'comment']

# Dictionary defining the default values of a new vocabulary file entry
vocabrowdict = { 'standard':'', 'vetted':0 }
