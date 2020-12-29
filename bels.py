# 1. Load file.cvs
# 2. Read header 
# 3. Pick Location terms 
# 4. Read terms in the Location terms header
# 5. Construct string
# 6. Hash string
# 7. Call Big Query
# 8. Return list of Location information


import csv 
import hashlib
import io

# List of DwC Location terms - maybe we could generate this automatically in case of addition or deleation of terms.
list_dwc_location_terms=["locationID", "higherGeographyID", "higherGeography", "continent", "waterBody", "islandGroup", "island", "country", "countryCode", "stateProvince", "county", "municipality", "locality", "verbatimLocality", "minimumElevationInMeters", "maximumElevationInMeters", "verbatimElevation", "minimumDepthInMeters", "maximumDepthInMeters", "verbatimDepth", "minimumDistanceAboveSurfaceInMeters", "maximumDistanceAboveSurfaceInMeters", "locationAccordingTo", "locationRemarks", "decimalLatitude", "decimalLongitude", "geodeticDatum", "coordinateUncertaintyInMeters", "coordinatePrecision", "pointRadiusSpatialFit", "verbatimCoordinates", "verbatimLatitude", "verbatimLongitude", "verbatimCoordinateSystem", "verbatimSRS", "footprintWKT", "footprintSRS", "footprintSpatialFit", "georeferencedBy", "georeferencedDate"
, "georeferenceProtocol", "georeferenceSources", "georeferenceVerificationStatus", "georeferenceRemarks"]

def read_from_csv(filename=None, content=None):\

	if filename:
		occurrences_file = open(filename, newline='')
	elif content:
		occurrences_file = io.StringIO(content)
	else:
		raise 'filename or content expected'

	file_reader = csv.reader(occurrences_file, delimiter=",", lineterminator="\n")
	header = next(file_reader)
	dict_location_terms = {}

	for items in header:
		# Verify terms // done by John 
		if(items in list_dwc_location_terms):
			dict_location_terms[items] = header.index(items)

	for line in file_reader:
		entry_csv_file = {}
		for terms, location in dict_location_terms.items():
			entry_csv_file[terms]=line[location]

		occurrence = Occurrence(**entry_csv_file)
		yield occurrence


class Occurrence(object):
	class Field(object):
		@classmethod
		def build(cls, data):
			if data:
				return cls(data)
			return Occurrence.EmptyField()

		def hash_data(self):
			raise NotImplemented('you have to import hash_data method')

	class EmptyField(Field):
		def hash_data(self):
			return b''

	class StringField(Field):
		def __init__(self, data):
			self.data = data

		def __repr__(self):
			return 'str:%s' % self.data

		def hash_data(self):
			return self.data.encode('utf-8')

	def __init__(self, **kwargs):
		self.highergeographyid = Occurrence.StringField.build(kwargs.get('higherGeographyID'))
		self.highergeography = Occurrence.StringField.build(kwargs.get('highergeography'))
		self.continent = Occurrence.StringField.build(kwargs.get('continent'))
		self.waterBody = Occurrence.StringField.build(kwargs.get('waterBody'))
		self.islandGroup = Occurrence.StringField.build(kwargs.get('islandGroup'))
		self.island = Occurrence.StringField.build(kwargs.get('island'))
		self.country = Occurrence.StringField.build(kwargs.get('country'))
		self.countryCode = Occurrence.StringField.build(kwargs.get('countryCode'))
		self.stateProvince = Occurrence.StringField.build(kwargs.get('stateProvince'))
		self.county = Occurrence.StringField.build(kwargs.get('county'))
		self.municipality = Occurrence.StringField.build(kwargs.get('municipality'))
		self.locality = Occurrence.StringField.build(kwargs.get('locality'))
		self.verbatimLocality = Occurrence.StringField.build(kwargs.get("verbatimLocality"))
		self.minimumElevationInMeters = Occurrence.StringField.build(kwargs.get("minimumElevationInMeters"))
		self.maximumElevationInMeters = Occurrence.StringField.build(kwargs.get("maximumElevationInMeters"))
		self.verbatimElevation = Occurrence.StringField.build(kwargs.get("verbatimElevation"))
		self.minimumDepthInMeters = Occurrence.StringField.build(kwargs.get("minimumDepthInMeters"))
		self.maximumDepthInMeters = Occurrence.StringField.build(kwargs.get("maximumDepthInMeters"))
		self.verbatimDepth = Occurrence.StringField.build(kwargs.get("verbatimDepth"))
		self.minimumDistanceAboveSurfaceInMeters = Occurrence.StringField.build(kwargs.get("minimumDistanceAboveSurfaceInMeters"))
		self.maximumDistanceAboveSurfaceInMeters = Occurrence.StringField.build(kwargs.get("maximumDistanceAboveSurfaceInMeters"))
		self.locationAccordingTo = Occurrence.StringField.build(kwargs.get("locationAccordingTo"))
		self.locationRemarks = Occurrence.StringField.build(kwargs.get("locationRemarks"))
		self.decimalLatitude = Occurrence.StringField.build(kwargs.get("decimalLatitude"))
		self.decimalLongitude = Occurrence.StringField.build(kwargs.get("decimalLongitude"))
		self.geodeticDatum = Occurrence.StringField.build(kwargs.get("geodeticDatum"))
		self.coordinateUncertaintyInMeters = Occurrence.StringField.build(kwargs.get("coordinateUncertaintyInMeters"))
		self.coordinatePrecision = Occurrence.StringField.build(kwargs.get("coordinatePrecision"))
		self.pointRadiusSpatialFit = Occurrence.StringField.build(kwargs.get("pointRadiusSpatialFit"))
		self.verbatimCoordinates = Occurrence.StringField.build(kwargs.get("verbatimCoordinates"))
		self.verbatimLatitude = Occurrence.StringField.build(kwargs.get("verbatimLatitude"))
		self.verbatimLongitude = Occurrence.StringField.build(kwargs.get("verbatimLongitude"))
		self.verbatimCoordinateSystem = Occurrence.StringField.build(kwargs.get("verbatimCoordinateSystem"))
		self.verbatimSRS = Occurrence.StringField.build(kwargs.get("verbatimSRS"))
		self.footprintWKT = Occurrence.StringField.build(kwargs.get("footprintWKT"))
		self.footprintSRS = Occurrence.StringField.build(kwargs.get("footprintSRS"))
		self.footprintSpatialFit = Occurrence.StringField.build(kwargs.get("footprintSpatialFit"))
		self.georeferencedBy = Occurrence.StringField.build(kwargs.get("georeferencedBy"))
		self.georeferencedDate = Occurrence.StringField.build(kwargs.get("georeferencedDate"))
		self.georeferenceProtocol = Occurrence.StringField.build(kwargs.get("georeferenceProtocol"))
		self.georeferenceSources = Occurrence.StringField.build(kwargs.get("georeferenceSources"))
		self.georeferenceVerificationStatus = Occurrence.StringField.build(kwargs.get("georeferenceVerificationStatus"))
		self.georeferenceRemarks = Occurrence.StringField.build(kwargs.get("georeferenceRemarks"))

	def hash(self):

		hasher = hashlib.new('sha256')

		hasher.update(b"dwc:highergeographyid")
		hasher.update(self.highergeographyid.hash_data())
		hasher.update(b"dwc:highergeography")
		hasher.update(self.highergeography.hash_data())
		hasher.update(b"dwc:continent")
		hasher.update(self.continent.hash_data())
		hasher.update(b"dwc:waterbody")
		hasher.update(self.waterBody.hash_data())
		hasher.update(b"dwc:islandgroup")
		hasher.update(self.islandGroup.hash_data())
		hasher.update(b"dwc:island")
		hasher.update(self.island.hash_data())
		hasher.update(b"dwc:country")
		hasher.update(self.country.hash_data())
		hasher.update(b"dwc:countryCode")
		hasher.update(self.countryCode.hash_data())
		hasher.update(b"dwc:stateprovince")
		hasher.update(self.stateProvince.hash_data())
		hasher.update(b"dwc:county")
		hasher.update(self.county.hash_data())
		hasher.update(b"dwc:municipality")
		hasher.update(self.municipality.hash_data())
		hasher.update(b"dwc:locality")
		hasher.update(self.locality.hash_data())
		hasher.update(b"dwc:verbatimlocality")
		hasher.update(self.verbatimLocality.hash_data())
		hasher.update(b"dwc:minimumelevationinmeters")
		hasher.update(self.minimumElevationInMeters.hash_data())
		hasher.update(b"dwc:maximumelevationinmeters")
		hasher.update(self.maximumElevationInMeters.hash_data())
		hasher.update(b"dwc:verbatimelevation")
		hasher.update(self.verbatimElevation.hash_data())
		hasher.update(b"dwc:minimumdepthinmeters")
		hasher.update(self.minimumDepthInMeters.hash_data())
		hasher.update(b"dwc:maximumdepthinmeters")
		hasher.update(self.maximumDepthInMeters.hash_data())
		hasher.update(b"dwc:verbatimdepth")
		hasher.update(self.verbatimDepth.hash_data())
		hasher.update(b"dwc:minimumdistanceabovesurfaceinmeters")
		hasher.update(self.minimumDistanceAboveSurfaceInMeters.hash_data())
		hasher.update(b"dwc:maximumdistanceabovesurfaceinmeters")
		hasher.update(self.maximumDistanceAboveSurfaceInMeters.hash_data())
		hasher.update(b"dwc:locationaccordingto")
		hasher.update(self.locationAccordingTo.hash_data())
		hasher.update(b"dwc:locationremarks")
		hasher.update(self.locationRemarks.hash_data())
		hasher.update(b"dwc:decimallatitude")
		hasher.update(self.decimalLatitude.hash_data())
		hasher.update(b"dwc:decimallongitude")
		hasher.update(self.decimalLongitude.hash_data())
		hasher.update(b"dwc:geodeticdatum")
		hasher.update(self.geodeticDatum.hash_data())
		hasher.update(b"dwc:coordinateuncertaintyinmeters")
		hasher.update(self.coordinateUncertaintyInMeters.hash_data())
		hasher.update(b"dwc:coordinateprecision")
		hasher.update(self.coordinatePrecision.hash_data())
		hasher.update(b"dwc:pointradiusspatialfit")
		hasher.update(self.pointRadiusSpatialFit.hash_data())
		hasher.update(b"dwc:verbatimcoordinates")
		hasher.update(self.verbatimCoordinates.hash_data())
		hasher.update(b"dwc:verbatimlatitude")
		hasher.update(self.verbatimLatitude.hash_data())
		hasher.update(b"dwc:verbatimlongitude")
		hasher.update(self.verbatimLongitude.hash_data())
		hasher.update(b"dwc:verbatimcoordinatesystem")
		hasher.update(self.verbatimCoordinateSystem.hash_data())
		hasher.update(b"dwc:verbatimsrs")
		hasher.update(self.verbatimSRS.hash_data())
		hasher.update(b"dwc:footprintwkt")
		hasher.update(self.footprintWKT.hash_data())
		hasher.update(b"dwc:footprintsrs")
		hasher.update(self.footprintSRS.hash_data())
		hasher.update(b"dwc:footprintspatialfit")
		hasher.update(self.footprintSpatialFit.hash_data())
		hasher.update(b"dwc:georeferencedby")
		hasher.update(self.georeferencedBy.hash_data())
		hasher.update(b"dwc:georeferenceddate")
		hasher.update(self.georeferencedDate.hash_data())
		hasher.update(b"dwc:georeferenceprotocol")
		hasher.update(self.georeferenceProtocol.hash_data())
		hasher.update(b"dwc:georeferencesources")
		hasher.update(self.georeferenceSources.hash_data())
		hasher.update(b"dwc:georeferenceverificationstatus")
		hasher.update(self.georeferenceVerificationStatus.hash_data())
		hasher.update(b"dwc:georeferenceremarks")
		hasher.update(self.georeferenceRemarks.hash_data())

		return hasher.hexdigest()

if __name__ == "__main__":
	import sys
	list_location_terms = read_from_csv(filename=sys.argv[1])
	for line in list_location_terms:
		dwc_location_hash = line.hash()
		print(dwc_location_hash)
		# send request to Big Query to see if ID exists

# python3 bels.py test.csv (or the name of your test file)
