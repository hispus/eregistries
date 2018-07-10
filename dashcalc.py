#!/usr/bin/env python3

import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import urllib.parse
import sys
import statistics
import numpy
import traceback

####################################################################################################
#
# From a DHIS 2 system, finds all the values in the past 3 months at a specified
# organisation unit level for indicators whose UID starts with 'dash'.
#
# For each such indicator, finds the average value for the past 3 months at
# in each organisation unit at the specified level, and compares this average
# value with the average values of other organisation units at the same level
# having the same parent.
#
# Based on this comparsion, writes aggregate monthly data values to the system
# for each organisation unit that has such an an average (has any data for the
# dash... indicator in the last 3 months). The data values are written for the
# month most recently ended. Data is written into the following data elements,
# based on the data element UID, where XXXXXXX is from the indicator with
# UID dashXXXXXXX:
#
# deXXXXXXXAv - Three month average for this organisation unit
# deXXXXXXXQ1 - 25th percentile average for all orgUnits with this parent
# deXXXXXXXQ2 - 50th percentile average for all orgUnits with this parent
# deXXXXXXXQ3 - 75th percentile average for all orgUnits with this parent
# deXXXXXXXDR - percentile for this average compared with all orgUnits with same parent
# deXXXXXXXsz - number of orgUnits with this parent having a value
# deXXXXXXXor - order (1, 2, 3) of this orgUnit among siblings
# deXXXXXXXsd - standard deviation within the same parent
#
# Note that the deXXXXXXXQn, deXXXXXXXsz, and deXXXXXXXsd data elements will have the
# same values for all orgUnits with the same parent. This is done so that validation
# rules and analytics can have the parent's data available with each orgUnit.
#
# These averages are also averaged among indicators belonging to the same
# indicator group within the 'dash_indicators' indicator group set if it exists.
# The rank for each OrgUnit is determined by comparing the average average for
# all indicators within the group against the average average for all orgUnits
# with the same parent. The rank is stored in the data element whose name is
# 'Overall: ' follwed by the indicator group name.
#
# This script can be given an argument which is the configuration file to load.
# If not given, the default is /var/local/etc/dashcalc.conf
#
# The configuration file is of the following form:
#
# {
#   "dhis": {
#     "baseurl": "http://localhost:8080",
#     "username": "admin",
#     "password": "district"
#     "orgUnitLevel": 4
#   }
# }
#
# This script requires the following:
#
#    pip install requests
#    pip install python-dateutil
#    pip install numpy
#
####################################################################################################

#
# load the configuration
#
if len(sys.argv) < 2:
	configFile = '/usr/local/etc/dashcalc.conf'
else:
    configFile = sys.argv[1]

config = json.loads(open(configFile).read())

dhis = config['dhis']
api = dhis['baseurl'] + '/api/'
credentials = (dhis['username'], dhis['password'])
orgUnitLevel = str(dhis['orgUnitLevel'])

#
# Get the names of the three monthly periods for data to collect
#
today = datetime.date.today()
p1 = (today+relativedelta(months=-3)).strftime('%Y%m')
p2 = (today+relativedelta(months=-2)).strftime('%Y%m')
p3 = (today+relativedelta(months=-1)).strftime('%Y%m')

#
# Handy functions for accessing dhis 2
#
def d2get(args, objects):
	# print(api + args) # debug
	response = requests.get(api + args, auth=credentials)
	try:
		return response.json()[objects]
	except:
		print( 'Tried: GET ', api + args, '\n', 'Unexpected server response: ', response.json() )
		traceback.print_stack()
		sys.exit(1)

def d2post(args, data):
	# print(api + args, json.dumps(data))
	return requests.post(api + args, json=data, auth=credentials)

#
# Get a list of the facilities we will need with parents,
# and create a map from facilities to parents.
#	
facilities = d2get('organisationUnits.json?filter=level:eq:' + orgUnitLevel + '&fields=id,parent&paging=false', 'organisationUnits')
parentMap = {}
for f in facilities:
	parentMap[f['id']] = f['parent']['id']

#
# Get a list of all indicators.
#
indicators = d2get('indicators.json?fields=id&paging=false', 'indicators')

#
# Get a list of all data elements.
#
dataElements = d2get('dataElements.json?fields=id,name&paging=false', 'dataElements')

#
# Get the default categoryOptionCombo (which is also the default attributeOptionCombo)
#
defaultCoc = d2get('categoryOptionCombos.json?filter=name:eq:default', 'categoryOptionCombos')[0]['id']

#
# Make a dictionary from data element name to ID
#
elementNameId = {}
for element in dataElements:
	elementNameId[element['name']] = element['id']

#
# For all indicators that are grouped, remember the group to which the indicator belongs
#
indicatorGroupSets = d2get('indicatorGroupSets.json?filter=name:eq:dash_indicators&fields=indicatorGroups[name,indicators[id]]&paging=false', 'indicatorGroupSets')
groupedIndicators = {};
if indicatorGroupSets:
	for indicatorGroup in indicatorGroupSets[0]['indicatorGroups']:
		for indicator in indicatorGroup['indicators']:
			groupedIndicators[indicator['id']] = indicatorGroup['name']

#
# Collect the input indicator data
# into nested dictionaries: parent . indicator . orgUnit . value array
#
input = {}
for i in indicators:
	if i['id'][0:4] == 'dash':
#	if i['id'][0:4] == 'dash' and i['id'] != 'dashDia0005' and i['id'][0:7] != 'dashHyp': # Temporary workaround
		rows = d2get('analytics.json?dimension=dx:' + i['id'] + '&dimension=ou:GD7TowwI46c;LEVEL-' + orgUnitLevel + '&dimension=pe:' + p1 + ';' + p2 + ';' + p3 + '&skipMeta=true', 'rows')
		for r in rows:
			indicator = r[0]
			orgUnit = r[1]
			period = r[2]
			value = float( r[3] )
			parent = parentMap[orgUnit]
			if not parent in input:
				input[parent] = {}
			if not indicator in input[parent]:
				input[parent][indicator] = {}
			if not orgUnit in input[parent][indicator]:
				input[parent][indicator][orgUnit] = []
			input[parent][indicator][orgUnit].append(value)

#
# Construct a list of data values to output.
#
def addGroup(dict1, key1, key2, value):
	if not key1 in dict1:
		dict1[key1] = {}
	dict2 = dict1[key1]
	if not key2 in dict2:
		dict2[key2] = []
	dict2[key2].append( value );

output = { 'dataValues': [] }

def putOut(orgUnit, dataElement, value):
	output['dataValues'].append( {
		'attributeOptionCombo': defaultCoc,
		'categoryOptionCombo': defaultCoc,
		'dataElement': dataElement,
		'orgUnit': orgUnit,
		'period': p3,
		'value': str( value )
		} )

for parent, indicators in input.items():
	groups = {} # { group: { orgUnit: [ average1, average2, ... ] } }

	for indicator, orgUnits in indicators.items():
		averages = []
		for orgUnit, values in orgUnits.items():
			average = int( round( statistics.mean( values ) ) )
			averages.append( average )
			if indicator in groupedIndicators:
				groupName = groupedIndicators[indicator]
				addGroup( groups, groupName, orgUnit, average )
		averages.sort()
		count = len( averages )
		q1 = int( round( averages [ int( (count-1) * .25 ) ] ) )
		q2 = int( round( averages [ int( (count-1) * .5 ) ] ) )
		q3 = int( round( averages [ int( (count-1) * .75 ) ] ) )
		stddev = int( round( numpy.std( averages ) ) )
		# print( '\nParent:', parent, 'indicator:', indicator, 'averages:', averages, 'q1-3:', q1, q2, q3, 'stddev:', stddev ) # debug
		uidBase = 'de' + indicator[4:]
		for orgUnit, values in orgUnits.items():
			mean = int( round( statistics.mean( values ) ) )
			bigRank = float( sum( [ a <= mean for a in averages ] ) ) # big is best
			percentile = int( round( 100 * bigRank / count ) )
			smallRank = sum( [ a > mean for a in averages ] ) + 1 # small is best
			putOut( orgUnit, uidBase + 'Av', mean )
			if q1: putOut( orgUnit, uidBase + 'Q1', q1 )
			if q2: putOut( orgUnit, uidBase + 'Q2', q2 )
			if q3: putOut( orgUnit, uidBase + 'Q3', q3 )
			putOut( orgUnit, uidBase + 'DR', percentile )
			putOut( orgUnit, uidBase + 'sz', count )
			putOut( orgUnit, uidBase + 'or', smallRank )
			putOut( orgUnit, uidBase + 'sd', stddev )
			# print( 'OrgUnit:', orgUnit, 'mean:', mean, 'rank:', smallRank, 'percentile:', percentile ) # debug

	for group, orgUnitAverages in groups.items():
		groupAverages = []
		for orgUnit, averages in orgUnitAverages.items():
			average = int( round( statistics.mean( averages ) ) )
			groupAverages.append ( average )
		groupAverages.sort()
		count = len( groupAverages )
		# print( '\nGroup:', group, 'groupAverages:', groupAverages ) # debug
		for orgUnit, averages in orgUnitAverages.items():
			mean = int( round( statistics.mean( averages ) ) )
			smallRank = sum( [ a > mean for a in groupAverages ] ) + 1 # small is best
			putOut( orgUnit, elementNameId['Overall: ' + group], smallRank )
			# print( 'OrgUnit:', orgUnit, 'mean:', mean, 'rank:', smallRank ) # debug

#
# Import the output data into the DHIS 2 system.
#
status = d2post( 'dataValueSets', output )
if str(status) != '<Response [200]>':
	print( 'Data post return status:', str(status), status.json() )