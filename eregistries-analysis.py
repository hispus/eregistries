#!/usr/bin/env python3

import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import urllib.parse
import sys
import statistics

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
#
# Note that the three deXXXXXXXQn data elements will have the same values for
# all orgUnits with the same parent. This is done so that validation rules can
# be used to compare an orgUnit's deXXXXXXXAv value with each of the deXXXXXXXQn
# values.
#
# This script can be given an argument which is the configuration file to load.
# If not given, the default is /var/local/etc/eregistries-analysis.conf
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
####################################################################################################

#
# load the configuration
#
if len(sys.argv) < 2:
	configFile = '/usr/local/etc/eregistries-analysis.conf'
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
	# print(api + args)
	response = requests.get(api + args, auth=credentials)
	try:
		return response.json()[objects]
	except:
		print( 'Tried: GET ', api + args, '\n', 'Unexpected server response: ', response)
		exit

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
# Get the default categoryOptionCombo (which is also the default attributeOptionCombo)
#
defaultCoc = d2get('categoryOptionCombos.json?filter=name:eq:default', 'categoryOptionCombos')[0]['id']

#
# Collect the input indicator data
# into nested dictionaries: parent . indicator . orgUnit . value array
#
input = {}
for i in indicators:
	if i['id'][0:4] == 'dash':
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
	for indicator, orgUnits in indicators.items():
		uidBase = 'de' + indicator[4:]
		averages = []
		for orgUnit, values in orgUnits.items():
			averages.append( int( round( statistics.mean( values ) ) ) )
		averages.sort()
		count = len( averages )
		q1 = averages [ int( (count-1) * .25 ) ]
		q2 = averages [ int( (count-1) * .5 ) ]
		q3 = averages [ int( (count-1) * .75 ) ]
		# print( '\nParent:', orgUnit, 'averages:', averages, 'q1-3:', q1, q2, q3 )
		for orgUnit, values in orgUnits.items():
			mean = int( round( statistics.mean( values ) ) )
			rank = float( sum( [ a <= mean for a in averages ] ) )
			percentile = int( round( 100 * rank / count ) )
			putOut( orgUnit, uidBase + 'Av', mean )
			putOut( orgUnit, uidBase + 'Q1', q1 )
			putOut( orgUnit, uidBase + 'Q2', q2 )
			putOut( orgUnit, uidBase + 'Q3', q3 )
			putOut( orgUnit, uidBase + 'DR', percentile )
			# print( 'OrgUnit:', orgUnit, 'mean:', mean, 'rank:', int(rank), 'percentile:', percentile )

#
# Import the output data into the DHIS 2 system.
#
status = d2post( 'dataValueSets', output )
if status != '<Response [200]>':
	print( 'Data post return status:', status )
