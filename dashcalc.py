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
# From a DHIS2 system, this script compares data from an organisation unit with data
# from other organisation units within a peer group. In the simple case, the peer
# groups consist of all organisation units that are immediate children of a parent
# organisation unit, where the parent level is the "orgUnitLevel" configuration value.
#
# However if there is an organisation unit group set called "Dashboard Groups",
# then the peer groups are formed as follows: Peer groups are created for each
# oranisation unit group within "Dashboard Groups". Each peer group consists of
# the organisation units having the same ancestor at the configured organisation
# unit level "orgUnitLevel". However for organisation units that are already at
# or above the configured "orgUnitLevel", the peer groups for these organisation
# units will be formed by those organisaiton units having the same parent.
#
# The data compared within each peer group are the values in the past 3 months
# for indicators whose UID starts with 'dash'.
#
# For each such indicator, finds the average value for the past 3 months at
# in each organisation unit in a peer goup, and compares this average value
# with the average values of other organisation units in the same peer group.
#
# Based on this comparsion, writes aggregate monthly data values to the system
# for each organisation unit that has such an an average (has any data for the
# dash... indicator in the last 3 months). The data values are written for the
# month most recently ended. Data is written into the following data elements,
# based on the data element UID, where XXXXXXX is from the indicator with
# UID dashXXXXXXX:
#
# deXXXXXXXAv - Three month average for this organisation unit
# deXXXXXXXQ1 - 25th percentile average for all orgUnits in the peer group
# deXXXXXXXQ2 - 50th percentile average for all orgUnits in the peer group
# deXXXXXXXQ3 - 75th percentile average for all orgUnits in the peer group
# deXXXXXXXDR - percentile for this average compared with all orgUnits in the peer group
# deXXXXXXXsz - number of orgUnits in the peer group having a value
# deXXXXXXXor - order (1, 2, 3) of this orgUnit in the peer group
# deXXXXXXXsd - standard deviation in the peer group
#
# Note that the deXXXXXXXQn, deXXXXXXXsz, and deXXXXXXXsd data elements will have the
# same values for all orgUnits in the peer group. This is done so that validation
# rules and analytics can have the peer group's data available with each orgUnit.
#
# These averages are also averaged among indicators belonging to the same area
# defined by indicator group within the 'dash_indicators' indicator group set
# (if the 'dash_indicators' indicator group set exists.)
#
# The area average for each organisation unit is computed as the average of the
# averages in that area. This is stored in the data element named 'Overall Average: '
# follwed by the indicator group name for that area. The area average is compared
# with other organisation units in the peer group, and the rank (small is good)
# is stored in the data element whose name is 'Overall Rank: ' follwed by the
# indicator group name for that area.
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
#     "password": "district",
#     "orgUnitLevel": 3
#   }
# }
#
# This script writes one log file per month to /usr/local/var/log/dashcalc/dashcalc-yyyy-mm.log
# (if the directory exists and it has write access). With each run, it appends one
# line to the monthly log file giving the run ending date and time, the base URL
# of the DHIS 2 system accessed, the time it took to execute the script in
# (hours:minutes:seconds), and the count of data values imported, updated and ignored.
# For example:
#
# 2018-07-10 12:28:19.854 http://localhost:8080 0:02:11 imported: 0, updated: 10068, ignored: 0
#
# This script requires the following:
#
#    pip install requests
#    pip install python-dateutil
#    pip install numpy
#
####################################################################################################

startTime = datetime.datetime.now()

#
# load the configuration
#
if len(sys.argv) < 2:
	configFile = '/usr/local/etc/dashcalc.conf'
else:
    configFile = sys.argv[1]

config = json.loads(open(configFile).read())

dhis = config['dhis']
baseUrl = dhis['baseurl']
api = baseUrl + '/api/'
credentials = (dhis['username'], dhis['password'])
orgUnitLevel = dhis['orgUnitLevel']

#
# Get the names of the three monthly periods for data to collect
#
today = datetime.date.today()
p1 = (today+relativedelta(months=-3)).strftime('%Y%m')
p2 = (today+relativedelta(months=-2)).strftime('%Y%m')
p3 = (today+relativedelta(months=-1)).strftime('%Y%m')

startOfCurrentMonth = today.replace(day=1)

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
# If the org unit group set 'Dashboard groups' exists, then
# form the organisation unit peer groups accordingly.
#
# The peer group identifier is the common ancestor org unit
# UID follwed by the org unit group name.
#
# Also, remember the various org unit levels at which we will need to collect data.
#
peerGroupMap = {}
dataOrgUnitLevels = set()
groupSets = d2get('organisationUnitGroupSets.json?filter=name:eq:Dashboard+groups&fields=organisationUnitGroups[name,organisationUnits[id,level,path,closedDate]]', 'organisationUnitGroupSets')
if groupSets:
	for ouGroup in groupSets[0]['organisationUnitGroups']:
		# print("ouGroup", ouGroup)
		for facility in ouGroup['organisationUnits']:
			if 'closedDate' not in facility or facility['closedDate'] >= str(startOfCurrentMonth):
				if facility['level'] > orgUnitLevel:
					ancestor = facility['path'][12*orgUnitLevel-11:12*orgUnitLevel]
				elif facility['level'] > 1:
					ancestor = facility['path'][-23:-12]
				else:
					continue # Path too short to have a parent - ignore
				peerGroupMap[facility['id']] = ancestor + '-' + ouGroup['name']
				dataOrgUnitLevels.add(facility['level'])
				# print('peerGroupMap:', facility['id'], facility['path'], ancestor + '-' + ouGroup['name']) # debug

#
# If the org unit group set 'Dashboard groups' does not exist, then
# construct org unit peer groups as the children of the facilities
# at the configured orgUnitLevel. The peer group identifier is
# the parent org unit UID
#
else:
	dataOrgUnitLevels.add(orgUnitLevel+1)
	facilities = d2get('organisationUnits.json?filter=level:eq:' + str(orgUnitLevel+1) + '&fields=id,parent,closedDate&paging=false', 'organisationUnits')
	for facility in facilities:
		if 'closedDate' not in facility or facility['closedDate'] >= str(startOfCurrentMonth):
			peerGroupMap[facility['id']] = facility['parent']['id']
			# print('peerGroupMap:', facility['id'], facility['parent']['id']) # debug

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
# For all indicators that are grouped into areas, remember the area for each indicator
#
indicatorGroupSets = d2get('indicatorGroupSets.json?filter=name:eq:dash_indicators&fields=indicatorGroups[name,indicators[id]]&paging=false', 'indicatorGroupSets')
indicatorAreas = {};
if indicatorGroupSets:
	for indicatorGroup in indicatorGroupSets[0]['indicatorGroups']:
		for indicator in indicatorGroup['indicators']:
			indicatorAreas[indicator['id']] = indicatorGroup['name']

#
# Collect the input indicator data
# into nested dictionaries: peerGroup . indicator . orgUnit . value array
#
input = {}
for i in indicators:
	if i['id'][0:4] == 'dash':
		for level in dataOrgUnitLevels:
			rows = d2get('analytics.json?dimension=dx:' + i['id'] + '&dimension=ou:LEVEL-' + str(level) + '&dimension=pe:' + p1 + ';' + p2 + ';' + p3 + '&skipMeta=true', 'rows')
			for r in rows:
				indicator = r[0]
				orgUnit = r[1]
				period = r[2]
				value = float( r[3] )
				if orgUnit in peerGroupMap:
					peerGroup = peerGroupMap[orgUnit]
					if not peerGroup in input:
						input[peerGroup] = {}
					if not indicator in input[peerGroup]:
						input[peerGroup][indicator] = {}
					if not orgUnit in input[peerGroup][indicator]:
						input[peerGroup][indicator][orgUnit] = []
					input[peerGroup][indicator][orgUnit].append(value)

#
# Construct a list of data values to output.
#
def addAreaValue(areas, area, orgUnit, value):
	if not area in areas:
		areas[area] = {}
	orgUnits = areas[area]
	if not orgUnit in orgUnits:
		orgUnits[orgUnit] = []
	orgUnits[orgUnit].append( value );

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

for peerGroup, indicators in input.items():
	areas = {} # { area: { orgUnit: [ average1, average2, ... ] } }

	for indicator, orgUnits in indicators.items():
		averages = []
		for orgUnit, values in orgUnits.items():
			average = int( round( statistics.mean( values ) ) )
			averages.append( average )
			if indicator in indicatorAreas:
				area = indicatorAreas[indicator]
				addAreaValue( areas, area, orgUnit, average )
		averages.sort()
		count = len( averages )
		q1 = int( round( averages [ int( (count-1) * .25 ) ] ) )
		q2 = int( round( averages [ int( (count-1) * .5 ) ] ) )
		q3 = int( round( averages [ int( (count-1) * .75 ) ] ) )
		stddev = int( round( numpy.std( averages ) ) )
		# print( '\nPeerGroup:', peerGroup, 'indicator:', indicator, 'averages:', averages, 'q1-3:', q1, q2, q3, 'stddev:', stddev ) # debug
		uidBase = 'de' + indicator[4:]
		for orgUnit, values in orgUnits.items():
			mean = int( round( statistics.mean( values ) ) )
			bigRank = float( sum( [ a <= mean for a in averages ] ) ) # big is best
			percentile = int( round( 100 * bigRank / count ) )
			smallRank = sum( [ a > mean for a in averages ] ) + 1 # small is best
			putOut( orgUnit, uidBase + 'Av', mean )
			putOut( orgUnit, uidBase + 'Q1', q1 )
			putOut( orgUnit, uidBase + 'Q2', q2 )
			putOut( orgUnit, uidBase + 'Q3', q3 )
			putOut( orgUnit, uidBase + 'DR', percentile )
			putOut( orgUnit, uidBase + 'sz', count )
			putOut( orgUnit, uidBase + 'or', smallRank )
			putOut( orgUnit, uidBase + 'sd', stddev )
			# print( 'OrgUnit:', orgUnit, 'mean:', mean, 'rank:', smallRank, 'percentile:', percentile ) # debug

	for area, orgUnitAverages in areas.items():
		areaAverages = []
		for orgUnit, averages in orgUnitAverages.items():
			average = int( round( statistics.mean( averages ) ) )
			areaAverages.append ( average )
		areaAverages.sort()
		count = len( areaAverages )
		# print( '\nArea:', area, 'areaAverages:', areaAverages ) # debug
		for orgUnit, averages in orgUnitAverages.items():
			mean = int( round( statistics.mean( averages ) ) )
			smallRank = sum( [ a > mean for a in areaAverages ] ) + 1 # small is best
			putOut( orgUnit, elementNameId['Overall Average: ' + area], mean )
			putOut( orgUnit, elementNameId['Overall Rank: ' + area], smallRank )
			# print( 'OrgUnit:', orgUnit, 'overall average:', mean, 'overall rank:', smallRank ) # debug

#
# Import the output data into the DHIS 2 system.
#
status = d2post( 'dataValueSets', output )
if str(status) != '<Response [200]>' or status.json()['importCount']['ignored'] != 0:
	print( 'Data post return status:', str(status), status.json() )

#
# Log the run in the monthly log file (if the log directory exists).
#
endTime = datetime.datetime.now()
logFile = '/usr/local/var/log/dashcalc/dashcalc-' + today.strftime('%Y-%m') + '.log'
counts = status.json()['importCount']
logCounts = ' imported: ' + str(counts['imported']) + ', updated: ' + str(counts['updated']) + ', ignored: ' + str(counts['ignored'])
logLine = str(endTime)[:23] + ' ' + baseUrl + ' ' + (str(endTime-startTime).split('.', 2)[0]) + logCounts  + '\n'
try:
	open(logFile, 'a+').write(logLine)
except:
	pass
