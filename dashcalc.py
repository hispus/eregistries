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
import time
import math

####################################################################################################
#
# From a DHIS2 system, this script compares data from an organisation unit with data
# from other organisation units within a peer group. The peer groups are determined
# in one of three ways:
#
# 1. If only the "orgUnitLevel" is configured, the peer groups consist of all organisation
# units that are immediate children of a parent organisation unit, where the parent level
# is the "orgUnitLevel" configuration value.
#
# 2. If "peerLevel" is also configured, the peer groups consist of all organisation
# units at the peer level. They are grouped by the organisations at the "orgUnitLevel".
#
# 3. If there is an organisation unit group set called "Dashboard Groups",
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
# month most recently ended (and immediately-preceeding months if the configured
# "count" is greater than 1). Data is written into the following data elements,
# based on the data element UID, where XXXXXXX is from the indicator with
# UID dashXXXXXXX:
#
# deXXXXXXXAv - three month average for this organisation unit
# deXXXXXXXQ1 - 25th percentile average for all orgUnits in the peer group
# deXXXXXXXQ2 - 50th percentile average for all orgUnits in the peer group
# deXXXXXXXQ3 - 75th percentile average for all orgUnits in the peer group
# deXXXXXXXDR - percentile for this average compared with all orgUnits in the peer group
# deXXXXXXXsz - number of orgUnits in the peer group having a value
# deXXXXXXXor - "big rank" order (3, 2, 1) of this orgUnit in the peer group
# deXXXXXXXsr - "small rank" order (1, 2, 3) of this orgUnit in the peer group
# deXXXXXXXsd - standard deviation in the peer group
# deXXXXXXXDM - mean of all orgUnit values in the peer group
# deXXXXXXXd3 - three month sum of the indicator's denominator for this orgUnit
#
# Note that the deXXXXXXXQn, deXXXXXXXsz, and deXXXXXXXsd data elements will have the
# same values for all orgUnits in the peer group. This is done so that validation
# rules and analytics can have the peer group's data available with each orgUnit.
#
# These averages are also averaged among indicators belonging to the same area
# defined by indicator group within the 'dash_indicators' indicator group set
# (if the 'dash_indicators' indicator group set exists.)
#
# If "count" is configured, the script computes values for the last n completed
# months. If there is no "count", the script computes values only for the most
# recently-completed month.
#
# If "maxGetMonths" is configured, the script will limit each query for
# indicator data to no more than this number of months. This can be used to
# avoid gateway timeout errors. If there is no "maxGetMonths", the script
# will get all the months needed for each indicator in a single query.
# The value of "maxGetMonths" must be no greater than count + 2.
#
# The area average for each organisation unit is computed as the average of the
# averages in that area. This is stored in the data element named 'Overall Average: '
# follwed by the indicator group name for that area. The area average is compared
# with other organisation units in the peer group, and the rank (small is good)
# is stored in the data element whose name is 'Overall Rank: ' follwed by the
# indicator group name for that area.
#
# This script can be given an argument which is the configuration file to load.
# If not given, the default is /usr/local/etc/dashcalc.conf
#
# The configuration file is of the following form:
#
# {
#   "dhis": {
#     "baseurl": "http://localhost:8080",
#     "username": "admin",
#     "password": "district",
#     "orgUnitLevel": 3,
#     "peerLevel": 6,
#     "count": 6,
#     "maxGetMonths": 4
#   }
# }
#
# Notes:
# 		"peerLevel" is optional. If not specified, it is set to orgUnitLevel + 1.
# 		"count" is optional. If not specified, it is set to 1.
# 		"maxGetMonths" is optional. If not specified, it is set to count + 2.
#
# This script writes one log file per month to /usr/local/var/log/dashcalc/dashcalc-yyyy-mm.log
# (if the directory exists and it has write access). With each run, it appends one
# line to the monthly log file giving the run ending date and time, the base URL
# of the DHIS 2 system accessed, the time it took to execute the script in
# (hours:minutes:seconds), and the count of data values imported, updated and ignored.
# For example, in file /usr/local/var/log/dashcalc/dashcalc-2020-01.log:
#
# 2020-01-10 12:28:19.854 http://localhost:8080 0:02:11 imported: 0, updated: 10068, ignored: 0
#
# This script requires the following (you may need to replace pip with pip3):
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

try:
	configContents = open(configFile).read()
except Exception as e:
	print("Can't read configuration file:", e)
	sys.exit(1)

try:
	config = json.loads(configContents)
except Exception as e:
	print('Configuration file format error: in "' + configFile + '":', e)
	sys.exit(1)

dhis = config['dhis']
baseUrl = dhis['baseurl']
api = baseUrl + '/api/'
credentials = (dhis['username'], dhis['password'])
orgUnitLevel = dhis['orgUnitLevel']
peerLevel = dhis.get('peerLevel', orgUnitLevel + 1)
monthCount = dhis.get('count', 1)
maxGetMonths = dhis.get('maxGetMonths', monthCount + 2)

try:
	response = requests.get(api + 'me', auth=credentials)
	if response.status_code != 200:
		print('Error connecting to DHIS 2 system at "' + baseUrl + '" with username "' + dhis['username'] + '":', response)
		sys.exit(1)
except Exception as e:
	print('Cannot connect to DHIS 2 system at "' + baseUrl + '" with username "' + dhis['username'] + '":', e)
	sys.exit(1)

#
# Convert month string to a sequential number, e.g.:
# '201912' (December 2019) -> 24239
# '202001' (January 2020) -> 24240
#
def toNumber(month):
	return int(month[:4])*12 + int(month[4:])-1

#
# Convert sequential number to a month string
#
def toMonth(monthNumber):
	return str(monthNumber//12) + str(101+monthNumber%12)[1:]

#
# Find today and last month
#
today = datetime.date.today()
thisMonth = today.strftime('%Y%m')
thisMonthNumber = toNumber(thisMonth)
startOfCurrentMonth = today.replace(day=1)

#
# Handy functions for accessing dhis 2
#
def d2get(args, objects):
	retry = 0 # Sometimes gets a [502] error, waiting and retrying helps
	while True:
		# print(api + args) # debug
		response = requests.get(api + args.replace('[','%5B').replace(']','%5D'), auth=credentials)
		try:
			# print(api + args + ' --', len(response.json()[objects]))
			return response.json()[objects]
		except:
			retry = retry + 1
			if retry > 3:
				print( 'Tried GET', api + args, '\n' + 'Unexpected server response:', response.text )
				raise
			time.sleep(2) # Wait before retrying

def d2post(args, data):
	# print(api + args, len(json.dumps(data)), "bytes.")
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
# construct org unit peer groups at the peer level. The peer group
# identifier is the ancestor org unit UID
#
else:
	dataOrgUnitLevels.add(peerLevel)
	facilities = d2get('organisationUnits.json?filter=level:eq:' + str(peerLevel) + '&fields=id,path,closedDate&paging=false', 'organisationUnits')
	for facility in facilities:
		if 'closedDate' not in facility or facility['closedDate'] >= str(startOfCurrentMonth):
			ancestor = facility['path'][12*orgUnitLevel-11:12*orgUnitLevel]
			peerGroupMap[facility['id']] = ancestor
			# print('peerGroupMap:', facility['id'], ancestor, facility['path']) # debug

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
indicatorAreas = {}
if indicatorGroupSets:
	for indicatorGroup in indicatorGroupSets[0]['indicatorGroups']:
		for indicator in indicatorGroup['indicators']:
			indicatorAreas[indicator['id']] = indicatorGroup['name']

#
# Assemble the input indicator data into nested dictionaries:
# input [ peerGroup ] [ indicator ] [ orgUnit ] [ period ] { 'value', 'denominator' }
#
indicatorErrorCount = 0
input = {}
queryMonths = monthCount+2
allPeriods = [ toMonth(i) for i in range(thisMonthNumber-queryMonths, thisMonthNumber) ]
for i in indicators:
	if i['id'][0:4] == 'dash':
		for level in dataOrgUnitLevels:
			for loopCount in range( 0, math.ceil(float(queryMonths)/maxGetMonths) ):
				lastQueryMonth = (loopCount+1)*maxGetMonths if (loopCount+1)*maxGetMonths < queryMonths else queryMonths
				selectPeriods = ';'.join(allPeriods[loopCount*maxGetMonths:lastQueryMonth])
				try:
					rows = d2get('analytics.json?dimension=dx:' + i['id'] + '&dimension=ou:LEVEL-' + str(level) + '&dimension=pe:' + selectPeriods + '&skipMeta=true&includeNumDen=true', 'rows')
				except Exception as e:
					indicatorErrorCount = indicatorErrorCount + 1
					break # After one error on this indicator, move on to the next indicator.
				for r in rows:
					indicator = r[0]
					orgUnit = r[1]
					period = toNumber( r[2] )
					value = float( r[3] )
					denominator = float( r[5] )
					if orgUnit in peerGroupMap:
						peerGroup = peerGroupMap[orgUnit]
						if not peerGroup in input:
							input[peerGroup] = {}
						if not indicator in input[peerGroup]:
							input[peerGroup][indicator] = {}
						if not orgUnit in input[peerGroup][indicator]:
							input[peerGroup][indicator][orgUnit] = {}
						input[peerGroup][indicator][orgUnit][period] = { 'value': value, 'denominator': denominator }

# print('input', input) # debug

#
# Construct a list of data values to output.
#
def addAreaValue(areas, area, orgUnit, value):
	if not area in areas:
		areas[area] = {}
	orgUnits = areas[area]
	if not orgUnit in orgUnits:
		orgUnits[orgUnit] = []
	orgUnits[orgUnit].append( value )

#
# Initialze the output data and counts
#
output = { 'dataValues': [] }
totalImported = 0
totalUpdated = 0
totalIgnored = 0

#
# Periodically flush the output to avoid a POST that is too large
#
def flushOutput():
	global output
	global totalImported
	global totalUpdated
	global totalIgnored
	# print('POST: ',json.dumps(output)) # debug
	for retry in range(20): # Sometimes gets an error, waiting and retrying helps
		status = d2post( 'dataValueSets', output )
		success = ( str(status) == '<Response [200]>' or status.json()['importCount']['ignored'] != 0 ) # No error if data elements not found.
		if success:
			# print( 'POST success:', status.json() ) # debug
			counts = status.json()['importCount']
			totalImported = totalImported + counts['imported']
			totalUpdated = totalUpdated + counts['updated']
			totalIgnored = totalIgnored + counts['ignored']
			break
		else:
			time.sleep(10) # Wait before retrying
	if not success:
		print( 'Data post return status:', str(status), status.json() )
	output = { 'dataValues': [] }

#
# Output data to DHIS 2.
#
def putOut(orgUnit, month, dataElement, value):
	output['dataValues'].append( {
		'orgUnit': orgUnit,
		'period': month,
		'dataElement': dataElement,
		'value': str( value ),
		'categoryOptionCombo': defaultCoc,
		'attributeOptionCombo': defaultCoc
		} )
	if len(output['dataValues']) >= 4000:
		flushOutput()

def putOutByName(orgUnit, month, dataElementName, value):
	if dataElementName in elementNameId:
		putOut(orgUnit, month, elementNameId[dataElementName], value)
	# else: # debug
		# print("Warning: data element " + dataElementName + " not found.") # debug

def threeMonths(periods, monthNumber, valueType):
	data = []
	for m in [monthNumber - 2, monthNumber - 1, monthNumber]:
		if m in periods:
			data.append(periods[m][valueType])
	return data

for monthNumber in range(thisMonthNumber - monthCount, thisMonthNumber):
	month = toMonth(monthNumber)
	for peerGroup, indicators in input.items():
		areas = {} # { area: { orgUnit: [ average1, average2, ... ] } }
		for indicator, orgUnits in indicators.items():
			averages = []
			allPeersValues = []
			for orgUnit, periods in orgUnits.items():
				values = threeMonths(periods, monthNumber, 'value')
				allPeersValues.extend(values)
				# print('orgUnit:', orgUnit, 'periods:', periods, 'monthNumber:', monthNumber, 'values:', values)
				if len(values) == 0:
					continue # No indicator data for these 3 months for this orgUnit
				average = int( round( statistics.mean( values ) ) )
				averages.append( average )
				if indicator in indicatorAreas:
					area = indicatorAreas[indicator]
					addAreaValue( areas, area, orgUnit, average )
			count = len( averages )
			if count == 0:
				continue # No indicator data for these 3 months for this orgUnit peer group
			allPeersMean = int( round( statistics.mean( allPeersValues ) ) )
			averages.sort()
			q1 = int( round( averages [ int( (count-1) * .25 ) ] ) )
			q2 = int( round( averages [ int( (count-1) * .5 ) ] ) )
			q3 = int( round( averages [ int( (count-1) * .75 ) ] ) )
			stddev = int( round( numpy.std( averages ) ) ) or 0 # If only 1 sample, return stddev = 0
			# print( '\nmonth:', month, 'peerGroup:', peerGroup, 'indicator:', indicator, 'averages:', averages, 'q1-3:', q1, q2, q3, 'stddev:', stddev ) # debug
			uidBase = 'de' + indicator[4:]
			for orgUnit, periods in orgUnits.items():
				values = threeMonths(periods, monthNumber, 'value')
				if len(values) == 0:
					continue # No indicator data for these 3 months for this orgUnit
				mean = int( round( statistics.mean( values ) ) )
				bigRank = sum( [ a <= mean for a in averages ] ) # big is best
				percentile = int( round( 100 * float( bigRank ) / count ) )
				smallRank = sum( [ a > mean for a in averages ] ) + 1 # small is best
				denominatorSum = int( sum( threeMonths(periods, monthNumber, 'denominator') ) )
				putOut( orgUnit, month, uidBase + 'Av', mean )
				putOut( orgUnit, month, uidBase + 'Q1', q1 )
				putOut( orgUnit, month, uidBase + 'Q2', q2 )
				putOut( orgUnit, month, uidBase + 'Q3', q3 )
				putOut( orgUnit, month, uidBase + 'DR', percentile )
				putOut( orgUnit, month, uidBase + 'sz', count )
				putOut( orgUnit, month, uidBase + 'or', bigRank )
				putOut( orgUnit, month, uidBase + 'sr', smallRank )
				putOut( orgUnit, month, uidBase + 'sd', stddev )
				putOut( orgUnit, month, uidBase + 'DM', allPeersMean )
				putOut( orgUnit, month, uidBase + 'd3', denominatorSum )
				# print( 'Month:', month, 'peerGroup:', peerGroup, 'indicator:', indicator, 'orgUnit:', orgUnit, 'mean:', mean, 'smallRank:', smallRank, 'bigRank:', bigRank, 'percentile:', percentile, 'allPeersMean:', allPeersMean, 'denominatorSum:', denominatorSum, '3values:', threeMonths(periods, monthNumber, 'value'), '3denominators:', threeMonths(periods, monthNumber, 'denominator') ) # debug

		for area, orgUnitAverages in areas.items():
			areaAverages = []
			for orgUnit, averages in orgUnitAverages.items():
				average = int( round( statistics.mean( averages ) ) )
				areaAverages.append ( average )
			areaAverages.sort()
			count = len( areaAverages )
			# print( '\nMonth:', month, 'area:', area, 'areaAverages:', areaAverages ) # debug
			for orgUnit, averages in orgUnitAverages.items():
				mean = int( round( statistics.mean( averages ) ) )
				bigRank = sum( [ a <= mean for a in areaAverages ] )
				percentile = int( round( 100 * float( bigRank ) / count ) )
				putOutByName( orgUnit, month, 'Overall Average: ' + area, mean )
				putOutByName( orgUnit, month, 'Overall Rank: ' + area, percentile )

				# print( 'OrgUnit:', orgUnit, 'month:', month, 'overall average:', mean, 'overall rank:', percentile ) # debug

#
# Finish importing the output data into the DHIS 2 system.
#
flushOutput()

#
# Log the run in the monthly log file (if the log directory exists).
#
endTime = datetime.datetime.now()
logFile = '/usr/local/var/log/dashcalc/dashcalc-' + today.strftime('%Y-%m') + '.log'
logCounts = ' imported: ' + str(totalImported) + ', updated: ' + str(totalUpdated) + ', ignored: ' + str(totalIgnored) + ', indicator errors: ' + str(indicatorErrorCount)
logLine = str(endTime)[:23] + ' ' + baseUrl + ' ' + (str(endTime-startTime).split('.', 2)[0]) + logCounts  + '\n'
try:
	open(logFile, 'a+').write(logLine)
except:
	pass
