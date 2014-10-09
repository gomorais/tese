from subprocess import Popen, PIPE
import multiprocessing
import os
#import radix
from netaddr import *
from datetime import datetime
import scriptUtils
import copy
import math

prefixOrigin = {}
allocDict = {}
dateAlloc = {}
prefixComp = {}
prefix_to_24_Map = {}
prefix_to_24_MapCopy = {}
twentyfour_to_prefix_Map = {}
prefixSeen = {}
notRouted = set()

exhaustionDate = datetime.strptime('20110301', '%Y%m%d')
firstAllocDate = datetime.strptime('20011101', '%Y%m%d')
startAllocDate = datetime.strptime('20010101', '%Y%m%d')

def readFile(fil, prefixOrigin):
	inputfile = Popen(['sh', '-c', 'cat '+fil],stdout = PIPE, bufsize=10485760)
    	for line in inputfile.stdout:
		linearray = line.split(",")
		prefixOrigin[linearray[0]] = linearray[1]

	inputfile.stdout.close()
    	inputfile.terminate()
	return prefixOrigin

def allocationCheck(allocRtree, fileRtree, newRtree, dateRtree, prefix_to_24_Map, prefixSeen, date):
	filedate = datetime.strptime(date, '%Y%m%d')
	for key in allocRtree.keys():
		if key in fileRtree:
			allocDate = datetime.strptime(allocRtree[key], '%Y%m%d')
			if allocDate <= filedate:
				if not str(filedate.year) in prefixSeen:
					prefixSet = set()
					prefixSet.add(key)
					prefixSeen[str(filedate.year)] = prefixSet
				if str(filedate.year) in prefixSeen:
					prefixSeen[str(filedate.year)].add(key)
			if not key in newRtree:
				if allocDate <= filedate:
					timedelta = (filedate - allocDate).days
					newRtree[key] = (allocDate.date(), filedate.date(), timedelta)
					#allocRtree.pop(key)
					#network = twentyfour_to_prefix_Map[key] 
					#notRouted.discard(network)
					#prefix_to_24_Map[network].discard(key)
					if not str(allocDate.year) in dateRtree:
						prefixDays = {}
						prefixDays[key] = timedelta
						dateRtree[str(allocDate.year)] = prefixDays
					if str(allocDate.year) in dateRtree:
						dateRtree[str(allocDate.year)][key] = timedelta
					


def comparePrefixes(rtree, newRtree, difRtree, changeRtree):
	for key in rtree.keys():
		if not key in newRtree:
			difRtree[key] = rtree[key]
		if key in newRtree:
			if rtree[key] != newRtree[key]:
				changeRtree[key] = rtree[key]
	for key in newRtree.keys():
		if not key in rtree:
			difRtree[key] = newRtree[key]

def prefixToOrigin(rtree, prefix, origin, repeated):
    	''' Function that adds a new found prefix to a peer '''
    	ip = IPNetwork(prefix)
    	#subnets = list(ip.subnet(24))
	if prefix not in rtree:
		rtree[prefix] = origin
		return rtree 
    	if prefix in rtree and rtree[prefix] != origin:
		if prefix not in repeated:
			originList = set()
			originList.add(origin)
			repeated[prefix] = originList	
    			return repeated
		if prefix in repeated:
			repeated[prefix].add(origin)

def Task(fil):

	prefixOrigin = {}
	repeated = {} # store prefixes that were already seen but with diferent origin
	
    	print("Task(%s) processid = %s" % (fil, os.getpid()))
    	inputfile = Popen(['sh', '-c', 'zcat '+fil],stdout = PIPE, bufsize=10485760)
    	for line in inputfile.stdout:
		linearray = line.split("|")
		ases = linearray[6].split(" ")
		if ases[-1][0] != "{" and (ases[0] in peers):
			prefixToOrigin(prefixOrigin, linearray[5], ases[-1], repeated)
			#peer_orig[ases[0]].add(linearray[5])

    	inputfile.stdout.close()
    	inputfile.terminate()

	filename = os.path.split(fil)[1]
    	outputfile_peer = open(os.path.join("./results",os.path.split(fil)[1])+"_prefixMapping.csv", 'w')
    	'''creates a file with the prefix, origin and that file date'''
    	filedate = filename[4:8] +"-"+ filename[8:10] +"-"+filename[10:12]
    	for prefix in prefixOrigin:
		outputfile_peer.write(prefix+ "," + prefixOrigin[prefix] + "," + filedate + "\n" )
    	outputfile_peer.close()

	outputfile_repeated = open(os.path.join("./results",os.path.split(fil)[1])+"_prefixMappingRepeated.csv", 'w')
    	'''creates a file with the repeated seen prefixes but with diferent origin, the origins and file date'''
    	filedate = filename[4:8] +"-"+ filename[8:10] +"-"+filename[10:12]
    	for prefix in repeated:
		ases = repeated[prefix]
		outputfile_repeated.write(prefix+ "," + str(ases) + "," + filedate + "\n" )
    	outputfile_repeated.close()

	print repeated
	print len(prefixOrigin)
	print len(repeated)

if __name__ == "__main__":
	
	fullRouted = 0
	partRouted = 0
	directory = "./allocFiles"
	allocFiles = []
	allocFiles = scriptUtils.readIRR(allocFiles, directory)
	for f in allocFiles:
		scriptUtils.getAllocatedPrefix24(f, allocDict, prefix_to_24_Map, twentyfour_to_prefix_Map)

  	files = []
	for root, dirs, filenames in os.walk("./results/prefixMap24New_newPeer"):
		for name in filenames:
			files.append(os.path.join(root, name))
	files.sort()
	for key in prefix_to_24_Map.keys():
		notRouted.add(key)
		
	prefix_to_24_MapCopy = copy.deepcopy(prefix_to_24_Map)
	print len(notRouted)

  	for fil in files:
		print ("reading file: %s" % fil) 
		filename = os.path.split(fil)[1]
		filedate = filename[4:12]
		prefixOrigin = {}
		readFile(fil, prefixOrigin)
		allocationCheck(allocDict, prefixOrigin, prefixComp, dateAlloc, prefix_to_24_Map, prefixSeen, filedate)
			
	'''for key in dateAlloc:
		outputfile = open("./results/compareOriginAllocFiles_"+str(key)+"_24_new.csv", 'w')
		for prefix in dateAlloc[key]:
			outputfile.write(str(prefix) + "," + str(dateAlloc[key][prefix]) + "\n")
		outputfile.close()'''


	outputfile = open("./results/compareOriginAllocFiles_Global_perYear_24_new.csv", 'w')
	
	for key in dateAlloc:
		s = [value for value in dateAlloc[key].values()]
		avg = float(sum(s)/len(dateAlloc[key]))
		varianceLst = [(value-avg)**2 for value in s]
		variance = float(sum(varianceLst)/len(varianceLst))
		stdv = math.sqrt(variance)
		data = str(key) + "-01-01"
		outputfile.write(data + "," + str(avg) + "," + str(variance) + "," + str(stdv) + "\n")
		
	outputfile.close()
	#print prefix_to_24_MapCopy == prefix_to_24_Map

	'''outputfile = open("./results/compareOriginAllocFiles_RoutedStatus_24.csv", 'w')
	for date in prefixSeen:
		prefix_to_24_Map = copy.deepcopy(prefix_to_24_MapCopy)
		notRoutedCopy = copy.deepcopy(notRouted)
		fullRouted = 0
		partRouted = 0
		routingDate = str(date) + "-01-01"
		for prefix in prefixSeen[date]:
			network = twentyfour_to_prefix_Map[prefix] 
			notRouted.discard(network)
			if network in prefix_to_24_Map:
				prefix_to_24_Map[network].discard(prefix)
		for key in prefix_to_24_Map.keys():
			if len(prefix_to_24_Map[key]) == 0:
				fullRouted = fullRouted + 1
			if len(prefix_to_24_Map[key]) != 0:
				if not prefix_to_24_MapCopy[key].issubset(prefix_to_24_Map[key]):
					partRouted = partRouted +1

		outputfile.write(str(len(prefix_to_24_MapCopy)) + "," + str(len(notRoutedCopy)) + "," + str(partRouted) + "," + str(fullRouted) + "," + routingDate + "\n")
	outputfile.close()'''

	'''for key in prefix_to_24_Map.keys():
		if len(prefix_to_24_Map[key]) == 0:
			fullRouted = fullRouted + 1
		if len(prefix_to_24_Map[key]) != 0:
			if not prefix_to_24_MapCopy[key].issubset(prefix_to_24_Map[key]):
				partRouted = partRouted +1

	outputfile = open("./results/RoutedStatus.csv", 'w')
	outputfile.write(str(len(prefix_to_24_MapCopy)) + "," + str(len(notRouted)) + "," + str(partRouted) + "," + str(fullRouted) + "\n")
	outputfile.close()'''

  	print("File processing finished")

