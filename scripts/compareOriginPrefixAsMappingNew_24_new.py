from subprocess import Popen, PIPE
import multiprocessing
import os
#import radix
from netaddr import *
import copy

prefixOriginFirst = {}
prefixOriginSecond = {}
prefixDiferences = {}
prefixChanges = {}

def readFile(fil, prefixOrigin):
	inputfile = Popen(['sh', '-c', 'cat '+fil],stdout = PIPE, bufsize=10485760)
    	for line in inputfile.stdout:
		linearray = line.split(",")
		prefixOrigin[linearray[0]] = linearray[1]

	inputfile.stdout.close()
    	inputfile.terminate()
	return prefixOrigin
		
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
	
  	#pool = multiprocessing.Pool()
  	files = []
	for root, dirs, filenames in os.walk("./results/prefixMap24New_newPeer"):
		for name in filenames:
			files.append(os.path.join(root, name))
	files.sort()
	
	outputfile = open("./results/compareOriginPrefixNew_24_new.csv", 'w')
  	for fil in files:
		print ("reading file: %s" % fil) 
		filename = os.path.split(fil)[1]
		filedate = filename[4:8] +"-"+ filename[8:10] +"-"+filename[10:12]
		prefixDiferences = {}
		prefixOriginSecond = {}
		prefixChanges = {}
		if not prefixOriginFirst:
			readFile(fil, prefixOriginFirst)
		else:
			readFile(fil, prefixOriginSecond)
			comparePrefixes(prefixOriginFirst, prefixOriginSecond, prefixDiferences, prefixChanges)
			prefixOriginFirst = copy.deepcopy(prefixOriginSecond)
			#print prefixDiferences	
			print len(prefixDiferences)
			outputfile.write(str(len(prefixOriginFirst)) + "," + str(len(prefixDiferences))+ "," +str(len(prefixChanges))+ "," + filedate + "\n" )

	outputfile.close()
  	#pool.close()
  	print("pool closed")
  	#pool.join()
  	print("pool joined")

