from subprocess import Popen, PIPE
import multiprocessing
import os
#import radix
from netaddr import *
import copy

prefixOriginFirst = {} #Dictionary to store the first file with prefixes read, and then to store the last one read
prefixOriginSecond = {} #Dictionary to store the second file with prefixes read, and then to store the present one
prefixDiferences = {} #Dictionary to store the differences between the first and second, if key not found in one of them stores it
prefixChanges = {} #Dictionary to store the differences in Origins in the first and second, if origin is different saves it into a set
prefixCycles = {} #Dictionary to store the differences in Origins in the first and second, if origin is different saves it into a list to be able to see cycles
prefixAses = set()
count = 0.00

def readFile(fil, prefixOrigin):
	inputfile = Popen(['sh', '-c', 'cat '+fil],stdout = PIPE, bufsize=10485760)
    	for line in inputfile.stdout:
		linearray = line.split(",")
		prefixOrigin[linearray[0]] = linearray[1]
		#aggregateAs(prefixAses, linearray[0], linearray[1])		

	inputfile.stdout.close()
    	inputfile.terminate()
	return prefixAses
		
def comparePrefixes(rtree, newRtree, difRtree, changeRtree, cycleRtree):

	'''checks if the keys in prefixOriginSecond are in prefixOriginFirst if not saves them in prefixDiferences'''
	for key in newRtree.keys():
		if not key in rtree:
			difRtree[key] = newRtree[key]

	'''checks if the keys in prefixOriginFirst are in prefixOriginSecond if not saves them in prefixDiferences'''
	for key in rtree.keys():
		if not key in newRtree:
			difRtree[key] = rtree[key]
		'''if the key is in prefixOriginSecond check if the Origin value is the same'''
		if key in newRtree:
			if rtree[key] != newRtree[key]:
				if key == "163.43.0.0/16":
					print key,rtree[key],newRtree[key]	
					if key in cycleRtree:
						print cycleRtree[key]
				
				if key in changeRtree:
					changeRtree[key].add(newRtree[key])
				if not key in changeRtree:
					ases = set()
					ases.add(rtree[key])
					ases.add(newRtree[key])
					changeRtree[key] = ases

				if key in cycleRtree:
					if newRtree[key] in cycleRtree[key] and newRtree[key] != cycleRtree[key][-1]:
						changeRtree.pop(key)
						cycleRtree[key].append(newRtree[key])
					if newRtree[key] not in cycleRtree[key]:	
						cycleRtree[key].append(newRtree[key])
					
				if not key in cycleRtree:
					origins = []
					origins.append(rtree[key])
					origins.append(newRtree[key])
					cycleRtree[key] = origins
					

def aggregateAs(rtree, prefix, origin):
	if prefix not in rtree:
		ases = set()
		ases.add(origin)
		rtree[prefix] = ases
		return rtree
	rtree[prefix].add(origin)
	return rtree

def joinAses(rtree, changeRtree):
	for key in changeRtree.keys():
		t =()
		for ase in changeRtree[key]:
			t = t + (ase,)
		rtree.add(t)
	return rtree		

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
	
	outputfile = open("./results/aggregateAsesByStep_withCycle_24_new.csv", 'w')

	for fil in files:
		print ("reading file: %s" % fil) 
		filename = os.path.split(fil)[1]
		filedate = filename[4:8] +"-"+ filename[8:10] +"-"+filename[10:12]
		prefixDiferences = {}
		prefixOriginSecond = {}
		prefixChanges = {}
		prefixAses = set()
		if not prefixOriginFirst:
			readFile(fil, prefixOriginFirst)
		else:
			readFile(fil, prefixOriginSecond)
			comparePrefixes(prefixOriginFirst, prefixOriginSecond, prefixDiferences, prefixChanges, prefixCycles)
			joinAses(prefixAses, prefixChanges)
			prefixOriginFirst = copy.deepcopy(prefixOriginSecond)
			print len(prefixChanges)
			outputfile.write(str(len(prefixOriginFirst)) + "," + str(len(prefixDiferences))+ "," +str(len(prefixChanges))+ "," + str(len(prefixAses))+ "," + filedate + "\n" )	

	'''for prefix in prefixAses:
		outputfile.write(str(prefix))
		for ase in prefixAses[prefix]:
			outputfile.write("," + str(ase))
		outputfile.write("," + str(len(prefixAses[prefix])) + "\n" )
		count = count + len(prefixAses[prefix])'''
	#print count/len(prefixAses)
	outputfile.close()

	outputfile = open("./results/aggregateAsesByStep_CycleList_24_new.csv", 'w')
	for prefix in prefixCycles:
		outputfile.write(str(prefix))
		for ase in prefixCycles[prefix]:
			outputfile.write("," + str(ase))
		outputfile.write("," + str(len(prefixCycles[prefix])) + "\n" )	

	outputfile.close()

  	print("Finished processing")
  	
