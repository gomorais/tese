'''script to get the bgp files and return files with a prefix-origin pair'''

from subprocess import Popen, PIPE
import multiprocessing
import os
#import radix
from netaddr import *

peers = set()
peers.add("3356")
peers.add("3549")
peers.add("2914")
peers.add("3130")

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
			originList.add(rtree[prefix])
			repeated[prefix] = originList	
    			return repeated
		if prefix in repeated:
			repeated[prefix].add(origin)

def Task(fil):

	prefixOrigin = {}
	repeated = {}
	
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
    	outputfile_peer = open(os.path.join("./results/prefixMapNew_newPeer/",os.path.split(fil)[1])+"_prefixMapping.csv", 'w')
    	'''creates a file with the peer as ASnumber, the number of prefixes it advertises, the number of prefixes contained in Radb'''
    	filedate = filename[4:8] +"-"+ filename[8:10] +"-"+filename[10:12]
	for prefix in repeated:
		if prefix in prefixOrigin:
			prefixOrigin.pop(prefix)
    	for prefix in prefixOrigin:
		outputfile_peer.write(prefix+ "," + prefixOrigin[prefix] + "," + filedate + "\n" )
    	outputfile_peer.close()

	outputfile_repeated = open(os.path.join("./results/prefixMapRepeated",os.path.split(fil)[1])+"_prefixMappingRepeated.csv", 'w')
    	'''creates a file with the peer as ASnumber, the number of prefixes it advertises, the number of prefixes contained in Radb'''
    	filedate = filename[4:8] +"-"+ filename[8:10] +"-"+filename[10:12]
    	for prefix in repeated:
		ases = repeated[prefix]
		outputfile_repeated.write(prefix+ "," + str(ases) + "," + filedate + "\n" )
    	outputfile_repeated.close()

	print len(prefixOrigin)
	print len(repeated)

if __name__ == "__main__":
	
  	pool = multiprocessing.Pool()
  	files = []
	for root, dirs, filenames in os.walk("./files"):
		for name in filenames:
			files.append(os.path.join(root, name))
  	for fil in files:
		pool.apply_async(Task, (fil,))
  	pool.close()
  	print("pool closed")
  	pool.join()
  	print("pool joined")

