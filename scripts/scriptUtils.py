from subprocess import Popen, PIPE
import multiprocessing
import os
from netaddr import *
import re

files = []
prefixSet = set()
prefixDict = {}
prefix_to_24_Map = {}
twentyfour_to_prefix_Map = {}
masklenMapping = {}
masklenMapping["16777216"] = "/8"
masklenMapping["8388608"] = "/9"
masklenMapping["4194304"] = "/10"
masklenMapping["2097152"] = "/11"
masklenMapping["1048576"] = "/12"
masklenMapping["524288"] = "/13"
masklenMapping["262144"] = "/14"
masklenMapping["131072"] = "/15"
masklenMapping["65536"] = "/16"
masklenMapping["32768"] = "/17"
masklenMapping["16384"] = "/18"
masklenMapping["8192"] = "/19"
masklenMapping["4096"] = "/20"
masklenMapping["2048"] = "/21"
masklenMapping["1024"] = "/22"
masklenMapping["512"] = "/23"
masklenMapping["256"] = "/24"

def getSubnets(prefixDict, network, hostnumber, filedate):
	ip = IPNetwork(network)
	ip2 = IPNetwork(str(ip.ip +int(hostnumber) -1 ))
	r1 = IPRange(ip.ip, ip2.ip)
	for prefix in r1.cidrs():
		prefixDict[str(prefix)] = filedate
	return prefixDict

def getSubnets24(prefixDict, prefix_to_24_Map, twentyfour_to_prefix_Map, network, hostnumber, filedate):
	ip = IPNetwork(network)
	ip2 = IPNetwork(str(ip.ip +int(hostnumber) -1 ))
	r1 = IPRange(ip.ip, ip2.ip)
	for prefix in r1.cidrs():
		subnets = list(prefix.subnet(24))
		for subnet in subnets:
			prefixDict[str(subnet.cidr)] = filedate
			twentyfour_to_prefix_Map[str(subnet.cidr)] = str(ip.ip)
			if str(ip.ip) not in prefix_to_24_Map:
				prefixes = set()
				prefixes.add(str(subnet.cidr))
				prefix_to_24_Map[str(ip.ip)] = prefixes
			if str(ip.ip) in prefix_to_24_Map:
				prefix_to_24_Map[str(ip.ip)].add(str(subnet.cidr))
	return prefixDict, prefix_to_24_Map, twentyfour_to_prefix_Map

def readIRR(files, directory):
	'''function to read all files in certain directory and save them into a files matrix'''
	for root, dirs, filenames in os.walk(directory):
    		for name in filenames:
			files.append(os.path.join(root, name))
	return files


def getRouteIRR(fil, prefixSet):
	'''function to read a file and save route prefixes into a set'''
	inputfile = Popen(['zcat', fil],stdout = PIPE)
 
	for line in inputfile.stdout:
		if "route:" in line:
			result = re.search(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}", line)
			if result:
				prefix = result.group(0)
				'''ip = IPNetwork(prefix)
    				subnets = list(ip.subnet(24))
    				for subnet in subnets:
    					prefixSet.add(str(subnet.cidr))'''
				prefixSet.add(prefix)
			
	inputfile.stdout.close()
	inputfile.terminate()
	return prefixSet

def getAllocatedPrefix(fil, prefixDict):
	inputfile = Popen(['cat', fil],stdout = PIPE)
	for line in inputfile.stdout:
		linearray = line.split("|")
		if linearray[2] == "ipv4" and linearray[3] != "*" and len(linearray) > 6:
			if(linearray[6].strip('\n') == "assigned" or linearray[6].strip('\n') == "allocated"):
				getSubnets(prefixDict, linearray[3], linearray[4], linearray[5])
				#prefix = linearray[3] + masklenMapping[linearray[4]]
				#prefixDict[prefix] = linearray[5]
	
	inputfile.stdout.close()
	inputfile.terminate()
	return prefixDict

def getAllocatedPrefix24(fil, prefixDict, prefix_to_24_Map, twentyfour_to_prefix_Map):
	inputfile = Popen(['cat', fil],stdout = PIPE)
	for line in inputfile.stdout:
		linearray = line.split("|")
		if linearray[2] == "ipv4" and linearray[3] != "*" and len(linearray) > 6:
			if(linearray[6].strip('\n') == "assigned" or linearray[6].strip('\n') == "allocated"):
				getSubnets24(prefixDict, prefix_to_24_Map, twentyfour_to_prefix_Map, linearray[3], linearray[4], linearray[5])
				#prefix = linearray[3] + masklenMapping[linearray[4]]
				#prefixDict[prefix] = linearray[5]
	
	inputfile.stdout.close()
	inputfile.terminate()
	return prefixDict, prefix_to_24_Map, twentyfour_to_prefix_Map

if __name__ == "__main__":
	#directory = "./filesIRR"
	directory = "./allocFiles"
	readIRR(files, directory)
	for fil in files:
		#getRouteIRR(fil, prefixSet)
		getAllocatedPrefix(fil, prefixDict)
	print prefixDict
	print str(len(prefixDict))
