#!/usr/bin/python
from TOSSIM import *
import sys, os, getopt


optSimDuration = __SIMULATION_DURATION_SECS__
optNumNodes = __NUM_NODES__
optLowRadioFidelity = True

ONE_SECOND = 10000000000

def usage():
	print 'Usage: runTossim.py [-options]'
	print '\t\tto run the Tossim simulation.\n'
	print 'where options include:'
	print '-h, --help\tdisplay this message'
	print '--duration <t> in seconds\n\tdefault: ' + str(optSimDuration)
	print '--num-nodes <n>\n\tdefault: ' + str(optNumNodes)
	print '--low-radio-fidelity [true|false]\n\tdefault:' + str(optLowRadioFidelity) + " (set to true for faster simulations)"

#Converts a string to boolean based on the value of the first character.
def convertBool(text):
	if text == '':
		return False
	if text[0] in ('T','t'):
		return True
	if text[0] in ('F','f'):
		return False
	print 'Cannot convert "'+text+'" to boolean value'
	sys.exit(1)
	

def parseArgs(args):
	global optSimDuration, optNumNodes, optLowRadioFidelity

	try:
		optNames = ["help", "duration=", "num-nodes=", "low-radio-fidelity="]

		opts, args = getopt.getopt(args, "h", optNames)
	except getopt.GetoptError, err:
		print str(err)
		usage()
		sys.exit(2)
		
	for o, a in opts:

		if (o == "-h" or o== "--help"):
			usage()
			sys.exit()
			
		if (o=="--duration"):
			optSimDuration = int(a)
			continue
			
		if (o=="--num-nodes"):
			optNumNodes = int(a)
			continue
			
		if (o=="--low-radio-fidelity"):
			optLowRadioFidelity = convertBool(a)
			continue


def doTossimSimulation(simDuration, numNodes):
	global optLowRadioFidelity

	t = Tossim([])
	r = t.radio()

	print "Running simulation with %d nodes and %ds duration" % (numNodes, simDuration)

	#for now, all nodes can hear each other
	#the default CCA threshold is -72 dbM
	#Set the strength to be very high (i.e., 0dB), so there is (almost) no packet loss
	for i in range(0, numNodes):
		for j in range(0, numNodes):
			if (i!=j):
				r.add(i, j, 0) 
	
	#TOSSIM also simulates the RF noise and interference a node hears, both from other nodes as well as outside sources.
	#A noise model needs to be generated for tossim to work
	if (optLowRadioFidelity==True):
		#speeds up the simulation at the cost of lower radio noise fidelity	
		noise = open("./meyer-light.txt", "r")
	else:
		#slower simulation, higher radio noise fidelity
		tosdir = os.getenv("TOSDIR")
		noise = open(tosdir + "/lib/tossim/noise/meyer-heavy.txt", "r")
	lines = noise.readlines()
	for line in lines:
		str = line.strip()
		if (str != ""):
			val = int(str)
			for i in range(0, numNodes):
				t.getNode(i).addNoiseTraceReading(val)
	for i in range(0, numNodes):
		print "Creating noise model for ",i;
		t.getNode(i).createNoiseModel()


	#send all messages to standard out for now
	outputChannels = os.getenv("DBG")
	if outputChannels == None:
		t.addChannel("DBG_USR1", sys.stdout)
	else:
		for outCh in outputChannels.split(','):
			t.addChannel("DBG_"+outCh.upper(), sys.stdout)

	#boot all nodes at the same time
	#this means that synchronization code used in TinyOS1 should not be required
	for i in range(0, numNodes):
		m = t.getNode(i);
		m.bootAtTime(0);


	while (simDuration*ONE_SECOND > t.time()):
		t.runNextEvent()


def main(): 	

	#parse the command-line arguments
	parseArgs(sys.argv[1:])
		
	doTossimSimulation(optSimDuration, optNumNodes)
	
if __name__ == "__main__":
	main()
	

