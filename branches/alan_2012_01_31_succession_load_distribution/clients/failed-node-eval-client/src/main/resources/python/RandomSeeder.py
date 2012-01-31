import re, logging, random, string, os, sys, getopt

optRandomSeed = None
logger = None

def getOptNames():
	optNames = ["random-seed="]
	return optNames

def usage():
	print '\nFor the Tossim library:'
	print '--random-seed=<number> \n\tdefault: Set from Random fuunction'


def setOpts(opts):
	global optRandomSeed
	
	for o, a in opts:
		if (o == "--random-seed"):
			optRandomSeed = int(a)
			continue
			
#Registers a logger for this library
def registerLogger(l):
 	global logger
 	logger = l


#Ouput info message to screen and logger if applicable
def report(message):
 	if (logger != None):
 		logger.info (message)
 	print message


#Ouput warning message to screen and logger if applicable
def reportWarning(message):
 	if (logger != None):
 		logger.warning(message)
 	print message


#Ouput error message to screen and logger if applicable
def reportError(message):
 	if (logger != None):
 		logger.error(message)
 	print message
 		
def setRandom():
	global optRandomSeed
	if (optRandomSeed == None):
		randomSeed = random.randint(0, sys.maxint)
		random.seed(randomSeed)
		report("Random Seed used is: "+str(randomSeed)+ " which was randomly generated.\n")
	else:
		random.seed(optRandomSeed)
		report("Random Seed set to: "+str(optRandomSeed)+" requested in args.\n")

def main(): 	
	setRandom()
	report ("Done")
	

if __name__ == "__main__":
	main()
