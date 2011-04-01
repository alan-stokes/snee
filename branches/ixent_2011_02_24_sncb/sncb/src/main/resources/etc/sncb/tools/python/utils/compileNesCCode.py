import getopt, string, os, logging, sys, re, os.path, time
 
optDataPath = None

logger = None

optNescDir = os.getcwd()
optCompileTarget = "telosb_t2"
optSensorBoard = None

def usage():
	print 'usage: python compileNesCCode.py --nesc-dir=[dir] --compile-target=[telosb_t2] --sensor-board=[micasb|mts300]'
	sys.exit(2)

def parseArgs(args):
	global optNescDir, optCompileTarget, optSensorBoard

	try:
		optNames = ["help", "nesc-dir=", "compile-target=", "sensor-board="]

		opts, args = getopt.getopt(args, "h", optNames)
	except getopt.GetoptError, err:
		print str(err)
		usage()
		sys.exit(2)
		
	for o, a in opts:
		if (o == "-h" or o== "--help"):
			usage()
			sys.exit()
		if (o == '--nesc-dir'):
			optNescDir = str(a)
		if (o == '--compile-target'):
			optCompileTarget = str(a)
		if (o == '--sensor-board'):
			optSensorBoard = str(a)
			
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

def parseMemoryValues(logfile, targetParameter = "telosb"):
	inFile =  open(logfile)
	while 1:
		line = inFile.readline()
		if not line:
			return None;
		m = re.search("    compiled QueryPlan(\d+) to build/"+targetParameter+"/main.exe", line)
		if (m != None):
			mote = int(m.group(1))			
			report("Compiled mote "+str(mote))
		m = re.search("( +)(\d+) bytes in ROM", line)
		if (m != None):
			rom = int(m.group(2))
			report("ROM usage = "+str(rom))
		m = re.search("( +)(\d+) bytes in RAM", line)
		if (m != None):
			ram = int(m.group(2))
			report("RAM usage = "+str(ram))
			return (mote, ram, rom)
		
#reads the logfile from the current directory and reports the main details
def readMakeLog(logfile, sizeFile, targetParameter = "telosb"):
	global ram, rom
	sizeStr = ""
	(mote, ram, rom) = parseMemoryValues(logfile, targetParameter)
	if sizeFile!=None:
		sizeFile.write(str(mote)+","+str(rom)+","+str(ram)+","+str(rom+ram)+"\n")
	if (ram > 4096):
		reportError ("RAM = "+str(ram)+" in file "+str(sizeFile))
		return ram
	return 0

def doCompileNesCCode(dir, targetParameter = "telosb", sensorBoard = None):
	os.chdir(dir)
	exitVal = 0
	
	if sensorBoard!=None:
		commandStr = "SENSORBOARD="+sensorBoard+" "
	else:
		commandStr = ""

	commandStr += "make "+targetParameter+" >make.log"
	report(commandStr)
	exitVal = os.system(commandStr)

	if (exitVal != 0):
		reportError("Failed with "+dir+" compilation")
		sys.exit(exitVal)

	if (targetParameter=="telosb" and dir.startswith("mote")):
		exitVal = readMakeLog("make.log", None, targetParameter)
		if (exitVal != 0):
			reportError("RAM overflow with "+dir+" compilation")
			sys.exit(exitVal)

	return exitVal	

def generateODFile(dir, targetParameter):
	commandStr = "avr-objdump -zhD ./build/"+targetParameter+"/main.exe > ../"+dir+".od"
	os.system(commandStr)

	commandStr = "cp ./build/"+targetParameter+"/main.exe ../"+dir+".elf"
	os.system(commandStr)

#Given a nesC root directory, compiles the nesC code to an executable for given target
def compileNesCCode(nescRootDir, target = "telosb_t2", sensorBoard = None):
	exitVal = 0
	os.chdir(nescRootDir)

	targetParameter = "telosb"
	if (target == "tossim_t2"):
		targetParameter = "micaz sim"
	elif (target == "avrora_mica2_t2"):
		sensorBoard = "mts300"
		targetParameter = "mica2"
	elif (target == "avrora_micaz_t2"):
		targetParameter = "micaz"
		sensorBoard = "mts300"

	#TODO: check case when a combined image is used.
	if ((target == "telosb_t2") or (target == "avrora_mica2_t2") or (target == "avrora_micaz_t2")):
		for dir in os.listdir(nescRootDir):
			if (os.path.isdir(dir)):
				exitVal = doCompileNesCCode(dir, targetParameter, sensorBoard)
				if (target.startswith("avrora")):
				    generateODFile(dir, targetParameter)
				os.chdir(nescRootDir)
	elif (target == "tossim_t2"):
		exitVal = doCompileNesCCode(nescRootDir, targetParameter, sensorBoard)
	else:
		reportError("Unknown target: "+target)
		exitVal = -1

	return exitVal
	
def main():
	parseArgs(sys.argv[1:])
	exitVal = compileNesCCode(optNescDir, optCompileTarget, optSensorBoard)
	return exitVal

if __name__ == "__main__":
	main()

