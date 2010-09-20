import getopt, string, os, logging, sys, re, os.path, time
 
optDataPath = None

logger = None

optNescDir = os.getcwd()
optCompileTarget = "telosb"
optSensorBoard = None

def usage():
	print 'usage: python compileNesCCode.py --nesc-dir=[dir] --compile-target=[tmote] --sensor-board=[micasb|mts300]'
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

def parseMemoryValues(logfile, target = "tmote"):
	inFile =  open(logfile)
	while 1:
		line = inFile.readline()
		if not line:
			return None;
		m = re.search("    compiled QueryPlan(\d+) to build/"+target+"/main.exe", line)
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
def readMakeLog(logfile, sizeFile, target = "tmote"):
	global ram, rom
	sizeStr = ""
	(mote, ram, rom) = parseMemoryValues(logfile, target)
	if sizeFile!=None:
		sizeFile.write(str(mote)+","+str(rom)+","+str(ram)+","+str(rom+ram)+"\n")
	if (ram > 4096):
		reportError ("RAM = "+str(ram)+" in file "+str(sizeFile))
	return ram
			
#Given a nesC root directory, compiles the nesC code to an executable for given target
def compileNesCCode(nescRootDir, target = "tmote", sensorBoard = None):
	exitVal = 0
	os.chdir(nescRootDir)

	#TODO: check case when a combined image is used.
	for dir in os.listdir(nescRootDir):
		if (dir.startswith("mote") and os.path.isdir(dir)):
			os.chdir(dir)
			
			if sensorBoard!=None:
				commandStr = "SENSORBOARD="+sensorBoard+" "
			else:
				commandStr = ""

			commandStr += "make "+target+" >make.log"
			report(commandStr)
			exitVal = os.system(commandStr)

			if (exitVal != 0):
				reportError("Failed with "+dir+" compilation")
				return exitVal
			exitVal = readMakeLog("make.log", None, target)
			exitVal = 0
			if (exitVal != 0):
				reportError("RAM overflow with "+dir+" compilation")
				return exitVal

			# sleep for 10 seconds
			time.sleep(10)

			commandStr = "make telosb reinstall.1 bsl,/dev/tty.usbserial-M4A7J5HX"
			report(commandStr)
			exitVal = os.system(commandStr)

			# sleep for 10 seconds
			time.sleep(10)

		os.chdir(nescRootDir)
	return exitVal
	
def main():
	parseArgs(sys.argv[1:])
	compileNesCCode(optNescDir, optCompileTarget, optSensorBoard)

if __name__ == "__main__":
	main()

