
import re, logging, random, string, os, sys, getopt, UtilLib

optSneeqlRoot = os.getenv("SNEEQLROOT")
optCygwin = False
#java5CompilerExe = os.getenv("JAVA5BINDIR") + "/javac" #.exe removed for Mac
#java5exe  = os.getenv("JAVA5BINDIR") + "/java" #.exe removed for Mac
javaClasspathListSep = os.pathsep #i.e., either semicolon or colon -- need to override in Cygwin
javaClasspathPathSep = os.sep #i.e., either forward slash or back slash -- need to override in Cygwin
optCompileSneeql = False

#Setup options
optNumSourceNodes = None
optNonIntersecting = False

	#"iniFile"	#"evaluate-all-queries"	
optDeleteOldFiles = False
optQueryDir = "input/pipes"
optQuery = None 
	#MQE #"sinks"	#"qos-file"	
optSneeqlNetworkTopologyFile = None	
	#"site-resource-file"	
optSchemaFile = None
optOperatorMetadataFile = None
	#"types-file"	
optHaskellUse = True
	#"haskell-decl-dir"
	#"haskell-main"	#"haskell-decl-file"	#"qos-aware-partitioning"	#"qos-aware-routing"
	#"qos-aware-where-scheduling"	#"qos-aware-when-scheduling	
optOutputRootDir = None
	#"output-query-plan-dir"	#"output-nesc-dir"	
optDebugExitOnError = None
optMeasurementsActiveAgendaLoops = None
optMeasurementsIgnoreIn = None
optMeasurementsRemoveOperators = None
optMeasurementsThinOperators = None
optMeasurementsMultiAcquire = None
	#"display-graphs"
	#"display-operator-properties"	#"display-sensornet-link-properties"
	#"display-site-properties"	#"display-config-graphs"
optDisplayCostExpressions = None	
optRemoveUnrequiredOperators = None
optPushProjectionDown = None
optCombineAcquireAndSelect = None
	#"combine-acquire-and-select"	
	#"routing-random-seed"	#"routing-trees-to-generate"	#"routing-trees-to-keep"
optWhereSchedulingRemoveRedundantExchanges = None
	#"when-scheduling-decrease-bfactor-to-avoid-agenda-overlap"	
optTargets = None
	#"nesc-control-radio-off"	#"nesc-adjust-radio-power"	#"nesc-synchronization-period"
	#"nesc-power-management"
optNescDeliverLast = None
optNescLedDebug = None
optNescYellowExperiment = None
optNescGreenExperiment = None
optNescRedExperiment = None
optQosAcquisitionInterval = None	
	#"qos-min-acquisition-interval"	#"qos-max-acquisition-interval"
optQosMaxDeliveryTime = None
	
	#"qos-max-total-energy"	#"qos-min-lifetime"
optQosMaxBufferingFactor = None	
optQosBufferingFactor = None	#"qos-query-duration"

def getOptNames():
	optNames = ["sneeql-root=", "javac-5-exe=", "java-5-exe="]
	optNames += ["cygwin"]
	optNames += ["compile-sneeql=", "num-source-nodes=","non-intersecting="]
	#"iniFile"	#"evaluate-all-queries"			
	optNames += ["delete-old-files=","query-dir=","query="] 
	#MQE #"sinks"	#"qos-file"	
	optNames += ["sneeql-network-topology-file=","network-topology-file="]
	#"site-resource-file"	
	optNames += ["schema-file=", "operator-metadata-file="]
	#"types-file"	
	optNames += ["haskell-use="]
	#"haskell-decl-dir"
	#"haskell-main"	#"haskell-decl-file"	#"qos-aware-partitioning"	#"qos-aware-routing"
	#"qos-aware-where-scheduling"	#"qos-aware-when-scheduling	
	optNames += ["output-root-dir="]
	#"output-query-plan-dir"	#"output-nesc-dir"	
	optNames += ["debug-exit-on-error="]
	optNames += ["measurements-active-agenda-loops=","measurements-ignore-in="]
	optNames += ["measurements-remove-operators=","measurements-thin-operators="]
	optNames += ["measurements-multi-acquire="]
	#"display-graphs"
	#"display-operator-properties"	#"display-sensornet-link-properties"
	#"display-site-properties"	#"display-config-graphs"
	optNames += ["display-cost-expressions="]
	optNames += ["remove-unrequired-operators=","push-projection-down=","combine-acquire-and-select="]
	#"routing-random-seed"	#"routing-trees-to-generate"	#"routing-trees-to-keep"
	optNames += ["where-scheduling-remove-redundant-exchanges="]
	#"when-scheduling-decrease-bfactor-to-avoid-agenda-overlap"
	optNames += ['targets=']
	#"nesc-control-radio-off"	#"nesc-adjust-radio-power"	#"nesc-synchronization-period"
	#"nesc-power-management"	
	optNames += ["nesc-deliver-last"]
	optNames += ["nesc-led-debug=","nesc-yellow-experiment=","nesc-green-experiment="]
	optNames += ["nesc-red-experiment=",	"qos-acquisition-interval="]
	#"qos-min-acquisition-interval"	#"qos-max-acquisition-interval"
	optNames += ["qos-max-delivery-time="]
	#"qos-max-total-energy"	#"qos-min-lifetime"
	optNames += ["qos-max-buffering-factor=","qos-buffering-factor="]	#"qos-query-duration"

	return optNames

def usage():
	print '\nFor the SNEEql library:'
	print '--sneeql-root=<dir> \n\tdefault: '+optSneeqlRoot
	print '--javac-5-exe=<file> \n\tdefault: '+java5CompilerExe
	print '--java-5-exe=<file> \n\tdefault: '+java5exe
	print '--compile-sneeql \n\tdefault: ' + str(optCompileSneeql)
	print '--num-source-nodes=<number> \n\tdefault: Randomly generated based on other settings.'
	print "--non-intersecting=True|False \n\tdefaults: "+ str(optNonIntersecting)
	print 'Java Query compiler Settings.'
	print 'Default is not not specify it leaving it to the SNEEql.ini file setting'
	#"iniFile"	#"evaluate-all-queries"	
	print 'delete-old-files=True|False \n\tdefault:'+str(optDeleteOldFiles)
	print '--query-dir=<file> \n\tdefault: '+optQueryDir
	print '--queries=Q' 
	#MQE #"sinks"	#"qos-file"	
	print "--sneeql-network-topology-file=<file>"	
	#"site-resource-file"	
	print "--schema-file=<file>"
	print "--operator-metadata-file=<file>"
	#"types-file"	
	print "--haskell-use==True|False \n\tdefault:" + str(optHaskellUse)
	#"haskell-decl-dir"
	#"haskell-main"	#"haskell-decl-file"	#"qos-aware-partitioning"	#"qos-aware-routing"
	#"qos-aware-where-scheduling"	#"qos-aware-when-scheduling	
	print "--output-root-dir=<file>"
	#"output-query-plan-dir"	#"output-nesc-dir"	
	print "--debug-active-agenda-loops=True|False"
	print "--measurements-active-agenda-loops=Int"
	print "--measurements-ignore-in=String"
	print "--measurements-remove-operators=String"
	print "--measurements-thin-operators=String"	
	print "--measurements-multi-acquire=Int"
	#"display-graphs"
	#"display-operator-properties"	#"display-sensornet-link-properties"
	#"display-site-properties"	#"display-config-graphs"
	print "--display-cost-expressions=True|False"
	print "--remove-unrequired-operators=True|False"
	print "--push-projection-down=True|False"
	print "combine-acquire-and-select=True|False"
	#"routing-random-seed"	#"routing-trees-to-generate"	#"routing-trees-to-keep"
	print '--where-scheduling-remove-redundant-exchanges=True|False'
	#"when-scheduling-decrease-bfactor-to-avoid-agenda-overlap"
	print '--targets={avrora1,avrora2,tossim1,tossim2,insense}'
	#"nesc-control-radio-off"	#"nesc-adjust-radio-power"	#"nesc-synchronization-period"
	#"nesc-power-management"	
	print '--nesc-deliver-last=True|False'
	print '--nesc-led-debug=True|False'
	print '--nesc-yellow-experiment=String\n\tIf Set will force nesc-led-debug=True'
	print '--nesc-green-experiment=String\n\tIf Set will force nesc-led-debug=True'
	print '--nesc-red-experiment=String\n\tIf Set will force nesc-led-debug=True'
	print "--qos-acquisition-interval=Int"	#"qos-min-acquisition-interval"	#"qos-max-acquisition-interval"
	print "--qos-max-delivery-time=<int>"
	#"qos-max-total-energy"	#"qos-min-lifetime"
	print "--qos-max-buffering-factor=<int>"	
	print "--qos-buffering-factor=<int>"	#"qos-query-duration"
	
def setOpts(opts):
	global optSneeqlRoot, java5CompilerExe, java5exe 
	global optCygwin
	global javaClasspathListSep, javaClasspathPathSep
	global optCompileSneeql, optNumSourceNodes, optNonIntersecting

	#"iniFile"	#"evaluate-all-queries"	
	global optDeleteOldFiles, optQueryDir, optQuery 
	#MQE #"sinks"	#"qos-file"	
	global optSneeqlNetworkTopologyFile	
	#"site-resource-file"	
	global optSchemaFile
	global optOperatorMetadataFile
	#"types-file"	
	global optHaskellUse
	#"haskell-decl-dir"
	#"haskell-main"	#"haskell-decl-file"	#"qos-aware-partitioning"	#"qos-aware-routing"
	#"qos-aware-where-scheduling"	#"qos-aware-when-scheduling	
	global optOutputRootDir
	#"output-query-plan-dir"	#"output-nesc-dir"	
	global optDebugExitOnError
	global optMeasurementsActiveAgendaLoops, optMeasurementsIgnoreIn
	global optMeasurementsRemoveOperators, optMeasurementsThinOperators
	global optMeasurementsMultiAcquire
	#"display-graphs"
	#"display-operator-properties"	#"display-sensornet-link-properties"
	#"display-site-properties"	#"display-config-graphs"
	global optDisplayCostExpressions
	global optRemoveUnrequiredOperators, optPushProjectionDown, optCombineAcquireAndSelect
	#"routing-random-seed"	#"routing-trees-to-generate"	#"routing-trees-to-keep"
	global optWhereSchedulingRemoveRedundantExchanges 
	#"when-scheduling-decrease-bfactor-to-avoid-agenda-overlap"
	global optTargets
	#"nesc-control-radio-off"	#"nesc-adjust-radio-power"	#"nesc-synchronization-period"
	#"nesc-power-management"
	global optNescDeliverLast
	global optNescLedDebug,	optNescYellowExperiment, optNescGreenExperiment
	global optNescRedExperiment, optQosAcquisitionInterval	
	#"qos-min-acquisition-interval"	#"qos-max-acquisition-interval"
	global optQosMaxDeliveryTime	
	#"qos-max-total-energy"	#"qos-min-lifetime"
	global optQosMaxBufferingFactor, optQosBufferingFactor	#"qos-query-duration"
	global optNumSourceNodes
	
	for o, a in opts:
		if (o== "--sneeql-root"):
			optSneeqlRoot = a
			continue
		if (o== "--javac-5-exe"):
			java5CompilerExe = a
			continue
		if (o== "--java-5-exe"):
			java5exe = a
			continue
		if (o== "--cygwin"):
			optCygwin = True
			javaClasspathListSep = ";"
			javaClasspathPathSep = "\\"
			java5CompilerExe = java5CompilerExe + ".exe"
			java5exe = java5exe + ".exe"
			continue
		if (o == "--compile-sneeql"):
			optCompileSneeql = UtilLib.convertBool(a)
			continue			
		if (o== "--num-source-nodes"):
			optNumSourceNodes = int(a)
			continue
		if (o== "--non-intersecting"):
			optNonIntersecting= UtilLib.convertBool(a)
			continue		
		#Java compiler options	
		#"iniFile"	#"evaluate-all-queries"	#"query-dir"		
		if (o == "--delete-old-files"):
			optDeleteOldFiles = UtilLib.convertBool(a)
			continue
		if (o == "--query-dir"):
			optQueryDir = a
			continue
		if (o == "--query"):
			optQuery = a
			continue
		#MQE #"sinks"	#"qos-file"	
		if (o == "--sneeql-network-topology-file"):
			optSneeqlNetworkTopologyFile = a
			continue
		if (o == "--network-topology-file"):
			optSneeqlNetworkTopologyFile = a
			continue
		#"network-topology-file"	#"site-resource-file"
		if (o == "--schema-file"):
		    optSchemaFile = a
		    continue
		if (o == "--operator-metadata-file"):
		    optOperatorMetadataFile = a
		    continue
		#"types-file"	
		if (o == "--haskell-use"):
			optHaskellUse = UtilLib.convertBool(a)
			continue
		#"haskell-decl-dir"
		#"haskell-main"	#"haskell-decl-file"	#"qos-aware-partitioning"	#"qos-aware-routing"
		#"qos-aware-where-scheduling"	#"qos-aware-when-scheduling	
		if (o == "--output-root-dir"):
			optOutputRootDir = a
			continue
		#"output-query-plan-dir"	#"output-nesc-dir"	
		if (o == "--debug-exit-on-error"):
			optDebugExitOnError = a
			continue
		if (o == "--measurements-active-agenda-loops"):
			optMeasurementsActiveAgendaLoops = int(a)
			continue
		if (o == "--measurements-ignore-in"):
			optMeasurementsIgnoreIn = a
			continue
		if (o == "--measurements-remove-operators"):
			optMeasurementsRemoveOperators = a
			continue
		if (o == "--measurements-thin-operators"):
			optMeasurementsThinOperators = a
			continue
		if (o == "--measurements-multi-acquire"):
			optMeasurementsMultiAcquire = a
			continue
		#"display-graphs"
		#"display-operator-properties"	#"display-sensornet-link-properties"
		#"display-site-properties"	#"display-config-graphs"	
		if (o == "--display-cost-expressions"):
			optDisplayCostExpressions = UtilLib.convertBool(a)
			continue
		if (o == "--remove-unrequired-operators"):
			optRemoveUnrequiredOperators = UtilLib.convertBool(a)
			continue
		if (o == "--push-projection-down"):
			optPushProjectionDown = UtilLib.convertBool(a)
			continue
		if (o == "--combine-acquire-and-select"):
			optCombineAcquireAndSelect = UtilLib.convertBool(a)
			continue
		#"routing-random-seed"	#"routing-trees-to-generate"	#"routing-trees-to-keep"
		if (o == "--where-scheduling-remove-redundant-exchanges"):
			optWhereSchedulingRemoveRedundantExchanges = UtilLib.convertBool(a)
			continue
		#"when-scheduling-decrease-bfactor-to-avoid-agenda-overlap"	
		if (o == "--targets"):
			optTargets = a
			continue
		#"nesc-control-radio-off"	#"nesc-adjust-radio-power"	#"nesc-synchronization-period"
		#"nesc-power-management"
		if (o == "nesc-deliver-last"):
			optNescDeliverLast = UtilLib.convertBool(a)
			continue
		
		if (o == "--nesc-led-debug"):
			optNescLedDebug = UtilLib.convertBool(a)
			if (not optNescLedDebug):
				if (optNescYellowExperiment != None):
					reportWarning("Overriding --nesc-led-debug to true")
					optNescLedDebug = True
				if (optNescGreenExperiment != None):
					reportWarning("Overriding --nesc-led-debug to true")
					optNescLedDebug = True
				if (optNescRedExperiment != None):
					reportWarning("Overriding --nesc-led-debug to true")
					optNescLedDebug = True
			continue			
		if (o == "--nesc-yellow-experiment"):
			optNescYellowExperiment = a
			optNescLedDebug = True
			continue			
		if (o == "--nesc-green-experiment"):
			optNescGreenExperiment = a
			optNescLedDebug = True
			continue			
		if (o == "--nesc-red-experiment"):
			optNescRedExperiment = a
			optNescLedDebug = True
			continue			
		if (o == "--qos-acquisition-interval"):
		 	optQosAcquisitionInterval = int(a)
		 	continue  
		#"qos-min-acquisition-interval"	#"qos-max-acquisition-interval"
		if (o == "--qos-max-delivery-time"):
			optQosMaxDeliveryTime = int(a)
		#"qos-max-total-energy"	#"qos-min-lifetime"
		if (o == "--qos-max-buffering-factor"):
			optQosMaxbufferingFactor = int(a)
			continue
		if (o == "--qos-buffering-factor"):
			optQosBufferingFactor = int(a)
			continue
		#"qos-query-duration"

logger = None
			
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

#Compiles java query optimizer
#The 'c' directly after the java5exe is to make it a complier
def compileQueryOptimizer():
	if optCompileSneeql:
		os.chdir(optSneeqlRoot)

		commandStr = java5CompilerExe +" -sourcepath src -classpath \"src"+javaClasspathListSep+"lib"+javaClasspathPathSep+"antlr-2.7.5.jar\" src/uk/ac/manchester/cs/diasmc/querycompiler/QueryCompiler.java -d bin" #os.pathsep: windows uses ; in classpath, mac uses :
		report(commandStr)
		exitVal = os.system(commandStr)
		
		if (exitVal!=0):
			reportWarning('SNEEql Java query optimizer compilation failed with exit value '+str(exitVal))
			sys.exit(2)
		else:
			report('SNEEql Java query optimizer compiled succesfully')
		return exitVal
	else:
		report("Compilation of Query Optimizer skipped as requested")
		return 0	

#Given query optimizer parameters generates a query plan and nesC code accordingly
#Will exit the script if the SNEEql compiler exits with a value other than 0 or 1
def compileQueryParamStr(queryCompilerParams, desc):

	#compile query
	paramStr = string.join(queryCompilerParams, " ")
	os.chdir(optSneeqlRoot)
	
	commandStr = "%s -Xmx1024m -classpath \"bin%ssrc%slib%santlr-2.7.5.jar\" uk.ac.manchester.cs.diasmc.querycompiler.QueryCompiler %s > logFile.txt" % (java5exe, javaClasspathListSep, javaClasspathListSep, javaClasspathPathSep, paramStr) #problem with ; again 
	report(commandStr)
	exitVal = os.system(commandStr)
	
	#Write any console output to the logfile, if it exists
	lf = open("logFile.txt", 'r')
	if (logger != None):
		logger.info(lf.read())
	
	#Check if there is an error and if it is serious enough to stop the whole script
	if (exitVal==0):
		report('Query compiled succesfully for query '+desc)
	elif (exitVal <= 256): 
		reportError('Query compiler ended with exit value '+str(exitVal)+' for query '+desc)
	else:
		reportError('Query compiler ended with critical exit value '+str(exitVal)+' for query '+desc)
#		reportError('Critical Error aborting script')
#		sys.exit(2)
		
	return exitVal

def compileQuery(desc, 
	#"iniFile"	#"evaluate-all-queries"	
	deleteOldFiles = None,
	queryDir = None,
	query = None, 
	#"sinks" #"qos-file" #"network-topology-file"
	sneeqlNetworkTopologyFile = None,
	#"site-resource-file"	
	schemaFile = None,
	operatorMetadataFile = None,
	#"types-file"	
	haskellUse = None,
	#"haskell-decl-dir"	#"haskell-main"	#"haskell-decl-file"	#"qos-aware-partitioning"
	#"qos-aware-routing"	#"qos-aware-where-scheduling"	#"qos-aware-when-scheduling
	outputRootDir = None,
	#"output-query-plan-dir"	#"output-nesc-dir"	
	debugExitOnError = None,
	measurementsActiveAgendaLoops = None,
	measurementsIgnoreIn = None,
	measurementsRemoveOperators = None,
	measurementsThinOperators = None,
	measurementsMultiAcquire = None,
	#"display-graphs"
	#"display-operator-properties"	#"display-sensornet-link-properties"	#"display-site-properties"
	#"display-config-graphs"
	displayCostExpressions = None,
	removeUnrequiredOperators = None,
	pushProjectionDown = None, 
	combineAcquireAndSelect = None,
	#"routing-random-seed"
	#"routing-trees-to-generate"	#"routing-trees-to-keep"	
	whereSchedulingRemoveRedundantExchanges = None,
	#"when-scheduling-decrease-bfactor-to-avoid-agenda-overlap" 
	targets = None,
	#"nesc-control-radio-off"	#"nesc-adjust-radio-power"	#"nesc-synchronization-period"
	#"nesc-power-management"
	nescDeliverLast = None,
	nescLedDebug = None, nescYellowExperiment = None, nescGreenExperiment = None,
	nescRedExperiment = None, qosAcquisitionInterval = None,
	#"qos-min-acquisition-interval"	#"qos-max-acquisition-interval"
	qosMaxDeliveryTime = None,
	#"qos-max-total-energy"	#"qos-min-lifetime"	
	qosBufferingFactor = None, qosMaxBufferingFactor = None):
	#"qos-query-duration"
	
	params = []
	#"iniFile"	#"evaluate-all-queries"	#"query-dir"		

	if (deleteOldFiles != None):
		params += ["-delete-old-files="+str(deleteOldFiles)]
	elif (optDeleteOldFiles != None):
		params += ["-delete-old-files="+str(optDeleteOldFiles)]
	
	queryDirStr = ""
	if (queryDir != None):
		queryDirStr = queryDir + "/"
	elif (optQueryDir != None):
		queryDirStr = optQueryDir + "/"

	if (query != None):
		params += ["-query="+UtilLib.winpath(queryDirStr+query+".txt")]
	elif (optQuery != None):
		params += ["-query="+UtilLib.winpath(queryDirStr+optQuery+".txt")]	
	
	#"sinks" #"qos-file" #"network-topology-file"
	
	if (sneeqlNetworkTopologyFile != None): 
		params+= ["-network-topology-file="+UtilLib.winpath(sneeqlNetworkTopologyFile)]
	elif (optSneeqlNetworkTopologyFile != None): 
		params+= ["-network-topology-file="+UtilLib.winpath(optSneeqlNetworkTopologyFile)]
	
	#"site-resource-file"	
	
	if schemaFile:
		params+= ["-schema-file="+schemaFile]
	elif optSchemaFile:
		params+= ["-schema-file="+optSchemaFile]

	if operatorMetadataFile:
		params+= ["-operator-metadata-file="+operatorMetadataFile]
	elif optOperatorMetadataFile:
		params+= ["-operator-metadata-file="+optOperatorMetadataFile]
	
	#"types-file"	
	if haskellUse:
		params+= ["-haskell-use="+str(haskellUse)]
	elif optHaskellUse:
		params+= ["-haskell-use="+str(optHaskellUse)]

	#"haskell-decl-dir"	#"haskell-main"	#"haskell-decl-file"	#"qos-aware-partitioning"
	#"qos-aware-routing"	#"qos-aware-where-scheduling"	#"qos-aware-when-scheduling
	
	if (outputRootDir != None):
		params+= ["-output-root-dir="+UtilLib.winpath(outputRootDir)]	
	elif (optOutputRootDir != None):
		params+= ["-output-root-dir="+UtilLib.winpath(optOutputRootDir)]	
	
	#"output-query-plan-dir"	#"output-nesc-dir"	
	

	if (debugExitOnError != None):
		params+= ["-debug-exit-on-error="+str(debugExitOnError).lower()]	
	elif (optDebugExitOnError != None):
		params+= ["-debug-exit-on-error="+str(optDebugExitOnError).lower()]	

	if (measurementsActiveAgendaLoops != None):
		if (measurementsActiveAgendaLoops >= 0):
			params+= ["-measurements-active-agenda-loops="+str(measurementsActiveAgendaLoops).lower()]	
		else:
			params+= ["-measurements-active-agenda-loops="+str(-measurementsActiveAgendaLoops).lower()]	
	elif (optMeasurementsActiveAgendaLoops != None):
		if (optMeasurementsActiveAgendaLoops >= 0):
			params+= ["-measurements-active-agenda-loops="+str(optMeasurementsActiveAgendaLoops).lower()]	
		else:	
			params+= ["-measurements-active-agenda-loops="+str(-optMeasurementsActiveAgendaLoops).lower()]	

	if (measurementsIgnoreIn != None):
		if (measurementsIgnoreIn != ""):
			params+= ["-measurements-ignore-in="+str(measurementsIgnoreIn).lower()]	
	elif (optMeasurementsIgnoreIn != None):
		if (optMeasurementsIgnoreIn != ""):
			params+= ["-measurements-ignore-in="+str(optMeasurementsIgnoreIn).lower()]	

	if (measurementsRemoveOperators != None):
		if (measurementsRemoveOperators != ""):
			params+= ["-measurements-remove-operators="+str(measurementsRemoveOperators).lower()]	
	elif (optMeasurementsRemoveOperators != None):
		if (optMeasurementsRemoveOperators != ""):
			params+= ["-measurements-remove-operators="+str(optMeasurementsRemoveOperators).lower()]	

	if (measurementsThinOperators != None):
		if (measurementsThinOperators != ""):
			params+= ["-measurements-thin-operators="+str(measurementsThinOperators).lower()]	
	elif (optMeasurementsThinOperators != None):
		if (optMeasurementsThinOperators != ""):
			params+= ["-measurements-thin-operators="+str(optMeasurementsThinOperators).lower()]	

	if (measurementsMultiAcquire != None):
		if (measurementsMultiAcquire >= 0):
			params+= ["-measurements-multi-acquire="+str(measurementsMultiAcquire)]	
	elif (optMeasurementsMultiAcquire!= None):
		if (optMeasurementsMultiAcquire >= 0):
			params+= ["-measurements-multi-acquire="+str(optMeasurementsMultiAcquire)]	

#"display-graphs"
	#"display-operator-properties"	#"display-sensornet-link-properties"	#"display-site-properties"
	#"display-config-graphs"	

	if (displayCostExpressions != None):
		params+= ["-display-cost-expressions="+str(displayCostExpressions).lower()]	
	elif (optDisplayCostExpressions != None):
		params+= ["-display-cost-expressions="+str(optDisplayCostExpressions).lower()]	

	if (removeUnrequiredOperators != None):
		params+= ["-remove-unrequired-operators="+str(removeUnrequiredOperators).lower()]	
	elif (optRemoveUnrequiredOperators != None):
		params+= ["-remove-unrequired-operators="+str(optRemoveUnrequiredOperators).lower()]	
		
	if (pushProjectionDown != None):
		params+= ["-push-projection-down="+str(pushProjectionDown).lower()]	
	elif (optPushProjectionDown != None):
		params+= ["-push-projection-down="+str(optPushProjectionDown).lower()]	
		
	if (combineAcquireAndSelect != None):
		params+= ["-combine-acquire-and-select="+str(combineAcquireAndSelect).lower()]	
	elif (optCombineAcquireAndSelect):
		params+= ["-combine-acquire-and-select="+str(optCombineAcquireAndSelect).lower()]	
		
	#"routing-random-seed"	#"routing-trees-to-generate"	#"routing-trees-to-keep"	
	if (whereSchedulingRemoveRedundantExchanges != None):
		params+= ["-where-scheduling-remove-redundant-exchanges="+str(whereSchedulingRemoveRedundantExchanges).lower()]	
	elif (optWhereSchedulingRemoveRedundantExchanges):
		params+= ["-where-scheduling-remove-redundant-exchanges="+str(optWhereSchedulingRemoveRedundantExchanges).lower()]
	
	#"when-scheduling-decrease-bfactor-to-avoid-agenda-overlap" 
	
	if (targets != None):
		params+= ["-targets="+str(targets).lower()]	
	elif (optTargets):
		params+= ["-targets="+str(optTargets).lower()]	
	
	#"nesc-control-radio-off"	#"nesc-adjust-radio-power"	#"nesc-synchronization-period"
	#"nesc-power-management"
	
	if (nescDeliverLast != None):
		params+= ["nesc-deliver-last="+str(nescDeliverLast).lower()]
	elif (optNescDeliverLast != None):
		params+= ["nesc-deliver-last="+str(optNescDeliverLast).lower()]	

	if (nescLedDebug != None):
		params+= ["-nesc-led-debug="+str(nescLedDebug).lower()]
	elif (optNescLedDebug != None):
		params+= ["-nesc-led-debug="+str(optNescLedDebug).lower()]
		
	if (nescYellowExperiment != None):
		params+= ["-nesc-yellow-experiment="+str(nescYellowExperiment).lower()]
	elif (optNescYellowExperiment != None):
		params+= ["-nesc-yellow-experiment="+str(optNescYellowExperiment).lower()]

	if (nescGreenExperiment != None):
		params+= ["-nesc-green-experiment="+str(nescGreedExperiment).lower()]
	elif (optNescGreenExperiment != None):
		params+= ["-nesc-green-experiment="+str(optNescGreenExperiment).lower()]

	if (nescRedExperiment != None):
		params+= ["-nesc-red-experiment="+str(nescRedExperiment).lower()]
	elif (optNescRedExperiment != None):
		params+= ["-nesc-red-experiment="+str(optNescRedExperiment).lower()]

	if (qosAcquisitionInterval != None):
		params+= ["-qos-acquisition-interval="+str(qosAcquisitionInterval)]
	elif (optQosAcquisitionInterval != None):
		params+= ["-qos-acquisition-interval="+str(optQosAcquisitionInterval)]
	
	#"qos-min-acquisition-interval"	#"qos-max-acquisition-interval"
	#"qos-max-delivery-time"	
	if (qosMaxDeliveryTime != None):
		params+= ["-qos-max-delivery-time="+str(qosMaxDeliveryTime)]
	elif (optQosMaxDeliveryTime != None):
		params+= ["-qos-max-delivery-time="+str(optQosMaxDeliveryTime)]	
	#"qos-max-total-energy"	#"qos-min-lifetime"	
	if (qosMaxBufferingFactor != None):
		params+= ["-qos-max-buffering-factor="+str(qosMaxBufferingFactor)]
	elif (optQosMaxBufferingFactor != None):
		params+= ["-qos-max-buffering-factor="+str(optMaxQosBufferingFactor)]
	
	if (qosBufferingFactor != None):
		params+= ["-qos-buffering-factor="+str(qosBufferingFactor)]
	elif (optQosBufferingFactor != None):
		params+= ["-qos-buffering-factor="+str(optQosBufferingFactor)]
	else:
		print optQosBufferingFactor
	#"qos-query-duration"
	
	return compileQueryParamStr (params, desc)			

#Parses the query-plan-summary.txt file and returns the acquistion interval and buffering factor
def getQueryPlanSummary(queryPlanSummaryPath):
	acquisitionInterval = None
	bufferingFactor = None
	deliveryTime = None

	inFile = open(queryPlanSummaryPath, 'r')
	while 1:
		line = inFile.readline()
		if not line:
			break
		m = re.match("Acquisition interval used:\s*(\d+)", line)
		if (m != None):
			acquisitionInterval = int(m.group(1))
		m = re.match("Buffering factor used:\s*(\d+)", line)
		if (m != None):
			bufferingFactor = int(m.group(1))
		m = re.match("Delivery time:\s*(\d+)", line)
		if (m != None):
			deliveryTime = int(m.group(1))
	return (acquisitionInterval, bufferingFactor, deliveryTime)

#Parses the query-plan-summary.txt file and returns the buffering factor
def getAcquisitionFactor(queryPlanSummaryPath):
	bufferingFactor = None	
	return getQueryPlanSummary(queryPlanSummaryPath)[0]

#Parses the query-plan-summary.txt file and returns the buffering factor
def getBufferingFactor(queryPlanSummaryPath):
	bufferingFactor = None	
	return getQueryPlanSummary(queryPlanSummaryPath)[1]

#Parses the query-plan-summary.txt file and returns the buffering factor
def getDeliveryTime(queryPlanSummaryPath):
	deliveryTime = None	
	return getQueryPlanSummary(queryPlanSummaryPath)[2]

#Generates a random schema for streams dictionary, which is of the form {streamName : sensedAttributeName}  Thus, for each stream you can specify the name of the sensed attribute of which there is only assumed to be one.
# outPath: the path of the output file
# nonIntersecting: specifies whether the source sites for each stream may overlap or not
# numNodes: the number of nodes in the topology
def generateRandomSchema(streams, outPath, nonIntersecting = None, numNodes = 10, numSourceNodes = None):

	print "starting"

	numStreams = len(streams)

	#Determine total number of nodes to be used as source nodes based on argument and command-line parameter
	if (numSourceNodes == None):
		if (optNumSourceNodes != None):
			numSourceNodes = optNumSourceNodes
		if (numStreams > 1 and numNodes > 7):
			#limit joins to 7 * 7
			numSourceNodes = random.randrange(2, 7, 1)
		else:
			numSourceNodes = random.randrange(2, numNodes,1)
	if (nonIntersecting == None):
		nonIntersecting = optNumSourceNodes

	#Do some sanity checks
	numSourceNodesPerStream = numSourceNodes
	if nonIntersecting:		
		numSourceNodesPerStream = int(numSourceNodes / numStreams)
		if (numSourceNodesPerStream < 1):
				reportError("Unable to set "+str(numSourceNodes)+" sources  on "+str(numStreams)+" streams if schemas have to be non-intersecting")
				sys.exit(2)
	if numSourceNodesPerStream > numNodes:
		reportError("The number of source nodes specified is greater than the number of nodes in the network")
		sys.exit(2)
			
	outFile = open(outPath, 'w')
	outFile.writelines("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	outFile.writelines("<schema>\n")

	nodesUsed = []
	for streamName in streams.keys():
		if not nonIntersecting:
			nodesUsed = []
		#generate a random number of random source nodes for this stream

		sourceNodes = []
		while (len(sourceNodes) < numSourceNodesPerStream):
			candidateSourceNode = random.randrange(1, numNodes, 1)
			if (nodesUsed.count(candidateSourceNode) == 0):
				sourceNodes.append(candidateSourceNode)
				nodesUsed.append(candidateSourceNode)
		sourceNodes.sort()
		#sourceNodes = [1,2,3,4,7,9] # temporary
		sourceNodes = [str(x) for x in sourceNodes]
		sourceNodeStr = string.join(sourceNodes, ",")
		
		outFile.writelines("\t<stream name=\"" + streamName + "\" >\n")
		columns = streams.get(streamName)
		for column in columns:
			outFile.writelines("\t\t<column name=\"" + column + "\">\n")
			outFile.writelines("\t\t\t<type class =\"integer\"/>\n")
			outFile.writelines("\t\t</column>\n")
		outFile.writelines("\t\t<sites>" + sourceNodeStr + "</sites>\n")
		outFile.writelines("\t</stream>\n")
	outFile.writelines("</schema>\n")
	
	print "done"	
#generateRandomSchema({"InFlow" : "pressure", "OutFlow" : "pressure"}, "/cygdrive/c/ccc.txt", False, 100)

#compileQueryOptimizer()
#compileQuery([], None)
