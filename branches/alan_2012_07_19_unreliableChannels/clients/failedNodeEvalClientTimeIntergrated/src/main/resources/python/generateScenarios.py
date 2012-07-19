#!/usr/bin/python
#Generates scenarios for QoS-awareness experiments
#Scenarios 1 (exp1,2): random <query, schema, network> 30 times
#Scenarios 2 (exp3): fixed query, schema min/maj, queries with different densities
#NB: outputs dirs assumed to exist
#TODO: Move query generator out to a separate library
import os, random, RandomSeeder, math, networkLib, SneeqlLib, sys, getopt, UtilLib

optNumNodes = 15
optScenariosFile = 'scenarios.csv'

#Scenarios 1
optNumScenarios = 30
optOutputDir1 = os.getenv('HOME')+os.sep+"tmp"+os.sep+"results"+os.sep+"scenarios1"

#Scenarios 2
optPercentSources = [6,25]
optRValues = [1, 6, 30]
optNumNetworksPerNetworkType = 5
optOutputDir2 = os.getenv('HOME')+os.sep+"tmp"+os.sep+"results"+os.sep+"scenarios2"

#Query generation
optMaxQueryNesting = 1
optMaxSourcesPerQueryLevel = 1
optProbabilitySubquery = 1 #prob that each source in a query level is a sub-query
optProbabilityAggregate = 1 #prob that aggregate is projected in query level
topologyDensity = 6

#network generation
optRadioRange = 60

def parseArgs(args):	
	global optOutputDir1, optOutputDir2
	try:
		optNames = []
	
		#append the result of getOpNames to all the libraries 
		optNames += SneeqlLib.getOptNames();
		optNames = UtilLib.removeDuplicates(optNames)
		
		opts, args = getopt.getopt(args, "h",optNames)
	except getopt.GetoptError, err:
		#print str(err)
		usage()
		sys.exit(2)
			
	SneeqlLib.setOpts(opts)
	if SneeqlLib.optCygwin:
		optOutputDir1 = "c:/ixent/tmp/results/scenarios1"
		optOutputDir2 = "c:/ixent/tmp/results/scenarios2"

def getNextQueryLevel(nextExtent, nextSubQuery, topLevel, levelQualifier, extentsUsed, indentLevel):

    if optMaxSourcesPerQueryLevel == 1 or optMaxSourcesPerQueryLevel == 2:
        numSources = optMaxSourcesPerQueryLevel
    else :
        numSources = random.randrange(2, optMaxSourcesPerQueryLevel, 1)
    selectClause = "SELECT "
    fromClause = " FROM "
    whereClause = " " 

    qualifier = None
    prevQualifier = None
    attribute = None
    prevAttribute = None

    for i in range(0, numSources):
        if (i>0):
            fromClause += ", "

        if (random.random() <= optProbabilitySubquery or indentLevel >= optMaxQueryNesting):
            qualifier = "%snow" % (chr(nextExtent).lower())
            attribute = 'x'
            fromClause += "%s[NOW] %s" % (chr(nextExtent), qualifier)
            extentsUsed.extend(chr(nextExtent))
            nextExtent += 1
        else:
            qualifier = "sq%d" % (nextSubQuery)
            attribute = "%sx" % (qualifier)
            nextSubQuery += 1
            (subQuery, nextExtent, nextSubQuery, extentsUsed) = getNextQueryLevel(nextExtent, nextSubQuery, False, qualifier, extentsUsed, (indentLevel+1))
            fromClause += "(%s) %s" % (subQuery, qualifier) 
        if i==0:
            if topLevel:
                selectClause += "RSTREAM "
            if (random.random() > optProbabilityAggregate):
                selectClause += "AVG(%s.%s) as %sx" % (qualifier, attribute, levelQualifier)
            else:
                selectClause += "%s.%s as %sx" % (qualifier, attribute, levelQualifier)
        if i==1:
            whereClause = " WHERE " 
        if i>1:
            whereClause += " AND " 
        if i>0:
            
            whereClause += "%s.%s=%s.%s" % (prevQualifier, prevAttribute, qualifier, attribute)
        prevQualifier = qualifier
        prevAttribute = attribute

    semiColon=""
    if topLevel:
        semiColon=";"
    levelQuery = "%s%s%s%s" % (selectClause, fromClause, whereClause, semiColon)
    return (levelQuery, nextExtent, nextSubQuery, extentsUsed)

#Returns a list of the extents used in the query to aid schema generation
def generateRandomQuery(queryId = 'q1', outputDir = '.'):
    nextExtent = ord('A')
    nextSubQuery = 1
    topLevel = True
    extentsUsed = []

   # print ("optProbabilityAggregate = %f" %(optProbabilityAggregate))
   # print ("optProbabilitySubquery = %f" %(optProbabilitySubquery))
   # print ("optMaxSourcesPerQueryLevel = %f" %(optMaxSourcesPerQueryLevel))
   # print ("optMaxQueryNesting = %f" %(optMaxQueryNesting))
    (query, nextExtent, nextSubQuery, extentsUsed)  = getNextQueryLevel(nextExtent, nextSubQuery, True, "q", extentsUsed,0)

    outFile = open(outputDir+os.sep+"queries"+".txt", 'a')
    outFile.writelines(query + "\n")
    return extentsUsed

def generatePhysicalSchema(numNodes, percentSources, extentList, schemaId, outputDir, networkId):
    numSources = int(math.ceil(float(numNodes)*(percentSources/100.0)))
    if numSources > 7:
        numSources = 9
    nodes = range(0,numNodes)
    sourceNodes = random.sample(nodes, numSources)
    numExtents = len(extentList)

    schemaStr = """<?xml version="1.0" encoding="UTF-8"?>
<source xmlns="http://snee.cs.man.ac.uk/namespace/physical-schema"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://snee.cs.man.ac.uk/namespace/physical-schema ../schema/physical-schema.xsd ">

	<sensor_network name="wsn1">
		<topology>tests/%s.xml</topology>
		<site-resources>etc/common/tmotesky-site-resources.xml</site-resources>
		<gateways>0</gateways>
		<extents>
""" % (networkId)

#    print "numSources="+str(numSources)
#    print "numExtents="+str(numExtents)

    step = float(numSources)/float(numExtents)
    s = 0.0
    e = 0
    while (s<numSources and e<numExtents):
        start = int(s)
        end = max(start + 1, int(s + step) + 1)
        extent = extentList[e]
        extentSources = sourceNodes[start:end]

	schemaStr += """\t\t\t<extent name="%s">
\t\t\t\t<sites>%s</sites>
\t\t\t</extent>
""" % (extent, ",".join(map(str,extentSources)))
        s += step
        e += 1

    schemaStr += """\t\t</extents> \n"""
    schemaStr += """\t</sensor_network> \n"""
    schemaStr += "</source>\n"
 
    outFile = open(outputDir+os.sep+schemaId+".xml", 'w')
    outFile.writelines(schemaStr)
    outFile.close()


def getCandidateNode(nodes, rValue, id):
        randomNode = random.sample(nodes, 1)[0]
        angle = random.randrange(0,360,1)
        dx = math.floor(math.cos(angle)*(optRadioRange/float(rValue)))
        dy = math.floor(math.sin(angle)*(optRadioRange/float(rValue)))
        n = networkLib.Node(id, int(randomNode.xPos + dx), int(randomNode.yPos + dy))
        return n

def generateSpiderNetwork(numNodes, rValue, networkId, outputDir):

    minx = 0
    miny = 0
    maxx = 0
    maxy = 0

    sink = networkLib.Node(0, 0, 0)
    nodes = [sink]
    dupsCheckList = ["0_0"]

    for i in range(0, numNodes):
        success = False
        while success==False:
            n = getCandidateNode(nodes, rValue, i)
            key = "%d_%d" % (n.xPos, n.yPos)
            if not (key in dupsCheckList):
#                print dupsCheckList
                success = True
                dupsCheckList += [key]
           #     print "x=%d,y=%d" % (n.xPos, n.yPos)
                minx = min(minx, n.xPos)
                miny = min(miny, n.yPos)
                maxx = max(maxx, n.xPos)
                maxy = max(maxy, n.yPos)
                nodes += [n]

    xDim = maxx - minx
    yDim = maxy - miny
  #  print "xDim = %d, yDim = %d" %(xDim, yDim)
        
    f = networkLib.Field(xDim, yDim, minx, miny)
    for n in nodes:
        f.addNode(n.id, n.xPos, n.yPos)

    f.trimEdgesRandomlyToMeetAverageDegree(topologyDensity) #TODO: unhardcode this	
    f.generateSneeqlNetFile(outputDir+os.sep+networkId+".xml")
    f.generateTopFile(outputDir+os.sep+networkId+".top")
    f.generateTopDotFile(outputDir+os.sep+networkId+".dot")

def generateLogicalSchema(extentList, logicalSchemaId, outputDir):
    numExtents = len(extentList)
    schemaStr = """<?xml version="1.0" encoding="UTF-8"?> 

<schema xmlns="http://snee.cs.man.ac.uk/namespace/logical-schema" 
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://snee.cs.man.ac.uk/namespace/logical-schema ../schema/logical-schema.xsd">

"""
    e =0
    while (e<numExtents):
	extent = extentList[e]
	schemaStr += """\t<stream name="%s" type="pull">
\t\t<column name="x">
\t\t\t<type class ="integer"/>
\t\t</column>
\t</stream> \n""" %(extent)
	e += 1

    schemaStr += "\n</schema>\n"

    outFile = open(outputDir+os.sep+logicalSchemaId+".xml", 'w')
    outFile.writelines(schemaStr)
    outFile.close()
    
def generateSneeProperties(logicalSchemaId, physicalSchemaId, i, outputDir):
    filepath = outputDir+os.sep+"snee%d.properties" % (i)
    outFile = open(filepath, 'w')
    schemaStr= """
# Determines whether graphs are to be generated.
compiler.generate_graphs = true

# Instructs query operator tree graphs to show operator output type 
compiler.debug.show_operator_tuple_type = true

compiler.convert_graphs = true
graphviz.exe = /usr/local/bin/dot

# Purge old output files at the start of every run
compiler.delete_old_files = true

# the root directory for the compiler outputs
compiler.output_root_dir = output

# Using equivalence-preserving transformation removes unrequired
# operators (e.g., a NOW window combined with a RSTREAM)
# TODO: currently in physical rewriter, move this to logical rewriter
# TODO: consider removing this option
# FIXME: Should not be a required property
compiler.logicalrewriter.remove_unrequired_operators = true

# Pushes project operators as close to the leaves of the operator
# tree as possible.
# TODO: currently in physical rewriter, move this to logical rewriter
# TODO: consider removing this option
# FIXME: Should not be a required property
compiler.logicalrewriter.push_project_down = true

# Combines leaf operators (receive, acquire, scan) and select operators
# into a single operator
# NB: In Old SNEE in the translation/rewriting step
# TODO: Only works for acquire operators at the moment
# TODO: consider removing this option
# FIXME: Should not be a required property
compiler.logicalrewriter.combine_leaf_and_select = true

# Sets the random seed used for generating routing trees
# FIXME: Should not be a required property
compiler.router.random_seed = 4

# Removes unnecessary exchange operators from the DAF
# FIXME: Should not be a required property
compiler.where_sched.remove_redundant_exchanges = false

# Instructs where-scheduler to decrease buffering factor
# to enable a shorter acquisition interval.
# FIXME: Should not be a required property
compiler.when_sched.decrease_beta_for_valid_alpha = true

#Specifies whether agendas generated should allow sensing to have interruptions
#Use this option to enable high acquisition intervals
compiler.allow_discontinuous_sensing = true

# Location of the logical schema
logical_schema = tests/%s.xml

# Location of the physical schema
physical_schema = tests/%s.xml

# Location of the cost parameters file
# TODO: This should be moved to physical schema, as there  is potentially
# one set of cost parameters per source.
# FIXME: Should not be a required property
cost_parameters_file = etc/common/cost-parameters.xml

# The name of the file with the types
types_file = etc/common/Types.xml

# The name of the file with the user unit definitions
units_file = etc/common/units.xml

# Specifies whether individual images or a single image is sent to WSN nodes.
sncb.generate_combined_image = false

# Specifies whether the metadata collection program should be invoked, 
# or default metadata should be used.
sncb.perform_metadata_collection = false

# Specifies whether the command server should be included with SNEE query plan
# Only compatible with Tmote Sky TinyOS2 code generation target
sncb.include_command_server = true

#Specifies which stragety levels to run.
# Currently FL, FP, FG, ALL
wsn_manager.strategies = FailedNodePartial

# Specifies code generation target
# telosb_t2 generates TinyOS v2 code for TelosB or TmoteSky hardware
# avrora_mica2_t2 generates TinyOS v2 code for Avrora simulator emulating mica2 hardware
# avrora_micaz_t2 generates TinyOS v2 code for Avrora simulator emulating micaz hardware
# tossim_t2 generates TinyOS v2 code for Tossim simulator
sncb.code_generation_target = avrora_mica2_t2
#sncb.code_generation_target = telosb_t2

TIMESTAMP_FORMAT = yyyy-MM-dd HH:mm:ss.SSS Z
WEBROWSET_FORMAT = http://java.sun.com/xml/ns/jdbc

# Size of history, in tuples, maintained per stream
results.history_size.tuples = 1000""" %(logicalSchemaId, physicalSchemaId)

    outFile.writelines(schemaStr)
    outFile.close()

#30 random select * queries/networks/schemas
def generateScenarios1(numScenarios, numNodes, outputDir, scenariosFile, counter, counterMax):
    
    global optProbabilityAggregate 
    optProbabilityAggregate = 1
    global optProbabilitySubquery 
    optProbabilitySubquery = 1
    global optMaxSourcesPerQueryLevel 
    optMaxSourcesPerQueryLevel = 1
    global optMaxQueryNesting 
    optMaxQueryNesting = 1
    
    outFile = open(outputDir+os.sep+scenariosFile, 'w')
    outFile.writelines("scenarioId,queryId,networkId,numNodes,rValue,schemaId,percentSources\n")

    i = 0
    for i in range(counter, counterMax):
        percentSources = random.randrange(30, 100, 1)
        rValue = random.randrange(1, 5, 1)

        queryId = 'q%d' % (i)
	logicalSchemaId = 'logicalSchema_query%d' % (i)
        physicalSchemaId = 'physicalSchema_query%d' % (i)
        networkId = 'wsn_query%d' % (i)
    
        extentList = generateRandomQuery(queryId, outputDir)
	generateSpiderNetwork(numNodes, rValue, networkId, outputDir)
        generatePhysicalSchema(numNodes, percentSources, extentList, physicalSchemaId, outputDir, networkId)
	generateLogicalSchema(extentList, logicalSchemaId, outputDir)
	generateSneeProperties(logicalSchemaId, physicalSchemaId, i, outputDir)
    
        scenarioStr = "%d,%s,%s,%s,%s,%s,%s\n" % (i, queryId, networkId, numNodes, rValue, physicalSchemaId, percentSources)
        outFile.writelines(scenarioStr)
    outFile.close()

#30 random select avg queries/networks/schemas
def generateScenarios2(numScenarios, numNodes, outputDir, scenariosFile, counter, counterMax):

    global optProbabilityAggregate 
    optProbabilityAggregate = 0.01

    outFile = open(outputDir+os.sep+scenariosFile, 'w')
    outFile.writelines("scenarioId,queryId,networkId,numNodes,rValue,schemaId,percentSources\n")

    for i in range(counter, counterMax):
        percentSources = random.randrange(30, 100, 1)
        rValue = random.randrange(1, 5, 1)

        queryId = 'q%d' % (i)
	logicalSchemaId = 'logicalSchema_query%d' % (i)
        physicalSchemaId = 'physicalSchema_query%d' % (i)
        networkId = 'wsn_query%d' % (i)
    
        extentList = generateRandomQuery(queryId, outputDir)
	generateSpiderNetwork(numNodes, rValue, networkId, outputDir)
        generatePhysicalSchema(numNodes, percentSources, extentList, physicalSchemaId, outputDir, networkId)
	generateLogicalSchema(extentList, logicalSchemaId, outputDir)
	generateSneeProperties(logicalSchemaId, physicalSchemaId, i, outputDir)
    
        scenarioStr = "%d,%s,%s,%s,%s,%s,%s\n" % (i, queryId, networkId, numNodes, rValue, physicalSchemaId, percentSources)
        outFile.writelines(scenarioStr)
    outFile.close()

#30 random select join queries/networks/schemas
def generateScenarios3(numScenarios, numNodes, outputDir, scenariosFile, counter, counterMax):

    global optProbabilityAggregate 
    optProbabilityAggregate = 1
    global optProbabilitySubquery 
    optProbabilitySubquery = 0.5
    global optMaxSourcesPerQueryLevel 
    optMaxSourcesPerQueryLevel = 2
    global optMaxQueryNesting 
    optMaxQueryNesting = 1

    outFile = open(outputDir+os.sep+scenariosFile, 'w')
    outFile.writelines("scenarioId,queryId,networkId,numNodes,rValue,schemaId,percentSources\n")

    for i in range(counter, counterMax):
        percentSources = random.randrange(30, 100, 1)
        rValue = random.randrange(1, 5, 1)

        queryId = 'q%d' % (i)
	logicalSchemaId = 'logicalSchema_query%d' % (i)
        physicalSchemaId = 'physicalSchema_query%d' % (i)
        networkId = 'wsn_query%d' % (i)
    
        extentList = generateRandomQuery(queryId, outputDir)
	generateSpiderNetwork(numNodes, rValue, networkId, outputDir)
        generatePhysicalSchema(numNodes, percentSources, extentList, physicalSchemaId, outputDir, networkId)
	generateLogicalSchema(extentList, logicalSchemaId, outputDir)
	generateSneeProperties(logicalSchemaId, physicalSchemaId, i, outputDir)
    
        scenarioStr = "%d,%s,%s,%s,%s,%s,%s\n" % (i, queryId, networkId, numNodes, rValue, physicalSchemaId, percentSources)
        outFile.writelines(scenarioStr)
    outFile.close()

#30 random select agg join queries/networks/schemas
def generateScenarios4(numScenarios, numNodes, outputDir, scenariosFile, counter, counterMax):

    global optProbabilityAggregate 
    optProbabilityAggregate = 0.5
    global optProbabilitySubquery 
    optProbabilitySubquery = 0.5
    global optMaxSourcesPerQueryLevel 
    optMaxSourcesPerQueryLevel = 2
    global optMaxQueryNesting 
    optMaxQueryNesting = 2

    outFile = open(outputDir+os.sep+scenariosFile, 'w')
    outFile.writelines("scenarioId,queryId,networkId,numNodes,rValue,schemaId,percentSources\n")

    for i in range(counter, counterMax):
        percentSources = random.randrange(30, 100, 1)
        rValue = random.randrange(1, 5, 1)

        queryId = 'q%d' % (i)
	logicalSchemaId = 'logicalSchema_query%d' % (i)
        physicalSchemaId = 'physicalSchema_query%d' % (i)
        networkId = 'wsn_query%d' % (i)
    
        extentList = generateRandomQuery(queryId, outputDir)
	generateSpiderNetwork(numNodes, rValue, networkId, outputDir)
        generatePhysicalSchema(numNodes, percentSources, extentList, physicalSchemaId, outputDir, networkId)
	generateLogicalSchema(extentList, logicalSchemaId, outputDir)
	generateSneeProperties(logicalSchemaId, physicalSchemaId, i, outputDir)
    
        scenarioStr = "%d,%s,%s,%s,%s,%s,%s\n" % (i, queryId, networkId, numNodes, rValue, physicalSchemaId, percentSources)
        outFile.writelines(scenarioStr)
    outFile.close()


def main():
    print("starting script \n")
    #parse the command-line arguments
    parseArgs(sys.argv[2:])
    optOutputDir1 = sys.argv[1]
    random.seed(1)
    
    if os.path.exists(optOutputDir1+os.sep+"queries"+".txt"):
	  os.remove(optOutputDir1+os.sep+"queries"+".txt")
    #dense topology
    generateScenarios1(optNumScenarios, optNumNodes, optOutputDir1, optScenariosFile, 1, 30)
    generateScenarios2(optNumScenarios, optNumNodes, optOutputDir1, optScenariosFile, 30, 60)
    generateScenarios3(optNumScenarios, optNumNodes, optOutputDir1, optScenariosFile, 60, 90)
    generateScenarios4(optNumScenarios, optNumNodes, optOutputDir1, optScenariosFile, 90, 120)
    #not so dense network
    global topologyDensity
    topologyDensity = 3
    generateScenarios1(optNumScenarios, optNumNodes, optOutputDir1, optScenariosFile, 120, 150)
    generateScenarios2(optNumScenarios, optNumNodes, optOutputDir1, optScenariosFile, 150, 180)
    generateScenarios3(optNumScenarios, optNumNodes, optOutputDir1, optScenariosFile, 180, 210)
    generateScenarios4(optNumScenarios, optNumNodes, optOutputDir1, optScenariosFile, 210, 240)
    
    print("finished script \n")

if __name__ == "__main__":
	main()
