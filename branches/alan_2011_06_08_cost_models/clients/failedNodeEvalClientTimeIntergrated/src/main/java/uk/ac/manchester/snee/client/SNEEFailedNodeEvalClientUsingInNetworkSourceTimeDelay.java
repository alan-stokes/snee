package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClient;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerException;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.planner.ChoiceAssessorPreferenceEnum;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay extends SNEEClient 
{
	private static ArrayList<String> applicableConfulenceSites = new ArrayList<String>();
  private static String sep = System.getProperty("file.separator");
	@SuppressWarnings("unused")
  private static final Logger resultsLogger = 
	  Logger.getLogger(SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay.class.getName());
	@SuppressWarnings("unused")
  private String sneeProperties;
	private static int queryid = 1;
	protected static int testNo = 1;
	
	//used to calculate agenda cycles
	protected static int maxNumberofFailures = 8;
	protected static ArrayList<String> currentlyFailedNodes = new ArrayList<String>(maxNumberofFailures);
	protected static double originalLifetime;
	protected static int numberOfExectutionCycles;
	protected static boolean calculated = false;
	protected static SensorNetworkQueryPlan originalQEP;
	protected static SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay client;
	
	private static final FailedNodeTimeClientUtils utils = new FailedNodeTimeClientUtils();
	
	public SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay(String query, 
			double duration, String queryParams, String csvFile, String sneeProperties) 
	throws SNEEException, IOException, SNEEConfigurationException {
		super(query, duration, queryParams, csvFile, sneeProperties);

		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEECostModelClientUsingInNetworkSource()");		
		this.sneeProperties = sneeProperties;
		if (logger.isDebugEnabled())
			logger.debug("RETURN SNEECostModelClientUsingInNetworkSource()");
	}

	/**
	 * The main entry point for the SNEE in-network client.
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) 
	{    
	  PropertyConfigurator.configure(
        SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay.class.
        getClassLoader().getResource("etc/common/log4j.properties"));   
	  
		Long duration = Long.valueOf("120");
		String queryParams = "etc/query-parameters.xml";
		BufferedWriter failedOutput; 
		Iterator<String> queryIterator;
		
		try
    {
      utils.checkRecoveryFile(queryid);

      runIxentsScripts();
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      collectQueries(queries);
      
	    queryIterator = queries.iterator();
	    failedOutput = utils.createFailedTestListWriter();
	    
	    while(queryIterator.hasNext())
	    {
	      recursiveRun(queryIterator, duration, queryParams, true, failedOutput);
      }
	    /*
	    queryIterator = queries.iterator();
	    while(queryIterator.hasNext())
      {
        recursiveRun(queryIterator, duration, queryParams, true, failedOutput);
        calculated = false;
      }
      */
	    failedOutput.write("\\end{document}");
	    failedOutput.flush();
	    failedOutput.close();  
    } 
		catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      System.out.println("error message was " + e.getMessage());
      try
      {
        utils.plotTopology(maxNumberofFailures);
      }
      catch (IOException e1)
      {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      logger.fatal(e);
      e.printStackTrace();
    }
	}

  private static void recursiveRun(Iterator<String> queryIterator, 
	                                 Long duration, String queryParams, 
	                                 boolean allowDeathOfAcquires, 
	                                 BufferedWriter failedOutput) 
  throws IOException, SNEEException, SNEEConfigurationException, 
  SNEECompilerException, EvaluatorException, MetadataException, 
  OptimizationException, SchemaMetadataException, TypeMappingException, 
  AgendaException, UnsupportedAttributeTypeException, SourceMetadataException, 
  TopologyReaderException, SNEEDataSourceException, CostParametersException,
  SNCBException, CodeGenerationException 
	{
	//get query & schemas
    String currentQuery = queryIterator.next();
    String propertiesPath = "tests/snee" + queryid + ".properties";
    
    System.out.println("Running Tests on query " + (queryid));
    client = new  SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay(currentQuery, 
                           duration, queryParams, null, propertiesPath);
    //set queryid to correct id
    SNEEController contol = (SNEEController) client.controller;
    contol.setQueryID(queryid);
    //added to allow recovery from crash
    utils.updateRecoveryFile(queryid);
    System.out.println("compiled control");
    client.runCompilelation();
    utils.plotOrginial(queryid, testNo);

    try
    {
      // run for global
      runGlobalTests(currentQuery, allowDeathOfAcquires);
    }
    catch(Exception e)
    {
      try
      {
        //run for partial
        runPartialTests(currentQuery, allowDeathOfAcquires);
      }
      catch (Exception e1)
      {
        System.out.println("system failed as: " + e1.getMessage());
        e1.printStackTrace();
        utils.plotTopology(maxNumberofFailures);
        queryid ++;
        utils.newPlotters(queryid);
        System.out.println("Ran all tests on query " + (queryid) + " going onto next topology");
        recursiveRun(queryIterator, duration, queryParams, allowDeathOfAcquires, failedOutput);
      }
      utils.plotTopology(maxNumberofFailures);
      queryid ++;
      utils.newPlotters(queryid); 
      recursiveRun(queryIterator, duration, queryParams, allowDeathOfAcquires, failedOutput);
    }
    try
    {
      // run partial
      runPartialTests(currentQuery, allowDeathOfAcquires);
    }
    catch(Exception e)
    {
      System.out.println("system failed as: " + e.getMessage());
      e.printStackTrace();
      utils.plotTopology(maxNumberofFailures);
      queryid ++;
      utils.newPlotters(queryid);
      System.out.println("Ran all tests on query " + (queryid) + " going onto next topology");
      recursiveRun(queryIterator, duration, queryParams, allowDeathOfAcquires, failedOutput);
    }
    utils.plotTopology(maxNumberofFailures);
    queryid ++;
    utils.newPlotters(queryid);
    System.out.println("Ran all tests on query " + (queryid) + " going onto next topology");
  }
  
  private static void runPartialTests(String currentQuery,
      boolean allowDeathOfAcquires)
  throws Exception
  {
    resetQEP(originalQEP);
    System.out.println("running tests for partial ");
    SNEEProperties.setSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE, ChoiceAssessorPreferenceEnum.Partial.toString());
    
    //run for partial 
    for(int currentNumberOfFailures = 1; currentNumberOfFailures <= maxNumberofFailures; currentNumberOfFailures++)
    {
      calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
      System.out.println("running tests with " + currentNumberOfFailures + " failures");
      double currentLifetime = 0;
      for(int currentFailure = 1; currentFailure <= currentNumberOfFailures; currentFailure++)
      {
        System.out.println("running with test no" + testNo);
        client.getQEP().getLAF().setQueryName("query" + queryid + "-" + maxNumberofFailures);
        client.runTests(client, currentQuery, queryid, allowDeathOfAcquires);
        currentLifetime = numberOfExectutionCycles * originalQEP.getAgendaIOT().getLength_bms(false) * currentNumberOfFailures;
        testNo++;
      }
      currentlyFailedNodes.clear();
      utils.storeAdaptation(queryid, testNo -1, currentLifetime, PlotterEnum.PARTIAL);
      resetQEP(originalQEP);
      client.resetMetaData(originalQEP);
    }
    
  }

  private static void runGlobalTests(String currentQuery, boolean allowDeathOfAcquires) 
  throws Exception
  {
    originalQEP = client.getQEP();
    System.out.println("running tests for global ");
    SNEEProperties.setSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE, ChoiceAssessorPreferenceEnum.Global.toString());
    testNo = 1;
    for(int currentNumberOfFailures = 1; currentNumberOfFailures <= maxNumberofFailures; currentNumberOfFailures++)
    {
      calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
      System.out.println("running tests with " + currentNumberOfFailures + " failures");
      double currentLifetime = 0;
      for(int currentFailure = 1; currentFailure <= currentNumberOfFailures; currentFailure++)
      {
        System.out.println("running with test no" + testNo);
        client.getQEP().getLAF().setQueryName("query" + queryid + "-"+ currentFailure + "-" + maxNumberofFailures);
        client.runTests(client, currentQuery, queryid, allowDeathOfAcquires);
        currentLifetime = numberOfExectutionCycles * originalQEP.getAgendaIOT().getLength_bms(false) * currentNumberOfFailures;
        testNo++;
      }
      currentlyFailedNodes.clear();
      utils.storeAdaptation(queryid, testNo -1, currentLifetime, PlotterEnum.GLOBAL);
      resetQEP(originalQEP);
      client.resetMetaData(originalQEP);
    }
    
  }

  private static void resetQEP(SensorNetworkQueryPlan qep) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException, 
  IOException, CodeGenerationException 
  {
    SNEEController control = (SNEEController) client.controller;
    control.resetQEP(qep);
  }
  

  private void resetMetaData(SensorNetworkQueryPlan qep) 
  throws SourceDoesNotExistException, SourceMetadataException,
  SNEEConfigurationException, SNCBException, TopologyReaderException
  {
    SNEEController control = (SNEEController) controller;
    control.resetMetaData(qep);
  }

  private static void calculateAgendaExecutionsBetweenFailures(int currentNumberOfFailures, SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay client)
  {
    if(!calculated)
    {
      originalLifetime = getOriginalLifetime();
      originalQEP = client.getQEP();
      calculated = true;
    }
    double timeBetweenFailures =  originalLifetime / (currentNumberOfFailures + 1); //s
    Long agendaLength = originalQEP.getAgendaIOT().getLength_bms(false) / 1024; // s
    numberOfExectutionCycles = (int) (timeBetweenFailures/agendaLength);
  }


  private static double getOriginalLifetime()
  {
    
    File inputFolder = new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + 
                                "OTASection" + sep + "storedObjects");
    ArrayList<Adaptation> orginalList = utils.readInObjects(inputFolder);
    Adaptation orginal = orginalList.get(0);
    return orginal.getLifetimeEstimate();
  }

  private void runCompilelation() 
  throws 
  SNEECompilerException, MalformedURLException, 
  EvaluatorException, SNEEException, MetadataException, 
  SNEEConfigurationException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, AgendaException, 
  UnsupportedAttributeTypeException, SourceMetadataException, 
  TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException, IOException, 
  CodeGenerationException 
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    SNEEController control = (SNEEController) controller;
    control.addQuery(_query, _queryParams, null, true, false, true);
    controller.close();
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }

  private static void collectQueries(ArrayList<String> queries) throws IOException
  {
    //String filePath = Utils.validateFileLocation("tests/queries.txt");
    File queriesFile = new File("src/main/resources/tests/queries.txt");
    String filePath = queriesFile.getAbsolutePath();
    BufferedReader queryReader = new BufferedReader(new FileReader(filePath));
    String line = "";
    while((line = queryReader.readLine()) != null)
    {
      queries.add(line);
    }  
  }

  private static void runIxentsScripts() throws IOException
  {
    
     //run Ixent's modified script to produce random test cases. 
     //if tests exist, do not redo
     File pythonFolder = new File("src/main/resources/python/");
     String pythonPath = pythonFolder.getAbsolutePath();
     File testFolder = new File("src/main/resources/tests");
     if(!testFolder.exists())
       testFolder.mkdir();
     
     if(testFolder.list().length  == 1 || testFolder.list().length  == 0)
     {
       String testPath = testFolder.getAbsolutePath();
       System.out.println(testPath);
       String [] params = {"generateScenarios.py", testPath};
       Map<String,String> enviro = new HashMap<String, String>();
       System.out.println("running Ixent's scripts for 30 random queries");
       Utils.runExternalProgram("python", params, enviro, pythonPath);
       System.out.println("have ran Ixent's scripts for 30 random queries");
       
     }
     else
     {
       System.out.println("System already has test cases, will not re-execute process");
     }
  }

  private void runTests(SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay client, String currentQuery, 
                        int queryid, boolean allowDeathOfAcquires) 
  throws Exception
  {
  	updateSites(allowDeathOfAcquires);
  	if(applicableConfulenceSites.size() != 0)
  	{
  		String deadNode = chooseNodes();
  		controller.simulateEnergyDrainofAganedaExecutionCycles(numberOfExectutionCycles);
  		client.runForTests(deadNode, queryid); 
  		utils.updateRecoveryFile(queryid);
      System.gc();
      updateSites(allowDeathOfAcquires);  
  	}
  	else
  	{
  	  System.out.println("were no avilable nodes to fail, will not run test");
  	}
  	System.out.println("Stopping current query");
  	controller.close();
  }

  /**
   * goes though routing tree, looking for nodes which are confluence sites which are sites
   *  which will cause likely changes to results when lost counting them if they have acquire operators
   *  if the allowDeathOfAcquires is set
   * @param allowDeathOfAcquires 
   * @param routingTree2
   * @throws SourceDoesNotExistException 
   */
  private void updateSites(boolean allowDeathOfAcquires) 
  throws SourceDoesNotExistException
  {
    SensorNetworkQueryPlan qep = this.getQEP();
    RT routingTree = qep.getRT();
    Iterator<Site> siteIterator = routingTree.siteIterator(TraversalOrder.POST_ORDER);
    applicableConfulenceSites.clear();
    SNEEController snee = (SNEEController) controller;
    SourceMetadataAbstract metadata = snee.getMetaData().getSource(qep.getMetaData().getOutputAttributes().get(1).getExtentName());
    SensorNetworkSourceMetadata sensornetworkMetadata = (SensorNetworkSourceMetadata) metadata;
    int[] sources = sensornetworkMetadata.getSourceSites(qep.getDAF().getPAF());
    String sinkID = qep.getRT().getRoot().getID();
    
    while(siteIterator.hasNext())
    {
      Site currentSite = siteIterator.next();
      if((allowDeathOfAcquires ||  (!allowDeathOfAcquires && !isSource(currentSite, sources))) &&
          currentSite.getInDegree() > 1 && 
         !applicableConfulenceSites.contains(Integer.parseInt(currentSite.getID())) &&
         !currentSite.getID().equals(sinkID)
        )
          applicableConfulenceSites.add(currentSite.getID());
    }
    if(applicableConfulenceSites.size() == 0)
    {
      siteIterator = qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
      while(siteIterator.hasNext())
      {
        Site site = siteIterator.next();
        if(!site.isSource())
          applicableConfulenceSites.add(site.getID());
      }
    }
    if(applicableConfulenceSites.size() == 0)
    {
      siteIterator = qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
      while(siteIterator.hasNext())
      {
        Site site = siteIterator.next();
        if(!qep.getRT().getRoot().getID().equals(site.getID()))
        {
          applicableConfulenceSites.add(site.getID());
        }
      }
    } 
  }

  private boolean isSource(Site currentSite, int[] sources)
  {
    String siteIDs = currentSite.getID();
    int siteID = Integer.parseInt(siteIDs);
    boolean found = false;
    
    for(int index = 0; index < sources.length; index++)
    {
      if(sources[index] == siteID)
        found = true;
    }
    return found;
  }

  /**
   * selects a node from an array of options
   * @throws Exception 
   */
  private static String chooseNodes() throws Exception 
  {
	  int size = applicableConfulenceSites.size();
	  if(size == 0)
	    throw new Exception("no more avilable nodes to fail");
	  if(size == 1)
	    return applicableConfulenceSites.get(0);
	  else
	  {
  	  Random random = new Random();
  	  return applicableConfulenceSites.get(random.nextInt(size));
	  }
  }

  /**
   * runs the test
   * @param failedNodes
   * @param queryid
   * @throws Exception 
   */
  public void runForTests(String failedNode, int queryid)
  throws Exception
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    System.out.println("Failed node [" + failedNode + "] ");
    SNEEController control = (SNEEController) controller;
    control.giveAutonomicManagerQuery(_query);
    boolean sucessful = runSimulatedNodeFailure(failedNode, control);
    if(!sucessful)
      throw new AutonomicManagerException("couldnt adapt with any node failure");
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }

  private boolean runSimulatedNodeFailure(String failedNode,
      SNEEController control) 
  throws Exception
  {
    ArrayList<String> currentNodeFailures = new ArrayList<String>();
    currentlyFailedNodes.add(failedNode);
    currentNodeFailures.add(failedNode);
    try
    {
      control.runSimulatedNodeFailure(currentNodeFailures);
      return true;
    }
    catch(Exception e)
    { 
      System.out.println("system failed as " + e.getMessage());
      e.printStackTrace();
      currentNodeFailures.clear();
      currentlyFailedNodes.remove(failedNode);
      applicableConfulenceSites.remove(failedNode);
      String deadNode = chooseNodes();
      QueryExecutionPlan lastPlan = control.getQEP();
      resetMetaData(originalQEP);
      updateMetaDataBackToCurrentState(control);
      resetQEP((SensorNetworkQueryPlan) lastPlan);
      System.out.println("system failed to adapt with node " + failedNode + " so will try node " + deadNode);
      runSimulatedNodeFailure(deadNode, control);
      return true;
    }
  }

  private void updateMetaDataBackToCurrentState(SNEEController control) 
  throws SourceDoesNotExistException
  {
    Iterator<String> failedNodesIDsIterator =  currentlyFailedNodes.iterator();
    while(failedNodesIDsIterator.hasNext())
    {
      String failedID = failedNodesIDsIterator.next();
      control.removeNodeFromTheMetaData(failedID, originalQEP);
    }
    
  }
}
