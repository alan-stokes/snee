package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
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
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyEnum;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LogicalOverlayNetworkImpl;
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
  private static String sep = System.getProperty("file.separator");
	@SuppressWarnings("unused")
  private static final Logger resultsLogger = 
	  Logger.getLogger(SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay.class.getName());
	@SuppressWarnings("unused")
  private String sneeProperties;
	private static int queryid = 1;
	protected static int testNo = 1;
	//private static String dictonary = "testsSize30";
  //private static String dictonary = "testsSize100";
	private static String dictonary = "testsNatural30";
	
	//used to calculate agenda cycles
	protected static int maxNumberofFailures = 8;
	private static ArrayList<String> applicableConfulenceSites = new ArrayList<String>();
  protected static ArrayList<String> currentlyFailedNodes = new ArrayList<String>(maxNumberofFailures);
  protected static ArrayList<String> currentlyFailedFailedNodes = new ArrayList<String>();
  protected static ArrayList<Adaptation> currentSuccessfulAdaptations = new ArrayList<Adaptation>();
	protected static double originalLifetime;
	protected static int numberOfExectutionCycles;
	protected static boolean calculated = false;
	protected static SensorNetworkQueryPlan originalQEP;
	protected static SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay client;
	protected static LogicalOverlayNetworkImpl orginialOverlay;
	 protected static SensorNetworkQueryPlan lastPlan;
	
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
	      calculated = false;
	      queryid++;
      }
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
  SNCBException, CodeGenerationException, NumberFormatException, WhenSchedulerException 
	{
	//get query & schemas
    String currentQuery = queryIterator.next();
    String propertiesPath = dictonary + "/snee" + queryid + ".properties";
    
    System.out.println("Running Tests on query " + (queryid));
    client = new  SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay(currentQuery, 
                           duration, queryParams, null, propertiesPath);
    //set queryid to correct id
    SNEEController contol = (SNEEController) client.controller;
    contol.setQueryID(queryid);
    //added to allow recovery from crash
    utils.updateRecoveryFile(queryid);
    System.out.println("compiled control");
    
    SNEEProperties.setSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE, ChoiceAssessorPreferenceEnum.Global.toString());
    client.runCompilelation();
    utils.plotOrginial(queryid, testNo);
    
    ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + 
        "OTASection" + sep + "storedObjects"));
    currentSuccessfulAdaptations.add(adapts.get(0));
    
    int position = 0;
    runSeveralTests(position, currentQuery, allowDeathOfAcquires, queryIterator, duration, failedOutput);
  }
  
  private static void runSeveralTests(int position, String currentQuery,
                                      boolean allowDeathOfAcquires,
                                      Iterator<String> queryIterator,
                                      Long duration, BufferedWriter failedOutput)
  throws IOException, NumberFormatException, SNEEException, 
  SNEEConfigurationException, SNEECompilerException, EvaluatorException, 
  MetadataException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, AgendaException, UnsupportedAttributeTypeException,
  SourceMetadataException, TopologyReaderException, SNEEDataSourceException,
  CostParametersException, SNCBException, CodeGenerationException, WhenSchedulerException
  {
    try
    {
      if(position == 0)
      {
        runOriginalTests(currentQuery, allowDeathOfAcquires);
        System.out.println("finished running original");
        runSeveralTests(position + 1, currentQuery, 
                        allowDeathOfAcquires, queryIterator, 
                        duration, failedOutput);
      }
      if(position == 1)
      {
        runGlobalTests(currentQuery, allowDeathOfAcquires);
        System.out.println("finished running global");
        runSeveralTests(position + 1, currentQuery,
                        allowDeathOfAcquires, queryIterator, 
                        duration, failedOutput);
      }
      else if(position == 2)
      {
        runPartialTests(currentQuery, allowDeathOfAcquires);
        System.out.println("finished running partial");
        runSeveralTests(position + 1, currentQuery,
                        allowDeathOfAcquires, queryIterator, 
                        duration, failedOutput);
      }
      else if(position == 3)
      {
        runOverlayTests(currentQuery, allowDeathOfAcquires);
        System.out.println("finished running overlay");
        runSeveralTests(position + 1, currentQuery, 
                        allowDeathOfAcquires, queryIterator, 
                        duration, failedOutput);
      }
      else if(position == 4)
      {
        runBestTests(currentQuery, allowDeathOfAcquires);
        System.out.println("finished running best");
        runSeveralTests(position + 1, currentQuery, 
                        allowDeathOfAcquires, queryIterator, 
                        duration, failedOutput);
      }    
    }
    catch(Exception e)
    {
      if(position + 1 == 5)
      {
        System.out.println("system failed as: " + e.getMessage());
        e.printStackTrace();
        utils.plotTopology(maxNumberofFailures);
        queryid ++;
        utils.newPlotters(queryid);
        System.out.println("Ran all tests on query " + (queryid) + " going onto next topology");
        recursiveRun(null, null, currentQuery, allowDeathOfAcquires, failedOutput);
        
      }
      else
        runSeveralTests(position + 1, currentQuery, 
                        allowDeathOfAcquires, queryIterator, 
                        duration, failedOutput);
    }
  }

  private static void runOriginalTests(String currentQuery, boolean allowDeathOfAcquires)
  throws Exception
  {
    //client.resetDataSources(originalQEP);
    System.out.println("running tests for orgiinal ");
    utils.setupOriginal(queryid);
    SNEEController control = (SNEEController) client.controller;
    client.resetDataSources((SensorNetworkQueryPlan) control.getQEP());
    Double leftOverLifetime = control.getEstimatedLifetime((SensorNetworkQueryPlan) control.getQEP(), new ArrayList<String>());
    SensorNetworkQueryPlan currentQEP = (SensorNetworkQueryPlan) control.getQEP();
    Double agendas = leftOverLifetime / (currentQEP.getAgendaIOT().getLength_bms(false)/ 1024);
    System.out.println("originalQEP : " + agendas);
    client.resetDataSources((SensorNetworkQueryPlan) control.getQEP());
    //run for orgiinal 
    for(int currentNumberOfFailures = 1; currentNumberOfFailures <= maxNumberofFailures; currentNumberOfFailures++)
    {
      calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
      System.out.println("running tests with " + currentNumberOfFailures + " failures");
      ArrayList<String> fails = new ArrayList<String>();
      Double lifetime = 0.0;
      for(int currentFailure = 1; currentFailure <= currentNumberOfFailures; currentFailure++)
      {
        System.out.println("running with test no" + testNo);
        client.getQEP().getLAF().setQueryName("query" + queryid + "-" + maxNumberofFailures);
        lifetime = client.runOrginialTests(currentQuery, allowDeathOfAcquires, fails);
        testNo++;
      }
      currentlyFailedNodes.clear();
      double currentLifetime = numberOfExectutionCycles * (originalQEP.getAgendaIOT().getLength_bms(false) / 1024) * currentNumberOfFailures ;
      lifetime = currentLifetime + lifetime; // seconds
      lifetime = lifetime / (originalQEP.getAgendaIOT().getLength_bms(false) / 1024);  // agendas
      utils.storeoriginal(queryid, currentNumberOfFailures, lifetime, fails);
      System.out.println("orginal :- " + lifetime);
      utils.writeOriginal(currentNumberOfFailures, lifetime);
      utils.plotTopology(testNo -1);
      
      client.resetDataSources(originalQEP);
      ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + 
          "OTASection" + sep + "storedObjects"));
      currentSuccessfulAdaptations.add(adapts.get(0));
    }
    utils.closeOriginal();
    
  }

  private double runOrginialTests( String currentQuery, boolean allowDeathOfAcquires,
                                 ArrayList<String> fails)
  throws Exception
  {
    updateSites(allowDeathOfAcquires);
    if(applicableConfulenceSites.size() != 0)
    {
      String deadNode = chooseNodes();
      fails.add(deadNode);
      controller.simulateEnergyDrainofAganedaExecutionCycles(numberOfExectutionCycles);
      SNEEController control = (SNEEController) controller;
      originalQEP = (SensorNetworkQueryPlan) control.getQEP();
      Double leftOverLifetime = control.getEstimatedLifetime(originalQEP, fails);
     // Double agendas = leftOverLifetime / (originalQEP.getAgendaIOT().getLength_bms(false)/ 1024);
      utils.updateRecoveryFile(queryid);
      System.gc();
      updateSites(allowDeathOfAcquires);  
      System.out.println("Stopping current query");
      controller.close();
      return leftOverLifetime;
    }
    else
    {
      System.out.println("were no avilable nodes to fail, will not run test");
      System.out.println("Stopping current query");
      controller.close();
      return 0.0;
    }
  }

  /**
   * does the partial tests
   * @param currentQuery
   * @param allowDeathOfAcquires
   * @throws Exception
   */
  private static void runPartialTests(String currentQuery, boolean allowDeathOfAcquires)
  throws Exception
  {
    try{
    //client.resetDataSources(originalQEP);
    originalQEP = client.getQEP();
    System.out.println("running tests for partial ");
    SNEEProperties.setSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE, ChoiceAssessorPreferenceEnum.Partial.toString());
    
    //run for partial 
    for(int currentNumberOfFailures = 1; currentNumberOfFailures <= maxNumberofFailures; currentNumberOfFailures++)
    {
      calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
      System.out.println("running tests with " + currentNumberOfFailures + " failures");
      ArrayList<String> fails = new ArrayList<String>();
      for(int currentFailure = 1; currentFailure <= currentNumberOfFailures; currentFailure++)
      {
        System.out.println("running with test no" + testNo);
        client.getQEP().getLAF().setQueryName("query" + queryid + "-" + maxNumberofFailures);
        client.runTests(client, currentQuery, queryid, allowDeathOfAcquires, fails);
        testNo++;
        ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
            (testNo -1) + sep + "Planner" + sep + "storedObjects"));
        utils.sortout(adapts, false);
        currentSuccessfulAdaptations.add(utils.getPartial());
      }
      currentlyFailedNodes.clear();
      double currentLifetime = numberOfExectutionCycles * (originalQEP.getAgendaIOT().getLength_bms(false) / 1024) * currentNumberOfFailures ; // seconds
      ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
          (testNo -1) + sep + "Planner" + sep + "storedObjects"));
      utils.sortout(adapts, false);
      currentLifetime = currentLifetime + utils.getPartial().getLifetimeEstimate();
      utils.storeAdaptation(queryid, testNo -1, currentLifetime, PlotterEnum.PARTIAL, fails);
      
      utils.plotTopology(testNo -1);
      client.resetDataSources(originalQEP);
      currentSuccessfulAdaptations.clear();
      adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + 
          "OTASection" + sep + "storedObjects"));
      currentSuccessfulAdaptations.add(adapts.get(0));
    }
    }
    catch(Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }
    
  }

  /**
   * does the global tests
   * @param currentQuery
   * @param allowDeathOfAcquires
   * @throws Exception
   */
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
      ArrayList<String> fails = new ArrayList<String>();
      System.out.println("running tests with " + currentNumberOfFailures + " failures");
      for(int currentFailure = 1; currentFailure <= currentNumberOfFailures; currentFailure++)
      {
        System.out.println("running with test no" + testNo);
        client.getQEP().getLAF().setQueryName("query" + queryid + "-"+ currentFailure + "-" + maxNumberofFailures);
        client.runTests(client, currentQuery, queryid, allowDeathOfAcquires, fails);
        testNo++;
        ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
            (testNo -1) + sep + "Planner" + sep + "storedObjects"));
        utils.sortout(adapts, false);
        currentSuccessfulAdaptations.add(utils.getGlobal());
      }
      currentlyFailedNodes.clear();
      double currentLifetime = numberOfExectutionCycles * (originalQEP.getAgendaIOT().getLength_bms(false) / 1024) * currentNumberOfFailures;
      ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
          (testNo -1) + sep + "Planner" + sep + "storedObjects"));
      utils.sortout(adapts, false);
      currentLifetime = currentLifetime + utils.getGlobal().getLifetimeEstimate();
      utils.storeAdaptation(queryid, testNo -1, currentLifetime, PlotterEnum.GLOBAL, fails);
      utils.plotTopology(testNo -1);
      client.resetDataSources(originalQEP);
      currentSuccessfulAdaptations.clear();
      adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + 
          "OTASection" + sep + "storedObjects"));
      currentSuccessfulAdaptations.add(adapts.get(0));
      
    }
    
  }
  
  /**
   * does the overlay tests
   * @param currentQuery
   * @param allowDeathOfAcquires
   * @throws Exception
   */
  static int failedTests = 0;
  private static void runOverlayTests(String currentQuery, boolean allowDeathOfAcquires) 
  throws Exception
  {
    //client.resetDataSources(originalQEP);
    originalQEP = client.getQEP();
    orginialOverlay = client.getOverlay();
    System.out.println("running tests for local ");
    SNEEProperties.setSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE, ChoiceAssessorPreferenceEnum.Local.toString());
    testNo = 1;
    orginialOverlay = client.getOverlay();
    for(int currentNumberOfFailures = 1; currentNumberOfFailures <= maxNumberofFailures; currentNumberOfFailures++)
    {
      calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
      System.out.println("running tests with " + currentNumberOfFailures + " failures");
      client.setupOverlay();
      ArrayList<String> fails = new ArrayList<String>();
      
      for(int currentFailure = 1; currentFailure <= currentNumberOfFailures; currentFailure++)
      {
        System.out.println("running with test no" + testNo);
        client.getQEP().getLAF().setQueryName("query" + queryid + "-"+ currentFailure + "-" + maxNumberofFailures);
        boolean successful = client.runTests(client, currentQuery, queryid, allowDeathOfAcquires, fails);
        if(!successful)
          failedTests++;
        testNo++;
        
        ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
            (testNo -1) + sep + "Planner" + sep + "storedObjects"));
        utils.sortout(adapts, false);
        currentSuccessfulAdaptations.add(utils.getLocal());
      }
      currentlyFailedNodes.clear();
      double currentLifetime = numberOfExectutionCycles * (originalQEP.getAgendaIOT().getLength_bms(false) / 1024) * (currentNumberOfFailures - failedTests);
      ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
          (testNo -1 - failedTests) + sep + "Planner" + sep + "storedObjects"));
      utils.sortout(adapts, false);
      if(failedTests  == 0)
        currentLifetime = currentLifetime + utils.getLocal().getLifetimeEstimate();
      utils.storeAdaptation(queryid, testNo -1, currentLifetime, PlotterEnum.LOCAL, fails);
      utils.plotTopology(testNo -1);
      client.resetDataSources(originalQEP);
      currentSuccessfulAdaptations.clear();
     adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + 
          "OTASection" + sep + "storedObjects"));
      currentSuccessfulAdaptations.add(adapts.get(0));
      failedTests = 0;
    }
    
  }
  
  private LogicalOverlayNetworkImpl getOverlay() 
  throws SchemaMetadataException, TypeMappingException, OptimizationException,
  IOException, SNEEConfigurationException, CodeGenerationException
  {
    SNEEController control = (SNEEController) controller;
    return (LogicalOverlayNetworkImpl) control.getOverlay();
  }

  private void setupOverlay() 
  throws SchemaMetadataException, TypeMappingException, OptimizationException,
  IOException, SNEEConfigurationException, CodeGenerationException
  {
    SNEEController control = (SNEEController) controller;
    control.setupOverlay();
    
  }

  /**
   * does the best tests
   * @param currentQuery
   * @param allowDeathOfAcquires
   * @throws Exception
   */
  private static void runBestTests(String currentQuery, boolean allowDeathOfAcquires) 
  throws Exception
  {
    //client.resetDataSources(originalQEP);
    try{
    originalQEP = client.getQEP();
    System.out.println("running tests for best ");
    SNEEProperties.setSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE, ChoiceAssessorPreferenceEnum.Best.toString());
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_STRATEGIES, FailedNodeStrategyEnum.All.toString());
    testNo = 1;
    orginialOverlay = client.getOverlay();
    testNo = 1;
    lastPlan = originalQEP;
    for(int currentNumberOfFailures = 1; currentNumberOfFailures <= maxNumberofFailures; currentNumberOfFailures++)
    {
      calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
      client.setupOverlay();
      System.out.println("running tests with " + currentNumberOfFailures + " failures");
      ArrayList<String> fails = new ArrayList<String>();
      for(int currentFailure = 1; currentFailure <= currentNumberOfFailures; currentFailure++)
      {
        System.out.println("running with test no" + testNo);
        client.getQEP().getLAF().setQueryName("query" + queryid + "-"+ currentFailure + "-" + maxNumberofFailures);
        boolean successful = client.runTests(client, currentQuery, queryid, allowDeathOfAcquires, fails);
        if(!successful)
          failedTests++;
        testNo++;
        ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
            (testNo -1) + sep + "Planner" + sep + "storedObjects"));
        utils.sortout(adapts, false);
        currentSuccessfulAdaptations.add(utils.getBest());
      }
      lastPlan = originalQEP;
      currentlyFailedNodes.clear();
      double currentLifetime = numberOfExectutionCycles * (originalQEP.getAgendaIOT().getLength_bms(false) / 1024) * (currentNumberOfFailures - failedTests);
      ArrayList<Adaptation> adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
          (testNo -1- failedTests) + sep + "Planner" + sep + "storedObjects"));
      utils.sortout(adapts, true);
      if(failedTests == 0)
        currentLifetime = currentLifetime + utils.getBest().getLifetimeEstimate();
      utils.storeAdaptation(queryid, testNo -1, currentLifetime, PlotterEnum.ALL, fails);
      utils.plotTopology(testNo -1);
      client.resetDataSources(originalQEP);
      currentSuccessfulAdaptations.clear();
      adapts = utils.readInObjects(new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + 
          "OTASection" + sep + "storedObjects"));
      currentSuccessfulAdaptations.add(adapts.get(0));
      failedTests = 0;
    }
    }catch(Exception e)
    {
      e.printStackTrace();
    }
    
  }

  /**
   * resets all the data stores for a new run
   * @param qep
   * @throws SourceDoesNotExistException
   * @throws SourceMetadataException
   * @throws SNEEConfigurationException
   * @throws SNCBException
   * @throws TopologyReaderException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws IOException
   * @throws CodeGenerationException
   */
  private void resetDataSources(SensorNetworkQueryPlan qep) 
  throws SourceDoesNotExistException, SourceMetadataException,
  SNEEConfigurationException, SNCBException, TopologyReaderException, 
  OptimizationException, SchemaMetadataException, TypeMappingException, 
  IOException, CodeGenerationException
  {
    SNEEController control = (SNEEController) controller;
    control.resetMetaData(qep);
    control.resetQEP(qep);
  }

  /**
   * calculate how many execution cycles are needed to run before the next node fails.
   * @param currentNumberOfFailures
   * @param client
   */
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

  /**
   * get the orginal lifetime
   * @return
   */
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
  CodeGenerationException, NumberFormatException, WhenSchedulerException 
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    SNEEController control = (SNEEController) controller;
    control.addQuery(_query, _queryParams, null, true, true, true);
    controller.close();
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }

  private static void collectQueries(ArrayList<String> queries) throws IOException
  {
    //String filePath = Utils.validateFileLocation("tests/queries.txt");
    File queriesFile = new File("src/main/resources/" + dictonary + "/queries.txt");
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
     File testFolder = new File("src/main/resources/" + dictonary);
     if(!testFolder.exists())
       testFolder.mkdir();
     
     if(testFolder.list().length  == 1 || testFolder.list().length  == 0)
     {
       String testPath = testFolder.getAbsolutePath();
       System.out.println(testPath);
       String [] params = {"generateScenariosNatural.py", testPath};
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

  private boolean runTests(SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay client, String currentQuery, 
                        int queryid, boolean allowDeathOfAcquires, ArrayList<String> fails) 
  throws Exception 
  {
  	updateSites(allowDeathOfAcquires);
  	if(applicableConfulenceSites.size() != 0)
  	{
  		  String deadNode = chooseNodes();
  		  controller.simulateEnergyDrainofAganedaExecutionCycles(numberOfExectutionCycles);
  		  client.runForTests(deadNode, queryid, fails); 
  		  utils.updateRecoveryFile(queryid);
        System.gc();
        updateSites(allowDeathOfAcquires);  
  	}
  	else
  	{
  	  System.out.println("were no avilable nodes to fail, will not run test");
  	  return false;
  	}
  	System.out.println("Stopping current query");
  	controller.close();
  	return true;
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
    allowDeathOfAcquires = false;
    
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
    ArrayList<String> removedNodes = new ArrayList<String>();
    removedNodes.addAll(currentlyFailedFailedNodes);
    removedNodes.addAll(currentlyFailedNodes);
    Iterator<String> failsIterator = removedNodes.iterator();
    while(failsIterator.hasNext())
    {
      String fail = failsIterator.next();
      applicableConfulenceSites.remove(fail);
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
    removedNodes = new ArrayList<String>();
    removedNodes.addAll(currentlyFailedFailedNodes);
    removedNodes.addAll(currentlyFailedNodes);
    failsIterator = removedNodes.iterator();
    while(failsIterator.hasNext())
    {
      String fail = failsIterator.next();
      applicableConfulenceSites.remove(fail);
    }
    /*
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
    
    removedNodes = new ArrayList<String>();
    removedNodes.addAll(currentlyFailedFailedNodes);
    removedNodes.addAll(currentlyFailedNodes);
    failsIterator = removedNodes.iterator();
    while(failsIterator.hasNext())
    {
      String fail = failsIterator.next();
      applicableConfulenceSites.remove(fail);
    }*/
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
   * @param fails 
   * @throws Exception 
   */
  public boolean runForTests(String failedNode, int queryid, ArrayList<String> fails)
  throws Exception
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    System.out.println("Failed node [" + failedNode + "] ");
    SNEEController control = (SNEEController) controller;
    control.giveAutonomicManagerQuery(_query);
    boolean sucessful = runSimulatedNodeFailure(failedNode, control, fails);
    if(!sucessful)
      return sucessful;
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
    return sucessful;
  }

  private boolean runSimulatedNodeFailure(String failedNode,
      SNEEController control, ArrayList<String> fails) 
  throws Exception
  {
    ArrayList<String> currentNodeFailures = new ArrayList<String>();
    currentlyFailedNodes.add(failedNode);
    currentNodeFailures.add(failedNode);
    try
    {
      control.runSimulatedNodeFailure(currentNodeFailures, false);
      fails.add(failedNode);
      currentlyFailedFailedNodes.clear();
      lastPlan = client.getQEP();
      return true;
    }
    catch(Exception e)
    { 
      System.out.println("system failed as " + e.getMessage());
      //e.printStackTrace();
      currentNodeFailures.clear();
      this.updateSites(true);
      currentlyFailedFailedNodes.add(failedNode);
      currentlyFailedNodes.remove(failedNode);
      applicableConfulenceSites.remove(failedNode);
      System.out.println("choosing new failure with applicable sites [" + applicableConfulenceSites.toString() + "]");
      String deadNode = chooseNodes();
      //QueryExecutionPlan lastPlan = control.getQEP();
      
      resetMetaData(originalQEP);
      resetQEP(originalQEP);
      try{
        String setting = SNEEProperties.getSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE);
        if(setting.equals(ChoiceAssessorPreferenceEnum.Local.toString()) || 
           setting.equals(ChoiceAssessorPreferenceEnum.Best.toString()))
            control.resetOverlayCost(orginialOverlay);
        updateMetaDataBackToCurrentState(control);
        resetQEP((SensorNetworkQueryPlan) lastPlan);
        controller.simulateEnergyDrainofAganedaExecutionCycles(numberOfExectutionCycles);
        System.out.println("system failed to adapt with node " + failedNode + " so will try node " + deadNode);
        boolean success = runSimulatedNodeFailure(deadNode, control, fails);
        fails.add(deadNode);
        return success;
      }
      catch(Exception f)
      {
        f.printStackTrace();
        return false;
      }
    }
  }

  private void resetQEP(SensorNetworkQueryPlan qep) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException, 
  IOException, CodeGenerationException
  {
    SNEEController control = (SNEEController) controller;
    control.resetQEP(qep);
  }

  private void resetMetaData(SensorNetworkQueryPlan qep) 
  throws SourceDoesNotExistException, SourceMetadataException, 
  SNEEConfigurationException, SNCBException, TopologyReaderException
  {
    SNEEController control = (SNEEController) controller;
    control.resetMetaData(qep);
  }

  private void updateMetaDataBackToCurrentState(SNEEController control) 
  throws SourceDoesNotExistException,
  FileNotFoundException, IOException, OptimizationException, SchemaMetadataException,
  TypeMappingException, SNEEConfigurationException
  {
    Iterator<String> failedNodesIDsIterator =  currentlyFailedNodes.iterator();
    int qepCounter = 0;
    while(failedNodesIDsIterator.hasNext())
    {
      String failedID = failedNodesIDsIterator.next();
      control.removeNodeFromTheMetaData(failedID, originalQEP);
      Adaptation adapt = currentSuccessfulAdaptations.get(qepCounter);
      try{
      control.simulateEnergyDrainofAganedaExecutionCycles(numberOfExectutionCycles, adapt.getOldQep(), adapt.getNewQep());
      }
      catch(Exception e)
      {
        e.printStackTrace();
      }
      qepCounter++;
    }
  }
}
