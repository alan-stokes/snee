package uk.ac.manchester.snee.client;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

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
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerException;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.ChoiceAssessorPreferenceEnum;
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


public class CondorFailedNodeClient extends SNEEClient 
{
  private static ArrayList<String> applicableConfulenceSites = new ArrayList<String>();
  protected static int maxNumberofFailures = 8;
  protected static ArrayList<String> currentlyFailedNodes = new ArrayList<String>(maxNumberofFailures);
  protected static double originalLifetime;
  protected static int numberOfExectutionCycles;
  protected static boolean calculated = false;
  protected static SensorNetworkQueryPlan originalQEP;
  private static int queryid = 64;
  protected static int testNo = 1;
  private static String sep = System.getProperty("file.separator");
  private static final FailedNodeTimeClientUtils utils = new FailedNodeTimeClientUtils();
  protected static CondorFailedNodeClient client;
  protected static int faieldTests = 0;
  protected static int preditctableNodeFailureIterations = 0;
  
  public CondorFailedNodeClient(String query, double duration,
      String queryParams, String csvFilename, String sneeProperties)
      throws SNEEException, IOException, SNEEConfigurationException
  {
    super(query, duration, queryParams, csvFilename, sneeProperties);
    // TODO Auto-generated constructor stub
  }
  
  
  /**
   * The main entry point for the condor client.
   * All input files are given by the args and the snee.properities handed to args.
   * @param args
   * @throws IOException
   * @throws InterruptedException 
   */
  public static void main(String[] args) 
  { 
    try
    {
      
      Long duration = Long.valueOf("120");
      String queryParams = "query-parameters.xml";
      
      String query = args[0];
      query = query.replace("_", " ");
      String propertiesPath = args[1];
      queryid = Integer.parseInt(args[2]);
      //File output = new File("output");
      //output.mkdir();
      //File result = new File(output.toString() + "/" + "ran" + query + queryid);
      //result.mkdir();
     // System.out.println("made folder output and " + output.toString() + "/" + "ran" + query + queryid);
      recursiveRun(query, duration, queryParams, false, propertiesPath) ;
    }
    catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      System.out.println("error message was " + e.getMessage());
      logger.fatal(e);
      e.printStackTrace();
    }
  }
  
  private static void recursiveRun(String currentQuery, 
      Long duration, String queryParams, 
      boolean allowDeathOfAcquires, String propertiesPath) 
  throws IOException 
  {
    System.out.println("Running Tests on query " + (queryid));
    try
    {
      System.out.println("initisling client");
      client = 
      new  CondorFailedNodeClient(currentQuery, duration, queryParams, null, propertiesPath);
      //set queryid to correct id
      System.out.println("getting controller");
      SNEEController contol = (SNEEController) client.getController();
      System.out.println("setting queryid");
      contol.setQueryID(queryid);
      System.out.println("running compilation");
      client.runCompilelation();
      int position = 0;
      runSeveralTests(position, currentQuery, allowDeathOfAcquires);
      System.out.println("Ran all tests on query " + queryid);
      queryid ++;
    }
    catch(Exception e)
    {
      System.out.println("something major failed on query "+ queryid);
      e.printStackTrace();
      System.out.println(e.getMessage());
      System.exit(0);
    }
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
  CodeGenerationException, NumberFormatException, WhenSchedulerException,
  AgendaLengthException 
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    SNEEController control = (SNEEController) getController();
    
    
    control.addQuery(_query, _queryParams);
    getController().close();
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }  
  
  private static void runSeveralTests(int position, String currentQuery,
      boolean allowDeathOfAcquires) throws IOException
  {
    try
    {
      if(position == 0)
      {
        runGlobalTests(currentQuery, allowDeathOfAcquires);
        runSeveralTests(position + 1, currentQuery, allowDeathOfAcquires);
      }
      else if(position == 1)
      {
        runPartialTests(currentQuery, allowDeathOfAcquires);
        runSeveralTests(position + 1, currentQuery, allowDeathOfAcquires);
      }
    }
    catch(Exception e)
    {
      if(position + 1 == 4)
      {
        System.out.println("system failed as: " + e.getMessage());
        e.printStackTrace();
        utils.plotTopology(maxNumberofFailures);
        queryid ++;
        utils.newPlotters(queryid);
        System.out.println("Ran all tests on query " + (queryid) + " going onto next topology");
      }
      else
      {
        System.out.println("system failed as: " + e.getMessage());
        System.out.println("running with new position");
        runSeveralTests(position + 1, currentQuery, allowDeathOfAcquires);
      }
    }
  }
  
  /**
   * does the partial tests
   * @param currentQuery
   * @param allowDeathOfAcquires
   * @throws Exception
   */
  private static void runPartialTests(String currentQuery,
      boolean allowDeathOfAcquires)
  throws Exception
  {
    //client.resetDataSources(originalQEP);
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
      }
      currentlyFailedNodes.clear();
      boolean successful = false;
      double currentLifetime = 0.0;
      if(faieldTests == 0)
      {
        successful = true;
        currentLifetime = numberOfExectutionCycles * (currentNumberOfFailures);
      }
      else
      {
        currentLifetime = numberOfExectutionCycles * (currentNumberOfFailures - (faieldTests -1));
      }
      faieldTests = 0;
      utils.storeAdaptation(queryid, testNo -1, currentLifetime, PlotterEnum.PARTIAL, fails, successful);
      utils.plotTopology(testNo -1);
      client.resetDataSources(originalQEP, false);
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
      }
      currentlyFailedNodes.clear();
      boolean successful = false;
      double currentLifetime = 0.0;
      if(faieldTests == 0)
      {
        successful = true;
        currentLifetime = numberOfExectutionCycles * (currentNumberOfFailures);
      }
      else
      {
        currentLifetime = numberOfExectutionCycles * (currentNumberOfFailures - (faieldTests -1));
      }
      faieldTests = 0;
      utils.storeAdaptation(queryid, testNo -1, currentLifetime, PlotterEnum.GLOBAL, fails, successful);
      utils.plotTopology(testNo -1);
      client.resetDataSources(originalQEP, false);
    } 
  }
  
  private static void calculateAgendaExecutionsBetweenFailures(int currentNumberOfFailures, CondorFailedNodeClient client)
  {
    if(!calculated)
    {
      originalLifetime = getOriginalLifetime();
      originalQEP = client.getQEP();
      calculated = true;
    }
    numberOfExectutionCycles =  new Double(originalLifetime / new Integer(currentNumberOfFailures + 1).doubleValue()).intValue(); //s
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
  
  private void resetDataSources(SensorNetworkQueryPlan qep, boolean keepEnergySoruces) 
  throws SourceDoesNotExistException, SourceMetadataException,
  SNEEConfigurationException, SNCBException, TopologyReaderException, 
  OptimizationException, SchemaMetadataException, TypeMappingException, 
  IOException, CodeGenerationException
  {
    SNEEController control = (SNEEController) getController();
    control.resetMetaData(qep);
    control.resetQEP(qep, keepEnergySoruces);
  }
  
  private boolean runTests(CondorFailedNodeClient client, String currentQuery, 
      int queryid, boolean allowDeathOfAcquires, ArrayList<String> fails) 
  throws Exception
  {
    try
    {
      updateSites(allowDeathOfAcquires);
      String deadNode = chooseNodes();
      if(applicableConfulenceSites.size() != 0)
      {
        Double energydrainLifetime = getController().getTimeTillNextNodefailsFromEnergyDelpetion();
        String energydrainNode = getController().getNextNodefailsFromEnergyDelpetion();
        
        if(energydrainLifetime < (numberOfExectutionCycles - preditctableNodeFailureIterations))
        {
          System.out.println("having to run a predictable node failure");
          System.out.println("runnign with node " + energydrainNode + " as lifetime is " + 
                             energydrainLifetime + "and should be "  + numberOfExectutionCycles);
          
          
          getController().simulateEnergyDrainofAganedaExecutionCycles(energydrainLifetime.intValue());
          client.runExpectedNodeFailure(energydrainNode, queryid, fails);
          preditctableNodeFailureIterations = energydrainLifetime.intValue();
        }
        else
        {
          getController().simulateEnergyDrainofAganedaExecutionCycles(numberOfExectutionCycles);
          client.runForTests(deadNode, queryid, fails); 
          preditctableNodeFailureIterations = 0;
        }
       // utils.updateRecoveryFile(queryid);
        System.gc();
        updateSites(allowDeathOfAcquires);  
      }
      else
      {
        System.out.println("were no avilable nodes to fail, will not run test");
        faieldTests++;
        client.updateAdpatationCount();
      }
      System.out.println("Stopping current query");
      getController().close();
      return true;
    }
    catch(Exception e)
    {
      System.out.println("were no avilable nodes to fail, will not run test");
      e.printStackTrace();
      faieldTests++;
      client.updateAdpatationCount();
      System.out.println("Stopping current query");
      getController().close();
      return false;
    }
  }
  
  private void runExpectedNodeFailure(String energydrainNode, int queryid,
                                      ArrayList<String> fails)
  throws Exception
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    System.out.println("Failed node [" + energydrainNode + "] ");
    SNEEController control = (SNEEController) getController();
    control.giveAutonomicManagerQuery(_query);
    ArrayList<String> currentNodeFailures = new ArrayList<String>();
    currentlyFailedNodes.add(energydrainNode);
    currentNodeFailures.add(energydrainNode);
    control.runSimulatedNodeFailure(currentNodeFailures);
    fails.add(energydrainNode);   
  }


  private void updateAdpatationCount()
  {
    SNEEController control = (SNEEController) getController();
    control.updateAdpatationCount();
    
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
    SNEEController snee = (SNEEController) getController();
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
  public void runForTests(String failedNode, int queryid, ArrayList<String> fails)
  throws Exception
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    System.out.println("Failed node [" + failedNode + "] ");
    SNEEController control = (SNEEController) getController();
    control.giveAutonomicManagerQuery(_query);
    boolean sucessful = runSimulatedNodeFailure(failedNode, control, fails);
    if(!sucessful)
      throw new AutonomicManagerException("couldnt adapt with any node failure");
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }
  
  private boolean runSimulatedNodeFailure(String failedNode,
      SNEEController control, ArrayList<String> fails) 
  throws Exception
  {
    ArrayList<String> currentNodeFailures = new ArrayList<String>();
    currentlyFailedNodes.add(failedNode);
    currentNodeFailures.add(failedNode);
    SensorNetworkQueryPlan lastQEP = client.getQEP();
    try
    {
      control.runSimulatedNodeFailure(currentNodeFailures);
      fails.add(failedNode);
      return true;
    }
    catch(Exception e)
    { 
      System.out.println("system failed as " + e.getMessage());
      e.printStackTrace();
      currentNodeFailures.clear();
      currentlyFailedNodes.remove(failedNode);
      applicableConfulenceSites.remove(failedNode);
      System.out.println("choosing new failure with applicable sites [" + applicableConfulenceSites.toString() + "]");
      String deadNode = chooseNodes();
      QueryExecutionPlan lastPlan = control.getQEP();
      
      resetMetaData(originalQEP);
      updateMetaDataBackToCurrentState(control);
      resetQEP((SensorNetworkQueryPlan) lastPlan);
      
      System.out.println("system failed to adapt with node " + failedNode + " so will try node " + deadNode);
      boolean success = runSimulatedNodeFailure(deadNode, control, fails);
      fails.add(deadNode);
      return success;
    }
  }
  
  private void resetQEP(SensorNetworkQueryPlan qep) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException, 
  IOException, CodeGenerationException, SNEEConfigurationException
  {
    SNEEController control = (SNEEController) getController();
    control.resetQEP(qep, true);
  }

  private void resetMetaData(SensorNetworkQueryPlan qep) 
  throws SourceDoesNotExistException, SourceMetadataException, 
  SNEEConfigurationException, SNCBException, TopologyReaderException
  {
    SNEEController control = (SNEEController) getController();
    control.resetMetaData(qep);
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
