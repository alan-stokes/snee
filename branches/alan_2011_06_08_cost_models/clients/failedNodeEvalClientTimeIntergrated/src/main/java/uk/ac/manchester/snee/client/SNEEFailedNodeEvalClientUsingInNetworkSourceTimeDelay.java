package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.SQLException;
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
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerException;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
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
	protected static int maxNumberofFailures = 3;
	protected static double originalLifetime;
	protected static int numberOfExectutionCycles;
	protected static boolean calculated = false;
	protected static SensorNetworkQueryPlan originalQEP;
	
	private static FailedNodeTimeClientUtils utils = new FailedNodeTimeClientUtils();
	
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
      utils.writeLatexCore();
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      collectQueries(queries);
      
	    queryIterator = queries.iterator();
	    failedOutput = utils.createFailedTestListWriter();
	    
	    while(queryIterator.hasNext())
	    {
	      recursiveRun(queryIterator, duration, queryParams, false, failedOutput);
      }
	    
	    queryIterator = queries.iterator();
	    while(queryIterator.hasNext())
      {
        recursiveRun(queryIterator, duration, queryParams, true, failedOutput);
        calculated = false;
      }
	    failedOutput.write("\\end{document}");
	    failedOutput.flush();
	    failedOutput.close();  
    } 
		catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      System.out.println("error message was " + e.getMessage());
      logger.fatal(e);
      e.printStackTrace();
    }
	}

  private static void recursiveRun(Iterator<String> queryIterator, 
	                                 Long duration, String queryParams, 
	                                 boolean allowDeathOfAcquires, 
	                                 BufferedWriter failedOutput) 
  throws IOException 
	{
	//get query & schemas
    String currentQuery = queryIterator.next();
    String propertiesPath = "tests/snee" + queryid + ".properties";
    
    System.out.println("Running Tests on query " + (queryid));
    try
    {
      
      SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay client = 
        new  SNEEFailedNodeEvalClientUsingInNetworkSourceTimeDelay(currentQuery, 
                           duration, queryParams, null, propertiesPath);
      //set queryid to correct id
      SNEEController contol = (SNEEController) client.controller;
      contol.setQueryID(queryid);
      //added to allow recovery from crash
      utils.updateRecoveryFile(queryid);
      System.out.println("compiled control");
      client.runCompilelation();
      utils.plotOrginial(queryid, testNo);
      Adaptation global = null;
      Adaptation partial = null;
      Adaptation local = null;
      
      // run first adaptation
      for(int currentNumberOfFailures = 1; currentNumberOfFailures <= maxNumberofFailures; currentNumberOfFailures++)
      {
        if(currentNumberOfFailures == 1)
        {
          calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
          System.out.println("running tests with " + currentNumberOfFailures + "nd failure");
          client.getQEP().getLAF().setQueryName("query" + queryid + "-" + maxNumberofFailures);
          client.runTests(client, currentQuery, queryid, allowDeathOfAcquires);
          double currentLifetime = numberOfExectutionCycles * originalQEP.getAgendaIOT().getLength_bms(false) * currentNumberOfFailures;
          utils.plotAdaptations(queryid, testNo, currentLifetime, PlotterEnum.ALL);
          global = utils.getGlobal();
          partial = utils.getPartial();
          local = utils.getLocal();
          testNo++;
        }
        else
        {
          if(global != null)
          {
            calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
            System.out.println("running tests with " + currentNumberOfFailures + "nd failure on global");
            client.resetQEP(global.getNewQep());
            client.getQEP().getLAF().setQueryName("query" + queryid + "-" + maxNumberofFailures);
            client.runTests(client, currentQuery, queryid, allowDeathOfAcquires);
            double currentLifetime = numberOfExectutionCycles * originalQEP.getAgendaIOT().getLength_bms(false) * currentNumberOfFailures;
            utils.plotAdaptations(queryid, testNo, currentLifetime, PlotterEnum.GLOBAL);
            global = utils.getGlobal();
          }
          if(partial != null)
          {
            calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
            System.out.println("running tests with " + currentNumberOfFailures + "nd failure on partial");
            client.resetQEP(partial.getNewQep());
            client.getQEP().getLAF().setQueryName("query" + queryid + "-" + maxNumberofFailures);
            client.runTests(client, currentQuery, queryid, allowDeathOfAcquires);
            double currentLifetime = numberOfExectutionCycles * originalQEP.getAgendaIOT().getLength_bms(false) * currentNumberOfFailures;
            utils.plotAdaptations(queryid, testNo, currentLifetime, PlotterEnum.PARTIAL);
            partial = utils.getPartial();
          }
          if(local != null)
          {
            calculateAgendaExecutionsBetweenFailures(currentNumberOfFailures, client); 
            System.out.println("running tests with " + currentNumberOfFailures + "nd failure on local");
            client.resetQEP(local.getNewQep());
            client.getQEP().getLAF().setQueryName("query" + queryid + "-" + maxNumberofFailures);
            client.runTests(client, currentQuery, queryid, allowDeathOfAcquires);
            double currentLifetime = numberOfExectutionCycles * originalQEP.getAgendaIOT().getLength_bms(false) * currentNumberOfFailures;
            utils.plotAdaptations(queryid, testNo, currentLifetime, PlotterEnum.LOCAL);
            local = utils.getLocal();
          }
        }
        testNo++;
        utils.endPlotLine();
      }
      queryid ++;
      System.out.println("Ran all tests on query " + (queryid) + " going onto next topology");
    }
    catch(Exception e)
    {
      e.printStackTrace();
      utils.writeErrorToFile(e, failedOutput, queryid, testNo);
      queryid ++;
      recursiveRun(queryIterator, duration, queryParams, allowDeathOfAcquires, failedOutput);
    }
	}
  
  private void resetQEP(SensorNetworkQueryPlan qep) 
  {
    SNEEController control = (SNEEController) controller;
    control.resetQEP(qep);
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
    Long agendaLength = originalQEP.getAgendaIOT().getLength_bms(false); // s
    numberOfExectutionCycles = (int) (timeBetweenFailures/agendaLength);
  }


  private static double getOriginalLifetime()
  {
    
    File inputFolder = new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + 
                                "OTASection" + sep + "storedObjects");
    ArrayList<Adaptation> orginalList = new FailedNodeTimeClientUtils().readInObjects(inputFolder);
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
    control.queryCompilationOnly(_query, _queryParams);
    control.addQueryWithoutCompilationAndStarting(_query, _queryParams);
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
  	testNo = 1;
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
   */
  private static String chooseNodes() 
  {
	  int size = applicableConfulenceSites.size();
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
   * @throws SNEECompilerException
   * @throws MetadataException
   * @throws EvaluatorException
   * @throws SNEEException
   * @throws SQLException
   * @throws SNEEConfigurationException
   * @throws MalformedURLException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws AgendaException
   * @throws UnsupportedAttributeTypeException
   * @throws SourceMetadataException
   * @throws TopologyReaderException
   * @throws SNEEDataSourceException
   * @throws CostParametersException
   * @throws SNCBException
   * @throws IOException
   * @throws CodeGenerationException
   * @throws AutonomicManagerException
   */
  public void runForTests(String failedNode, int queryid)
  throws SNEECompilerException, MetadataException, EvaluatorException,
  SNEEException, SQLException, SNEEConfigurationException, 
  MalformedURLException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, AgendaException, UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException, IOException, CodeGenerationException,
  AutonomicManagerException
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    System.out.println("Failed node [" + failedNode + "] ");
    SNEEController control = (SNEEController) controller;
    control.giveAutonomicManagerQuery(_query);
    ArrayList<String> nodeFailures = new ArrayList<String>();
    nodeFailures.add(failedNode);
    control.runSimulatedNodeFailure(nodeFailures);
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }
}
