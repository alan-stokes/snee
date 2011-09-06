package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStoreImpl;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClient;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.UtilsException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
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

public class SNEEFailedNodeEvalClientUsingInNetworkSource extends SNEEClient 
{
	private static ArrayList<String> siteIDs = new ArrayList<String>();
	private static RT routingTree;
	
	protected static Logger resultsLogger;
	private String sneeProperties;
	private static int testNo = 1;
	private static int recoveredTestValue = 0;
	private static boolean inRecoveryMode = false;
	private static int actualTestNo = 0;
	private static int queryid = 0;
	private static SensorNetworkQueryPlan qep;
	
	public SNEEFailedNodeEvalClientUsingInNetworkSource(String query, 
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
        SNEEFailedNodeEvalClientUsingInNetworkSource.class.
        getClassLoader().getResource("etc/common/log4j.properties"));   
	  
		Long duration = Long.valueOf("120");
		String queryParams = "etc/query-parameters.xml";
		try
    {
      checkRecoveryFile();
      runIxentsScripts();
      
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      collectQueries(queries);
      
	    Iterator<String> queryIterator = queries.iterator();
	  
	    moveQueryToRecoveryLocation(queries);
	   
	    while(queryIterator.hasNext())
	    {
	      RecursiveRun(queryIterator, duration, queryParams);
      }
    } catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      logger.fatal(e);
      e.printStackTrace();
      System.exit(1);
    }
	}
	
	
	private static void RecursiveRun(Iterator<String> queryIterator, Long duration, String queryParams) 
	throws 
	SNEEException, IOException, SNEEConfigurationException, 
	SNEECompilerException, MetadataException, EvaluatorException,
  OptimizationException, SQLException, UtilsException, 
  SchemaMetadataException, TypeMappingException, AgendaException, 
  UnsupportedAttributeTypeException, SourceMetadataException, 
  TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException, CodeGenerationException
	{
	//get query & schemas
    String currentQuery = queryIterator.next();
    String propertiesPath = "tests/snee" + queryid + ".properties";
    
    System.out.println("Running Tests on query " + (queryid + 1));
    try
    {
      SNEEFailedNodeEvalClientUsingInNetworkSource client = 
        new  SNEEFailedNodeEvalClientUsingInNetworkSource(currentQuery, duration, queryParams, null, propertiesPath);
      //set queryid to correct id
      SNEEController contol = (SNEEController) client.controller;
      contol.setQueryID(queryid + 1);
      //added to allow recovery from crash
      updateRecoveryFile();
      client.runCompilelation();
      System.out.println("compiled control");
      SensorNetworkQueryPlan currentQEP = client.getQEP();
      qep = currentQEP;
      routingTree = currentQEP.getRT();
      System.out.println("running tests");
      client.runTests(client, currentQuery, queryid);
      queryid ++;
      System.out.println("Ran all tests on query " + (queryid + 1));
    }
    catch(Exception e)
    {
      e.printStackTrace();
      queryid ++;
      System.out.println("tests failed for query " + queryid + " going onto query " + (queryid + 1));
      RecursiveRun(queryIterator, duration, queryParams);
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
      logger.debug("RETURN");// TODO Auto-generated method stub	
  }

private static void moveQueryToRecoveryLocation(ArrayList<String> queries)
  {
    Iterator<String> queryIterator = queries.iterator();
    //recovery move
    for(int i = 0; i < queryid - 1; i++)
    {
      queryIterator.next();  
    }
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
     //run Ixent's modified script to produce 30 random test cases. 
     File pythonFolder = new File("src/main/resources/python/");
     String pythonPath = pythonFolder.getAbsolutePath();
     File testFolder = new File("src/main/resources/tests");
     String testPath = testFolder.getAbsolutePath();
     String [] params = {"generateScenarios.py", testPath};
     Map<String,String> enviro = new HashMap<String, String>();
     System.out.println("running Ixent's scripts for 30 random queries");
     Utils.runExternalProgram("python", params, enviro, pythonPath);
     System.out.println("have ran Ixent's scripts for 30 random queries");
    
  }

  private void runTests(SNEEFailedNodeEvalClientUsingInNetworkSource client, String currentQuery, 
                        int queryid) 
  throws 
  Exception
  {
    
    updateSites(routingTree); 	 
    int noSites = siteIDs.size();
    //no failed nodes, dont bother running tests
    if(noSites == 0)
      throw new Exception("no avilable nodes to fail");
    int position = 0;
    ArrayList<String> deadNodes = new ArrayList<String>(); 
    chooseNodes(deadNodes, noSites, position, client, currentQuery, queryid, true);
  }

  /**
   * goes thoun routing tree, looknig for nodes which are not source nodes and are 
   * confluence sites which are sites which will cause likely changes to results when lost
   * @param routingTree2
   * @throws SourceDoesNotExistException 
   */
  private void updateSites(RT routingTree) 
  throws SourceDoesNotExistException
  {
    actualTestNo = 0;
    Iterator<Site> siteIterator = routingTree.siteIterator(TraversalOrder.POST_ORDER);
    siteIDs.clear();
    SNEEController snee = (SNEEController) controller;
    SourceMetadataAbstract metadata = snee.getMetaData().getSource(qep.getMetaData().getOutputAttributes().get(1).getExtentName());
    SensorNetworkSourceMetadata sensornetworkMetadata = (SensorNetworkSourceMetadata) metadata;
    int[] sources = sensornetworkMetadata.getSourceSites(qep.getDAF().getPAF());
    String sinkID = qep.getRT().getRoot().getID();
    
    while(siteIterator.hasNext())
    {
      Site currentSite = siteIterator.next();
      if(currentSite.getInDegree() > 1 && 
         !siteIDs.contains(Integer.parseInt(currentSite.getID())) &&
         !isSource(currentSite, sources) && 
         !currentSite.getID().equals(sinkID))
          siteIDs.add(currentSite.getID());
    }// TODO Auto-generated method stub
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
   * recursive method call, which iterates though the selected nodes to fail, 
   * using only nodes given in the updatesites method.
   * @param deadNodes
   * @param noSites
   * @param position
   * @param client
   * @param currentQuery
   * @param queryid2 
   * @throws Exception 
   */
  private static void chooseNodes(ArrayList<String> deadNodes, int noSites,
      int position, SNEEFailedNodeEvalClientUsingInNetworkSource client, String currentQuery, 
      int queryid, boolean first) 
  throws 
  Exception
  {
    if(position < noSites)
    {
      if(first && position == noSites -1)
      {
        deadNodes.add(siteIDs.get(position));
        chooseNodes(deadNodes, noSites, position + 1, client, currentQuery, queryid, first);
        first = false;
      }
      else
      {
        chooseNodes(deadNodes, noSites, position + 1, client, currentQuery, queryid, first);
        deadNodes.add(siteIDs.get(position));
        chooseNodes(deadNodes, noSites, position + 1, client, currentQuery, queryid, first);
        deadNodes.remove(deadNodes.size() -1);
      }
    }
    else
    {   
      updateRecoveryFile();
        
      if(inRecoveryMode)
      {
        inRecoveryMode = false;
        client.runForTests(deadNodes, queryid ); 
      }
      else
      {
        client.runForTests(deadNodes, queryid); 
        actualTestNo++;
      }      
    }
  }

  public void runForTests(ArrayList<String> failedNodes, int queryid)
  throws SNEECompilerException, MetadataException, EvaluatorException,
  SNEEException, SQLException, SNEEConfigurationException, 
  MalformedURLException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, AgendaException, UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException, IOException, CodeGenerationException
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    System.out.println("Failed nodes" + failedNodes.toString() );
    SNEEController control = (SNEEController) controller;
    control.giveAutonomicManagerQuery(_query);
    control.runSimulatedNodeFailure(failedNodes);
    //  List<ResultSet> results1 = resultStore.getResults();
    System.out.println("Stopping query " + queryid + ".");
    controller.close();
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }
  
  private static void updateRecoveryFile() throws IOException
  {
    File folder = new File("results"); 
    String path = folder.getAbsolutePath();
    //added to allow recovery from crash
    BufferedWriter recoverWriter = new BufferedWriter(new FileWriter(new File(path + "/recovery.tex")));
    actualTestNo = 0;
    recoverWriter.write(testNo + "\n");
    recoverWriter.write(actualTestNo + "\n");
    recoverWriter.flush();
    recoverWriter.close();
    
  }
  
  private static void checkRecoveryFile() throws IOException
  {
    //added to allow recovery from crash
    File folder = new File("results"); 
    String path = folder.getAbsolutePath();
    File resultsFile = new File(path + "/results.tex");
    File recoveryFile = new File(path + "/recovery.tex");
    if(recoveryFile.exists())
    {
      BufferedReader recoveryTest = new BufferedReader(new FileReader(recoveryFile));
      String recoveryQueryIdLine = recoveryTest.readLine();
      String recoverQueryTestLine = recoveryTest.readLine();
      System.out.println("recovery text located with query test value = " +  recoveryQueryIdLine + " and has test no = " + recoverQueryTestLine);
      queryid = Integer.parseInt(recoveryQueryIdLine);
      testNo = queryid;
      recoveredTestValue = Integer.parseInt(recoverQueryTestLine);
      inRecoveryMode = true;
      if(queryid == 0 && recoveredTestValue == 0)
      {
        boolean result;
        deleteAllFilesInResultsFolder(folder);
        result = resultsFile.createNewFile();
        result = recoveryFile.createNewFile();
        inRecoveryMode = false;
      }
    }
    else
    {
      System.out.println("create file recovery.tex with 2 lines each containing the number 0");
      folder.mkdir();    
    }
  }

  private static void deleteAllFilesInResultsFolder(File folder)
  {
    File [] filesInFolder = folder.listFiles();
    for(int fileIndex = 0; fileIndex < filesInFolder.length; fileIndex++)
    {
      filesInFolder[fileIndex].delete();
    }
  }
}
