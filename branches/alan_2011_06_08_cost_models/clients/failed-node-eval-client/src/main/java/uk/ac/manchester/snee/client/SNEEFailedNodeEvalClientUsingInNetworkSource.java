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
import uk.ac.manchester.cs.snee.manager.AutonomicManagerException;
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
  private static String sep = System.getProperty("file.separator");
	protected static Logger resultsLogger;
	private String sneeProperties;
	private static int recoveredTestValue = 0;
	private static boolean inRecoveryMode = false;
	private static int queryid = 1;
	protected static int testNo = 1;
	private static SensorNetworkQueryPlan qep;
	private static BufferedWriter latexCore = null;
	
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
      generateLatexCore();
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      collectQueries(queries);
      
	    Iterator<String> queryIterator = queries.iterator();
	  
	    moveQueryToRecoveryLocation(queries);
	   
	    BufferedWriter failedOutput = createFailedTestListWriter();
	    
	    while(queryIterator.hasNext())
	    {
	      recursiveRun(queryIterator, duration, queryParams, false, failedOutput);
      }
	    
	    queryIterator = queries.iterator();
	    while(queryIterator.hasNext())
      {
        recursiveRun(queryIterator, duration, queryParams, true, failedOutput);
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
      System.exit(1);
    }
	}
	
	
	private static void generateLatexCore() throws IOException
  {
	  File latexCoreF = new File("LatexSections");
	  if(!latexCoreF.exists())
    {
	    latexCoreF.mkdir();
    }
	  else
	  {
	    deleteFileContents(latexCoreF);
	  }
	  latexCoreF = new File(latexCoreF.toString() + sep + "core.tex");
	  latexCore = new BufferedWriter(new FileWriter(latexCoreF));
	  latexCore.write("\\documentclass[landscape, 10pt]{report} \n\\usepackage[landscape]{geometry} \n" +
	  		            "\\begin{document}  \n");
	  latexCore.flush();  
  }
	
	private static void deleteFileContents(File firstOutputFolder)
  {
    if(firstOutputFolder.exists())
    {
      File[] contents = firstOutputFolder.listFiles();
      for(int index = 0; index < contents.length; index++)
      {
        File delete = contents[index];
        if(delete.isDirectory())
          if(delete != null && delete.listFiles().length > 0)
            deleteFileContents(delete);
          else
            delete.delete();
        else
          delete.delete();
      }
    }
    else
    {
      firstOutputFolder.mkdir();
    }  
  }

  private static BufferedWriter createFailedTestListWriter() throws IOException
  {
	  File folder = new File("results"); 
	  File file = new File(folder + sep + "failedTests");
    return new BufferedWriter(new FileWriter(file));
  }

  private static void recursiveRun(Iterator<String> queryIterator, 
	                                 Long duration, String queryParams, 
	                                 boolean allowDeathOfAcquires, 
	                                 BufferedWriter failedOutput) 
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
    
    System.out.println("Running Tests on query " + (queryid));
    try
    {
      SNEEFailedNodeEvalClientUsingInNetworkSource client = 
        new  SNEEFailedNodeEvalClientUsingInNetworkSource(currentQuery, 
                           duration, queryParams, null, propertiesPath);
      //set queryid to correct id
      SNEEController contol = (SNEEController) client.controller;
      contol.setQueryID(queryid);
      //added to allow recovery from crash
      updateRecoveryFile();
      client.runCompilelation();
      System.out.println("compiled control");
      SensorNetworkQueryPlan currentQEP = client.getQEP();
      qep = currentQEP;
      routingTree = currentQEP.getRT();
      System.out.println("running tests");
      client.runTests(client, currentQuery, queryid, allowDeathOfAcquires);
      
      queryid ++;
      System.out.println("Ran all tests on query " + (queryid));
    }
    catch(Exception e)
    {
      e.printStackTrace();
      writeErrorToFile(e, failedOutput );
      queryid ++;
      recursiveRun(queryIterator, duration, queryParams, allowDeathOfAcquires, failedOutput);
    }
	}

  private static void writeErrorToFile(Exception e, BufferedWriter failedOutput) throws IOException
  {
    System.out.println("tests failed for query " + queryid + "  going onto query " + (queryid + 1));
    failedOutput.write(queryid + " | " + testNo + "          |         " + e.getMessage() + "\n\n" );
    StackTraceElement[] trace = e.getStackTrace();
    for(int index = 0; index < trace.length; index++)
    {
      failedOutput.write(trace[index].toString() + "\n");
    }
    failedOutput.write("\n\n");
    failedOutput.flush();
    
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

  private void runTests(SNEEFailedNodeEvalClientUsingInNetworkSource client, String currentQuery, 
                        int queryid, boolean allowDeathOfAcquires) 
  throws 
  Exception
  {
    
    updateSites(routingTree, allowDeathOfAcquires); 	 
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
   * @param allowDeathOfAcquires 
   * @param routingTree2
   * @throws SourceDoesNotExistException 
   */
  private void updateSites(RT routingTree, boolean allowDeathOfAcquires) 
  throws SourceDoesNotExistException
  {
    testNo = 0;
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
         (
             (allowDeathOfAcquires && isSource(currentSite, sources)) ||  
             (!allowDeathOfAcquires && !isSource(currentSite, sources))
         ) && 
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
        chooseNodes(deadNodes, noSites, position + 1, client, currentQuery, queryid, first);
        deadNodes.add(siteIDs.get(position));
        chooseNodes(deadNodes, noSites, position + 1, client, currentQuery, queryid, first);
        deadNodes.remove(deadNodes.size() -1);
    }
    else
    {   
      if(deadNodes.size()  != 0)
      {
        updateRecoveryFile();
          
        if(inRecoveryMode)
        {
          inRecoveryMode = false;
          client.runForTests(deadNodes, queryid ); 
          testNo++;
        }
        else
        {
          client.runForTests(deadNodes, queryid); 
          testNo++;
        }  
      }
    }
  }

  public void runForTests(ArrayList<String> failedNodes, int queryid)
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
    testNo = 0;
    recoverWriter.write(testNo + "\n");
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
      recoveredTestValue = 0;
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
