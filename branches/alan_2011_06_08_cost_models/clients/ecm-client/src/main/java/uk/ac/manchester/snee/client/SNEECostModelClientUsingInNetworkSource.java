package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStoreImpl;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClient;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.UtilsException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class SNEECostModelClientUsingInNetworkSource extends SNEEClient 
{
	private static ArrayList<Integer> siteIDs = new ArrayList<Integer>();
	private static RT routingTree;
	
	@SuppressWarnings("unused")
  private static final Logger resultsLogger = Logger.getLogger(SNEECostModelClientUsingInNetworkSource.class.getName());
	//private String sneeProperties;
	private static int testNo = 1;
	private static int recoveredTestValue = 0;
	private static boolean inRecoveryMode = false;
	private static int actualTestNo = 0;
	private static int queryid = 0;
	
	public SNEECostModelClientUsingInNetworkSource(String query, 
			double duration, String queryParams, String csvFile, String sneeProperties) 
	throws SNEEException, IOException, SNEEConfigurationException {
		
		//super(query, duration, queryParams, csvFile, sneeProperties);
		super(query, duration, queryParams, sneeProperties);
		
		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEECostModelClientUsingInNetworkSource()");		
		//this.sneeProperties = sneeProperties;
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
        SNEECostModelClientUsingInNetworkSource.class.
        getClassLoader().getResource("etc/common/log4j.properties"));   
	  
		Long duration = Long.valueOf("120");
		String queryParams = "etc/query-parameters.xml";
		try
    {
      checkRecoveryFile();
      if(!inRecoveryMode)
      {
        writeBeginningBlurb();
      }
      runIxentsScripts();
      
      
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      collectQueries(queries);
      
	    Iterator<String> queryIterator = queries.iterator();
	  
	    moveQueryToRecoveryLocation(queries);
	   
	    while(queryIterator.hasNext())
	    {
  		  //get query & schemas
  		  String currentQuery = queryIterator.next();
  		  String propertiesPath = "tests/snee" + queryid + ".properties";
  		  
        SNEECostModelClientUsingInNetworkSource client = 
          new  SNEECostModelClientUsingInNetworkSource(currentQuery, duration, queryParams, null, propertiesPath);
        
        writeQueryToResultsFile(currentQuery);
        testNo ++;
        
        //added to allow recovery from crash
        actualTestNo = 0;
        updateRecoveryFile();
        
        client.run();
        routingTree = client.getRT();
        System.out.println("ran control: success");
        runTests(client, currentQuery);
        queryid ++;
        System.out.println("Ran all tests on query " + testNo);
      }
	    writeLastResultsSection();
    } catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      logger.fatal(e);
      e.printStackTrace();
      System.exit(1);
    }
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

  private static void runTests(SNEECostModelClientUsingInNetworkSource client, String currentQuery) throws SNEECompilerException, MetadataException, EvaluatorException, SNEEException, SNEEConfigurationException, IOException, OptimizationException, SQLException, UtilsException
  {
    //go though all sites looking for confluence sites which are sites which will cause likely changes to results when lost
	  Iterator<Site> siteIterator = routingTree.siteIterator(TraversalOrder.POST_ORDER);
  	siteIDs.clear();
  	actualTestNo = 0;
  	while(siteIterator.hasNext())
  	{
  	  Site currentSite = siteIterator.next();
  	  if(currentSite.getInDegree() > 1)
  		  if(!siteIDs.contains(Integer.parseInt(currentSite.getID())))
  		    siteIDs.add(Integer.parseInt(currentSite.getID()));
  	}
  	 
    int noSites = siteIDs.size();
    int position = 0;
    ArrayList<Integer> deadNodes = new ArrayList<Integer>();
    writeIncludeImageSection();
    
    chooseNodes(deadNodes, noSites, position, client, currentQuery);
  }

  private static void chooseNodes(ArrayList<Integer> deadNodes, int noSites,
      int position, SNEECostModelClientUsingInNetworkSource client, String currentQuery) throws SNEECompilerException, MetadataException, EvaluatorException, SNEEException, SNEEConfigurationException, IOException, OptimizationException, SQLException, UtilsException
  {
    if(position < noSites)
    {
      chooseNodes(deadNodes, noSites, position + 1, client, currentQuery);
      deadNodes.add(siteIDs.get(position));
      chooseNodes(deadNodes, noSites, position + 1, client, currentQuery);
      deadNodes.remove(deadNodes.size() -1);
    }
    else
    {
      updateRecoveryFile();
        
      if(inRecoveryMode)
      {
    	  if(actualTestNo != recoveredTestValue)
    	  {
    		  actualTestNo++;
    	  }
    	  else
    	  {
      		inRecoveryMode = false;
      		client.resetNodes();
          client.setDeadNodes(deadNodes);
          client.runForTests(); 
    	  }
      }
      else
      {
        if(actualTestNo == 0)
        {
          actualTestNo++;
        }
        else
        {
      	  client.resetNodes();
          client.setDeadNodes(deadNodes);
          client.runForTests(); 
          actualTestNo++;
        }
      }      
    }
  }

  private void resetNodes() 
  {
	  Iterator<Site> routingTreeIterator = routingTree.siteIterator(TraversalOrder.POST_ORDER);
	  while(routingTreeIterator.hasNext())
	  {
		  Site currentSite = routingTreeIterator.next();
		  currentSite.setisDead(false);
	  }
  }

  public void runForTests()throws SNEECompilerException, MetadataException, EvaluatorException,
  SNEEException, SQLException, SNEEConfigurationException
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    SNEEController control = (SNEEController) controller;
    int queryId1 = control.addQueryWithoutCompilation(_query, _queryParams);
    

    long startTime = System.currentTimeMillis();
    long endTime = (long) (startTime + (_duration * 1000));

    System.out.println("Running query for " + _duration + " seconds. Scheduled end time " + new Date(endTime));

    ResultStoreImpl resultStore = 
      (ResultStoreImpl) controller.getResultStore(queryId1);
    resultStore.addObserver(this);
    
    try {     
      control = (SNEEController) controller;
      control.waitForQueryEnd();
    } catch (InterruptedException e) {
    }
    
    while (System.currentTimeMillis() < endTime) {
      Thread.currentThread();
      Thread.yield();
    }
    
  //  List<ResultSet> results1 = resultStore.getResults();
    System.out.println("Stopping query " + queryId1 + ".");
    controller.removeQuery(queryId1);
    controller.close();
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }

  private static void writeBeginningBlurb() throws IOException 
  {
	  File folder = new File("results");
    String path = folder.getAbsolutePath();
	  File resultsFile = new File(path + "/results.tex");
	  
	  BufferedWriter out = new BufferedWriter(new FileWriter(resultsFile, true));
	  //add blurb for results file, so can be compiled into latex doc at later date
    out.write("\\documentclass{article} \n \\usepackage{a4-mancs} \n \\usepackage{graphicx} \n" +
              "\\usepackage{mathtools} \n \\usepackage{subfig} \n \\usepackage[english]{babel} \n" +
              "\\usepackage{marvosym} \n\n\n \\begin{document} \n");
            
    out.write("\\begin{tabular}{|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|}  \n \\hline \n");
    out.write("testNo &  dead Sites List & Cost Model Epoch Cardinality & Cost Model Angeda Cardinality & " +
            " Snee Epoch Cardinality & Snee Agenda Cardinality & correct \\\\ \\hline \n");
    out.flush();
    out.close();	
  }
  
  private static void writeQueryToResultsFile(String currentQuery) throws IOException
  {
    File folder = new File("results");
    String path = folder.getAbsolutePath();
    File resultsFile = new File(path + "/results.tex");
    BufferedWriter out = new BufferedWriter(new FileWriter(resultsFile, true));
    
    if(!inRecoveryMode || recoveredTestValue == 0)
    {
      out = new BufferedWriter(new FileWriter(resultsFile, true));
      out.write(testNo + " &  \\multicolumn{6}{|c|}{" + currentQuery + "} \\\\ \\hline \n");
      out.write("\\hline \n");
      out.flush();
      out.close();
    } 
  }
  
  private static void writeIncludeImageSection() throws IOException, UtilsException
  {
    File folder = new File("results");
    String path = folder.getAbsolutePath();
    File resultsFile = new File(path + "/results.tex");
    BufferedWriter out = new BufferedWriter(new FileWriter(resultsFile, true));
    int numberOfTests = (int) Math.pow(2.0, (double) siteIDs.size()) -1;
    int realQueryid = queryid+1;
    out.write("\\multirow{" + numberOfTests + "}{*}{\\includegraphics[width=1.6cm}{query" + realQueryid + "-RT-" + realQueryid + "}");
    out.flush();
    out.close();
    copyRTtoResultsFolder();
  }
  
  private static void copyRTtoResultsFolder() throws UtilsException
  {
    int realQueryid = queryid+1;
    String path = Utils.validateFileLocation("output/query" + realQueryid + "/query-plan/");
    File rtDotFile = new File(path + "/query" + realQueryid  + "-RT-" + realQueryid + ".dot");
    File resultsFolder = new File("results");
    String resultsPath = resultsFolder.getAbsolutePath();
    File movedFile = new File(resultsPath + "/query"+ realQueryid  + "-RT-" + realQueryid + ".dot");
    boolean success = rtDotFile.renameTo(movedFile);
    if(!success)
      System.out.println("attemptt o move file failed");
  }

  private static void writeLastResultsSection() throws IOException
  {
    File folder = new File("results");
    String path = folder.getAbsolutePath();
    File resultsFile = new File(path + "/results.tex");
    BufferedWriter out = new BufferedWriter(new FileWriter(resultsFile, true));
    out = new BufferedWriter(new FileWriter(path, true));
    out.write("\\end{tabular} \n \\end{document} \n");
    out.flush();
    out.close();
  }
  
  private static void updateRecoveryFile() throws IOException
  {
    File folder = new File("results"); 
    String path = folder.getAbsolutePath();
    //added to allow recovery from crash
    BufferedWriter recoverWriter = new BufferedWriter(new FileWriter(new File(path + "/recovery.tex")));
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
        deleteAllFilesInResultsFolder(folder);
        resultsFile.createNewFile();
        recoveryFile.createNewFile();
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
