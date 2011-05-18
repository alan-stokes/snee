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
	private static final int MAXQUERYTEST = 30;
  private static ArrayList<Integer> siteIDs = new ArrayList<Integer>();
	private static RT routingTree;
	
	protected static Logger resultsLogger;
	@SuppressWarnings("unused")
  private String sneeProperties;
	private static int recoveredTestValue = 0;
	private static boolean inRecoveryMode = false;
	private static int actualTestNo = 0;
	private static int queryid = 0;
	private static int queryidSinceStart = 1;
	
	public SNEECostModelClientUsingInNetworkSource(String query, 
			double duration, String queryParams, String sneeProperties) 
	throws SNEEException, IOException, SNEEConfigurationException {
		super(query, duration, queryParams, sneeProperties, inRecoveryMode);

		if (logger.isDebugEnabled()) 
			logger.debug("ENTER SNEECostModelClientUsingInNetworkSource()");		
		//Set sleep to 10 seconds
		_sleepDuration = 10000;
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
        SNEECostModelClientUsingInNetworkSource.class.
        getClassLoader().getResource("etc/common/log4j.properties"));   
	  
		Long duration = Long.valueOf("120");
		String queryParams = "etc/query-parameters.xml";
		try
		{
		  //check if recovery file exists, and if so collect recovery info
      checkRecoveryFile();
      //set up crash file, so if system crashes can be determined after run ends
      setupCrashFile();
      //if not already produced beginning of results latex doc, produce
      if(!inRecoveryMode)
      {
        writeBeginningBlurb();
      }
      //run scripts to produce test scenarios.
      runIxentsScripts();
      
      
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      collectQueries(queries);
      
	    Iterator<String> queryIterator = queries.iterator();
	  
	    moveQueryToRecoveryLocation(queries);
	    //set up all running sections
	    initialise(queryIterator, duration, queryParams);
	    //finished all tests, nothing to do, exit.
	    clearRecoveryFile();
	    System.exit(0);
	    
    } 
		catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      logger.fatal(e);
      e.printStackTrace();
      System.exit(1);
    }
	}

	private static void clearRecoveryFile() throws IOException
  {
	  File folder = new File("results"); 
    String path = folder.getAbsolutePath();
    File recoveryFile = new File(path + "/recovery.tex");
    recoveryFile.delete();
  }

  private static void setupCrashFile()
  {
	  try
    {
      File folder = new File("results");
      String path = folder.getAbsolutePath();
      File crashFile = new File(path + "/crash.tex");
      if(!crashFile.exists())
      {
        crashFile.createNewFile();
      }
      else
      {
        crashFile.delete();
        crashFile.createNewFile();
      }
      
      BufferedWriter out = new BufferedWriter(new FileWriter(crashFile, true));
      out.write("\\documentclass{article} \n \\usepackage{a4-mancs} \n \\usepackage{graphicx} \n" +
          "\\usepackage{mathtools} \n \\usepackage{subfig} \n \\usepackage[english]{babel} \n" +
          "\\usepackage{marvosym} \n\n\n \\begin{document} \n");
        
      out.write("\\begin{tabular}{|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|}  \n \\hline \n");
      out.write("queryID &  testNo & query & error message & error stack trace \\\\ \\hline \n");
      out.flush();
      out.close();
    }
	  catch(Exception e)
	  {
      System.out.println("unexpected error when writing to crash.tex");
      e.printStackTrace();
      System.exit(1);
	  }
    
  }

  private static void initialise(Iterator<String> queryIterator, Long duration, String queryParams)
	{
	  String currentQuery = "";
	  try
	  {
	    while(queryIterator.hasNext())
      {
        //get query & schemas
        currentQuery = queryIterator.next();
        String propertiesPath = "tests/snee" + queryid + ".properties";
        
        SNEECostModelClientUsingInNetworkSource client = 
          new  SNEECostModelClientUsingInNetworkSource(currentQuery, duration, queryParams, propertiesPath);
        
        writeIncludeImageSection();
        writeQueryToResultsFile(currentQuery);
        
        //added to allow recovery from crash
        actualTestNo = 0;
        updateRecoveryFile();
        
        client.run(queryid);
        routingTree = client.getRT();
        System.out.println("ran control: success");
        runTests(client, currentQuery);
        writeCloseTabularSection();
        queryid ++;
        queryidSinceStart++;
        System.out.println("Ran all tests on query " + queryid);
      }
      writeLastLatexSection("results.tex");
      writeLastLatexSection("crash.tex");
	  }
	  catch(Exception e)//system crashes for some unknown reason. skip query, keep testing, record error for later dealment
	  {
	    System.out.println("system failed at query " + queryid + " please refer to errors.txt located in results folder");
	    storeCrashInfo(currentQuery, actualTestNo, e);
	    queryid++;
	    inRecoveryMode = false;
	    recoveredTestValue = 0;
	    if(queryid < MAXQUERYTEST)
	      initialise(queryIterator, duration, queryParams);
	  }
	}

  private static void writeCloseTabularSection() throws IOException
  {
    File folder = new File("results");
    String path = folder.getAbsolutePath();
    File resultsFile = new File(path + "/" + "results.tex");
    BufferedWriter out = new BufferedWriter(new FileWriter(resultsFile, true));
    out.write("\\end{tabular} \n \\caption{table of results for topology " + queryid + "}\n");
    out.write("\\end{table}\n\n \\clearpage \n\n");
    out.flush();
    out.close();
  }

  private static void storeCrashInfo(String currentQuery, int actualTestNo, Exception e)
  {
    try
    {
      File folder = new File("results");
      String path = folder.getAbsolutePath();
      File crashFile = new File(path + "/crash.tex");
      BufferedWriter out = new BufferedWriter(new FileWriter(crashFile, true));
      StackTraceElement [] trace = e.getStackTrace();
      out.write(queryid + " & " + actualTestNo + " & " + currentQuery + " & " + e.getMessage() + " & ");
      for(int line = 0; line < trace.length; line++)
      {
        out.write(trace[line].toString() + "\\newline ");
      }
      out.write("\\\\ \\hline \n");
      out.flush();
      out.close();
    }
    catch(Exception f)
    {
      System.out.println("unexpected error when writing to crash.tex");
      f.printStackTrace();
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

  private static void runTests(SNEECostModelClientUsingInNetworkSource client, String currentQuery) 
  throws SNEECompilerException, MetadataException, EvaluatorException, SNEEException, 
  SNEEConfigurationException, IOException, OptimizationException, SQLException, UtilsException
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
    chooseNodes(deadNodes, noSites, position, client, currentQuery);
    copyRTtoResultsFolder();
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

  @SuppressWarnings("static-access")
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
      Thread.currentThread().yield();
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
              "\\usepackage{marvosym} \n \\usepackage{multirow} \n \\usepackage{longtable} \n\n\n " +
              "\\begin{document} \n");    
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
      out.write("\\begin{table} \n");
      out.write("\\begin{tabular}{|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{1cm}|}  \n \\hline \n");
      out.write("dead Sites List & CM Epoch & CM Angeda & " +
              " Snee Epoch & Snee Agenda & correct \\\\ \\hline \n");
      out.write("\\multicolumn{6}{|c|}{" + currentQuery + "} \\\\ \\hline \n");
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
    out.write( "\\begin{figure} \n ");
    out.write("\\includegraphics[scale=0.5]{query" + queryid + "-RT-" + queryid + "} \n");
    out.write("\\caption{topology for test " + queryid + "} \n");
    out.write("\\end{figure} \n");
    out.flush();
    out.close();
  }
  
  private static void copyRTtoResultsFolder() throws UtilsException
  {
    String path = Utils.validateFileLocation("output/query" + queryid + "/query-plan/");
    File rtDotFile = new File(path + "/query" + queryid  + "-RT-" + queryidSinceStart + ".dot");
    File resultsFolder = new File("results");
    String resultsPath = resultsFolder.getAbsolutePath();
    File movedFile = new File(resultsPath + "/query"+ queryid  + "-RT-" + queryid  + ".dot");
    boolean success = rtDotFile.renameTo(movedFile);
    if(!success)
      System.out.println("attemptt to move image file '" + queryid  + "-RT-" + queryidSinceStart + ".dot'"+ "for query " + queryid + " failed");
  }

  private static void writeLastLatexSection(String fileName) throws IOException
  {
    File folder = new File("results");
    String path = folder.getAbsolutePath();
    File resultsFile = new File(path + "/" + fileName);
    BufferedWriter out = new BufferedWriter(new FileWriter(resultsFile, true));
    out = new BufferedWriter(new FileWriter(path, true));
    out.write("\\hline \n");
    out.write("\\caption{table showing results for topology " + queryid + "} \n");
    out.write("\\end{tabular} \n \\end{table} \n \\end{document} \n");
    out.flush();
    out.close();
  }
  
  private static void updateRecoveryFile() throws IOException
  {
    File folder = new File("results"); 
    String path = folder.getAbsolutePath();
    //added to allow recovery from crash
    BufferedWriter recoverWriter = new BufferedWriter(new FileWriter(new File(path + "/recovery.tex")));
    recoverWriter.write(queryid + "\n");
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
      if(recoveryQueryIdLine != null)
        queryid = Integer.parseInt(recoveryQueryIdLine);
      else
        queryid = 0;
      if(recoverQueryTestLine != null)
        recoveredTestValue = Integer.parseInt(recoverQueryTestLine);
      else
        recoveredTestValue = 0;
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
      queryid = 0;
      recoveredTestValue = 0;
      deleteAllFilesInResultsFolder(folder);
      resultsFile.createNewFile();
      recoveryFile.createNewFile();
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
