package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.client.SNEEClient;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.UtilsException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;

public class SNEECostModelClientUsingInNetworkSource extends SNEEClient 
{
	private static ArrayList<Integer> siteIDs = null;
	
	protected static Logger resultsLogger;
	private static int testNo = 1;
	private String sneeProperties;
	
	public SNEECostModelClientUsingInNetworkSource(String query, 
			double duration, String queryParams, String sneeProperties) 
	throws SNEEException, IOException, SNEEConfigurationException {
		super(query, duration, queryParams, sneeProperties);

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
		  
      //update results file to put header and query, testno
      File folder = new File("results");
      String path = folder.getAbsolutePath();
      folder.delete();
      folder.mkdir();
      File resultsFile = new File(path + "/results.tex");
      if(!resultsFile.exists())
        resultsFile.createNewFile();
      else
      {
        resultsFile.delete();
        resultsFile.createNewFile();
      }
      //add blurb for results file, so can be compiled into latex doc at later date
      BufferedWriter out = new BufferedWriter(new FileWriter(resultsFile, true));
      out.write("\\documentclass{article} \n \\usepackage{a4-mancs} \n \\usepackage{graphicx} \n" +
                "\\usepackage{mathtools} \n \\usepackage{subfig} \n \\usepackage[english]{babel} \n" +
                "\\usepackage{marvosym} \n\n\n \\begin{document} \n");
                
      out.write("\\begin{tabular}{|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|p{2cm}|}  \n \\hline \n");
      out.write("testNo &  dead Sites List & Cost Model Epoch Cardinality & Cost Model Angeda Cardinality & " +
                " Snee Epoch Cardinality & Snee Agenda Cardinality & correct \\\\ \\hline \n");
      out.flush();
      out.close();
    
      
      //run Ixent's modified script to produce 30 random test cases. 
     // String pythonPath = Utils.validateFileLocation("resources/python/generateScenarios.py");
      File pythonFolder = new File("src/main/resources/python/");
      String pythonPath = pythonFolder.getAbsolutePath();
      File testFolder = new File("src/main/resources/tests");
      String testPath = testFolder.getAbsolutePath();
      String [] params = {"generateScenarios.py", testPath};
      Map<String,String> enviro = new HashMap<String, String>();
      System.out.println("running Ixent's scripts for 30 random queries");
      Utils.runExternalProgram("python", params, enviro, pythonPath);
      System.out.println("have ran Ixent's scripts for 30 random queries");
      
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      //String filePath = Utils.validateFileLocation("tests/queries.txt");
      File queriesFile = new File("src/main/resources/tests/queries.txt");
      String filePath = queriesFile.getAbsolutePath();
      BufferedReader queryReader = new BufferedReader(new FileReader(filePath));
      String line = "";
      while((line = queryReader.readLine()) != null)
      {
        queries.add(line);
      }
      
      int queryid = 0;
  		Iterator<String> queryIterator = queries.iterator();
  		while(queryIterator.hasNext())
  		{
  		  //get query & schemas
  		  String currentQuery = queryIterator.next();
  		  String propertiesPath = "tests/snee" + queryid + ".properties";
  		  
        SNEECostModelClientUsingInNetworkSource client = 
          new  SNEECostModelClientUsingInNetworkSource(currentQuery, duration, queryParams, propertiesPath);
        out = new BufferedWriter(new FileWriter(resultsFile, true));
        out.write(testNo + " &  \\multicolumn{6}{|c|}{" + currentQuery + "} \\\\ \\hline \n");
        out.flush();
        out.close();
        client.run();
        testNo ++;
        siteIDs = client.getSties();
        System.out.println("ran control: success");
        runTests(client, currentQuery);
        queryid ++;
        System.out.println("Ran to completion");
  		}
  		out = new BufferedWriter(new FileWriter(path, true));
      out.write("\\end{tabular} \n \\end{document} \n");
      out.flush();
      out.close();
    } catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      logger.fatal(e);
      e.printStackTrace();
      System.exit(1);
    }
	}

  private static void runTests(SNEECostModelClientUsingInNetworkSource client, String currentQuery) throws SNEECompilerException, MetadataException, EvaluatorException, SNEEException, SNEEConfigurationException, IOException, OptimizationException, SQLException, UtilsException
  {
    //go though all site combinations running with snee to test validity
    int noSites = siteIDs.size();
    int position = 0;
    ArrayList<Integer> deadNodes = new ArrayList<Integer>();
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
      client.setDeadNodes(deadNodes);
      client.runForTests();
    }
  }
  
  public void runForTests()throws SNEECompilerException, MetadataException, EvaluatorException,
  SNEEException, SQLException, SNEEConfigurationException
  {
    if (logger.isDebugEnabled()) 
      logger.debug("ENTER");
    System.out.println("Query: " + _query);
    SNEEController control = (SNEEController) controller;
    int queryId1 = control.addQueryWithoutCompile(_query, _queryParams);
    

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
}
