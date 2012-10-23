package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class RelibaleChannelClient extends SNEEClient 
{
	
  private static String sep = System.getProperty("file.separator");
	private static int queryid = 1;
	protected static int testNo = 1;
	private static int max = 120;
  private static File testFolder =  new File("src/main/resources/mini");
  private static File sneetestFolder =  new File("mini");
  @SuppressWarnings("unused")
  private static boolean inRecoveryMode = false;
	
	//private static uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator _myDataSource;

	public RelibaleChannelClient(String query, 
			double duration, String queryParams, String csvFile, String sneeProps) 
	throws SNEEException, IOException, SNEEConfigurationException 
	{
		super(query, duration, queryParams, csvFile, sneeProps);
	}

	/**
	 * The main entry point for the client.
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) 
	{
		//This method represents the web server wrapper
		// Configure logging
	//	PropertyConfigurator.configure(
			//	SuccessorClient.class.
			//	getClassLoader().getResource("etc/common/log4j.properties"));
		
		
	  Long duration = Long.valueOf("120");
    String queryParams = "etc/query-parameters.xml";
    Iterator<String> queryIterator;
    
    try
    {
      checkRecoveryFile();
      runIxentsScripts();
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      //collectQueries(queries);
      queries.add("SELECT RSTREAM anow.x as qx FROM A[NOW] anow ;");
      
      queryIterator = queries.iterator();
      
      //TODO remove to allow full run
      while(queryIterator.hasNext() && queryid <= max)
      {
        recursiveRun(queryIterator, duration, queryParams, true);
        removeBinaries(queryid);
      }
    }
    catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      System.out.println("error message was " + e.getMessage());
      logger.fatal(e);
      e.printStackTrace();
    }
	}
	
	 private static void checkRecoveryFile() throws IOException
	 {
	    //added to allow recovery from crash
	    File folder = new File("recovery"); 
	    String path = folder.getAbsolutePath();
	    File recoveryFile = new File(path + "/recovery.tex");
	    if(recoveryFile.exists())
	    {
	      BufferedReader recoveryTest = new BufferedReader(new FileReader(recoveryFile));
	      String recoveryQueryIdLine = recoveryTest.readLine();
	      String recoverQueryTestLine = recoveryTest.readLine();
	      System.out.println("recovery text located with query test value = " +  recoveryQueryIdLine + " and has test no = " + recoverQueryTestLine);
	      queryid = Integer.parseInt(recoveryQueryIdLine);
	      inRecoveryMode = true;
	      if(queryid == 0)
	      {
	        deleteAllFilesInResultsFolder(folder);
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
	 
	 private static void runIxentsScripts() throws IOException
	 {
	    
	     //run Ixent's modified script to produce random test cases. 
	     //if tests exist, do not redo
	     File pythonFolder = new File("src/main/resources/python/");
	     String pythonPath = pythonFolder.getAbsolutePath();
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
	 
	 private static void collectQueries(ArrayList<String> queries) throws IOException
	 {
	    //String filePath = Utils.validateFileLocation("tests/queries.txt");
	    File queriesFile = new File(testFolder.toString() + sep + "queries.txt");
	    String filePath = queriesFile.getAbsolutePath();
	    BufferedReader queryReader = new BufferedReader(new FileReader(filePath));
	    String line = "";
	    int counter = 0;
	    while((line = queryReader.readLine()) != null)
	    {
	      if(counter >= queryid)
	        queries.add(line);
	      else
	        counter++;
	    }  
	 }
	 
	 private static void updateRecoveryFile() throws IOException
	 {
	    File folder = new File("recovery"); 
	    String path = folder.getAbsolutePath();
	    //added to allow recovery from crash
	    BufferedWriter recoverWriter = new BufferedWriter(new FileWriter(new File(path + "/recovery.tex")));
	    
	    recoverWriter.write(queryid + "\n");
	    recoverWriter.flush();
	    recoverWriter.close();
	    
	 }
	 
	private static void recursiveRun(Iterator<String> queryIterator, 
                                   Long duration, String queryParams, 
                                   boolean allowDeathOfAcquires) 
  throws IOException 
  {
    //get query & schemas
    String currentQuery = queryIterator.next();
    String propertiesPath = sneetestFolder.toString() + sep + "snee" + queryid + ".properties";
    
    System.out.println("Running Tests on query " + (queryid));
    try
    {
      RelibaleChannelClient client = 
      new  RelibaleChannelClient(currentQuery, duration, queryParams, null, propertiesPath);
      //set queryid to correct id
      SNEEController contol = (SNEEController) client.controller;
      contol.setQueryID(queryid);
      //added to allow recovery from crash
      updateRecoveryFile();
      client.runCompilelation();
      System.out.println("Ran all tests on query " + (queryid));
      System.exit(0);
      queryid ++;
    }
    catch(Exception e)
    {
      System.out.println("something major failed");
      e.printStackTrace();
      queryid ++;
      System.exit(0);
      if(queryid <= max)
      {
      recursiveRun(queryIterator, duration, queryParams, allowDeathOfAcquires);
      }
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
    SNEEController control = (SNEEController) controller;
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_SUCCESSOR, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_SIM_FAILED_NODES, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_AVRORA_SIMULATOR, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_AVRORA_SIMULATOR, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_INITILISE_FRAMEWORKS, "FALSE");
    
    control.addQuery(_query, _queryParams);
    controller.close();
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }
	
	 public static void removeBinaries(int queryid) throws SNEEConfigurationException
	  {
	   String outputDir = SNEEProperties.getSetting(SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR);
	    File file = new File(outputDir + sep + "query" + queryid);
	    if(file.isDirectory())
	    {
	      String [] fileList = file.list();
	      for(int fileIndex = 0; fileIndex < fileList.length; fileIndex++)
	      {
	        removeBinaries(file.getAbsolutePath() + sep + fileList[fileIndex]);
	      }
	    }
	    
	    
	  }
	  
	  private static void removeBinaries(String filePath)
	  {
	    File file = new File(filePath);
	    if(file.isDirectory())
	    {
	      String [] fileList = file.list();
	      for(int fileIndex = 0; fileIndex < fileList.length; fileIndex++)
	      {
	        if(fileList[fileIndex].equals("avrora_micaz_t2"))
	        {
	          File binaryFile = new File(file.getAbsolutePath() + sep + fileList[fileIndex]);
	          RelibaleChannelClient.deleteFileContents(binaryFile);
	        }
	        else
	        {
	          removeBinaries(file.getAbsolutePath() + sep + fileList[fileIndex]);
	        }
	      }
	    }
	  }
	  
	  
	  /**
	   * cleaning method
	   * @param firstOutputFolder
	   */
	public static void deleteFileContents(File firstOutputFolder)
	{
    if(firstOutputFolder.exists())
    {
      File[] contents = firstOutputFolder.listFiles();
      for(int index = 0; index < contents.length; index++)
      {
        File delete = contents[index];
        if(delete.isDirectory())
          if(delete != null && delete.listFiles().length > 0)
          {
            deleteFileContents(delete);
            delete.delete();
          }
          else
            delete.delete();
        else
          delete.delete();
      }
    } 
  }	  
}