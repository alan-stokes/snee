package uk.ac.manchester.snee.client;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

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
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class UnreliableChannelClient extends SNEEClient 
{
  
  private static String sep = System.getProperty("file.separator");
  private static int queryid = 1;
  protected static int testNo = 1;
  private static File sneetestFolder =  new File("testsSize30");
  
  //private static uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator _myDataSource;

  public UnreliableChannelClient(String query, 
      double duration, String queryParams, String csvFile, String sneeProps) 
  throws SNEEException, IOException, SNEEConfigurationException 
  {
    super(query, duration, queryParams, csvFile, sneeProps);
  }

  /**
   * The main entry point for the SNEE successor client.
   * @param args
   * @throws IOException
   * @throws InterruptedException 
   */
  public static void main(String[] args) 
  {
    //This method represents the web server wrapper
    // Configure logging
  //  PropertyConfigurator.configure(
      //  SuccessorClient.class.
      //  getClassLoader().getResource("etc/common/log4j.properties"));
    
    
    Long duration = Long.valueOf("120");
    String queryParams = "etc/query-parameters.xml";
    try
    {
      recursiveRun(duration, queryParams, false);
    }
    catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      System.out.println("error message was " + e.getMessage());
      logger.fatal(e);
      e.printStackTrace();
    }
  }
   
  private static void recursiveRun(Long duration, String queryParams, 
                                   boolean allowDeathOfAcquires) 
  throws IOException 
  {
    //get query & schemas
   // String currentQuery = "SELECT * FROM DetectorA[now] a, DetectorB[now] b where a.x > b.x;";
    String currentQuery = "SELECT RSTREAM anow.x as qx FROM A[NOW] anow ;";
    String propertiesPath = sneetestFolder.toString() + sep + "snee1.properties";
    
    System.out.println("Running Tests on query " + (queryid));
    try
    {
      UnreliableChannelClient client = 
      new  UnreliableChannelClient(currentQuery, duration, queryParams, null, propertiesPath);
      //set queryid to correct id
      SNEEController contol = (SNEEController) client.controller;
      contol.setQueryID(queryid);
      client.runCompilelation();
      System.out.println("Ran all tests on query " + (queryid));
      queryid ++;
    }
    catch(Exception e)
    {
      System.out.println("something major failed");
      e.printStackTrace();
      queryid ++;
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
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS, "TRUE");
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_INITILISE_FRAMEWORKS, "FALSE");
    
    control.addQuery(_query, _queryParams,1, true, false, true);
    controller.close();
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  } 
}
