package uk.ac.manchester.snee.client;

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
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class CondorSuccessorClient extends SNEEClient 
{
  
  private static int queryid = 1;
  protected static int testNo = 1;
  @SuppressWarnings("unused")
  private static boolean inRecoveryMode = false;
  
  //private static uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator _myDataSource;

  public CondorSuccessorClient(String query, 
      double duration, String queryParams, String csvFile, String sneeProps) 
  throws SNEEException, IOException, SNEEConfigurationException 
  {
    super(query, duration, queryParams, csvFile, sneeProps);
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
      int nodeFailureCount = Integer.parseInt(args[3]);
      //File output = new File("output");
      //output.mkdir();
      //File result = new File(output.toString() + "/" + "ran" + query + queryid);
      //result.mkdir();
     // System.out.println("made folder output and " + output.toString() + "/" + "ran" + query + queryid);
      recursiveRun(query, duration, queryParams, true, propertiesPath, nodeFailureCount) ;
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
                                   boolean allowDeathOfAcquires, String propertiesPath,
                                   int nodeFailureCount) 
  throws IOException 
  {
    System.out.println("Running Tests on query " + (queryid));
    try
    {
      System.out.println("initisling client");
      CondorSuccessorClient client = 
      new  CondorSuccessorClient(currentQuery, duration, queryParams, null, propertiesPath);
      //set queryid to correct id
      System.out.println("getting controller");
      SNEEController contol = (SNEEController) client.getController();
      System.out.println("setting queryid");
      contol.setQueryID(queryid);
      System.out.println("running compilation");
      client.runCompilelation(nodeFailureCount);
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

  private void runCompilelation(Integer nodeFailureCount) 
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
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_SUCCESSOR, "TRUE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_SIM_FAILED_NODES, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_AVRORA_SIMULATOR, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_AVRORA_SIMULATOR, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_INITILISE_FRAMEWORKS, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_SUCCESSOR_NODE_FAILURES, nodeFailureCount.toString());
    
    control.addQuery(_query, _queryParams);
    getController().close();
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }  
}