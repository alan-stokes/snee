package uk.ac.manchester.cs.snee.manager;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.anayliser.Anaylsiser;
import uk.ac.manchester.cs.snee.manager.executer.Executer;
import uk.ac.manchester.cs.snee.manager.monitor.Monitor;
import uk.ac.manchester.cs.snee.manager.planner.Planner;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;

public class AutonomicManager 
{
  private Anaylsiser anyliser;
  private Monitor monitor;
  private Planner planner;
  private Executer executer;
  private QueryExecutionPlan qep;
  private ArrayList<Integer> deadNodes = null;
  private int noDeadNodes = 0;
  private int adaptionCount = 1;
  private static Logger resultsLogger = 
    Logger.getLogger("results.autonomicManager");
  // folder for autonomic data
  private File outputFolder = new File("AutonomicManagerData");;
  //fixed parameters of autonomic calculations
  private final int numberOfTreesToUse = 10;
  
  public AutonomicManager()
  {
	  anyliser = new Anaylsiser(this);
	  monitor = new Monitor(this);
	  planner = new Planner(this);
	  executer = new Executer(this);
  }
 
  public void setQueryExecutionPlan(QueryExecutionPlan qep) 
  throws SNEEException, 
         SNEEConfigurationException, 
         SchemaMetadataException
  {
	  this.qep = qep;
	  SensorNetworkQueryPlan sQep = (SensorNetworkQueryPlan) qep;
    setupOutputFolder();
	  //Initialise other components
	  anyliser.initilise(qep, numberOfTreesToUse);
	  monitor.setQueryPlan(qep);
  }

  private void setupOutputFolder() throws SNEEConfigurationException
  {
    // TODO Auto-generated method stub
    SensorNetworkQueryPlan sQep = (SensorNetworkQueryPlan) this.qep;
    //sort out output folder
    String sep = System.getProperty("file.separator");
    String outputDir = SNEEProperties.getSetting(
        SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
        sep + sQep.getAgendaIOT().getQueryName();
    File firstOutputFolder = new File(outputDir + sep + "AutonomicManData");
    
    deleteFileContents(firstOutputFolder);
    outputFolder = new File(firstOutputFolder.toString() + sep + "Adaption" + adaptionCount);
    outputFolder.mkdir();
  }

  private void deleteFileContents(File firstOutputFolder)
  {
    if(firstOutputFolder.exists())
    {
      File[] contents = firstOutputFolder.listFiles();
      for(int index = 0; index < contents.length; index++)
      {
        File delete = contents[index];
        if(delete != null && delete.listFiles().length > 0)
          deleteFileContents(delete);
        else
          delete.delete();
      }
    }
    else
    {
      firstOutputFolder.mkdir();
    } 
    
  }

  public void runStragity2(ArrayList<String> failedNodes) 
  throws SNEEConfigurationException, 
         OptimizationException, 
         SchemaMetadataException, 
         TypeMappingException, 
         AgendaException, 
         SNEEException, 
         MalformedURLException, 
         WhenSchedulerException, 
         MetadataException, 
         UnsupportedAttributeTypeException, 
         SourceMetadataException, 
         TopologyReaderException, 
         SNEEDataSourceException, 
         CostParametersException, 
         SNCBException
  {
    SensorNetworkQueryPlan newQEP = anyliser.adapatationStrategyIntermediateSpaceAndTimePinned(failedNodes);
    //newQEP.getIOT().exportAsDotFileWithFrags(fname, label, exchangesOnSites)
    //new AgendaIOTUtils( newQEP.getAgendaIOT(), newQEP.getIOT(), true).generateImage();
  }
  
  
  public void runCostModels() 
  throws OptimizationException, 
         SNEEConfigurationException, 
         SchemaMetadataException, 
         TypeMappingException, 
         AgendaException, 
         SNEEException, 
         MalformedURLException, 
         WhenSchedulerException, 
         MetadataException, 
         UnsupportedAttributeTypeException, 
         SourceMetadataException, 
         TopologyReaderException, 
         SNEEDataSourceException, 
         CostParametersException, 
         SNCBException
  {    
    anyliser.runECMs();
    monitor.chooseFakeNodeFailure();
  }
  
  public void runAnyliserWithDeadNodes() 
  throws OptimizationException
  {
	  if(deadNodes != null)
	    anyliser.simulateDeadNodes(deadNodes);
	  else
      anyliser.simulateDeadNodes(noDeadNodes);
	  monitor.queryStarting();
	  anyliser.queryStarted();
  }
  
  public void setDeadNodes(ArrayList<Integer> deadNodes)
  {
	  this.deadNodes = deadNodes;
  }
  
  public void setNoDeadNodes(int noDeadNodes)
  {
	  this.noDeadNodes = noDeadNodes;
  }
  
  public float getCECMEpochResult() 
  throws OptimizationException
  {
    return anyliser.getCECMEpochResult();
  }
  
  public float getCECMAgendaResult() 
  throws OptimizationException
  {
    return anyliser.getCECMAgendaResult();
  }

  public void setListener(SNCBSerialPortReceiver mr)
  {
    monitor.addPacketReciever(mr);
  }
  
  public void callAnaysliserAnaylsisSNEECard(Map <Integer, Integer> sneeTuplesPerEpoch)
  {
    anyliser.anaylsisSNEECard(sneeTuplesPerEpoch);
  }

  public void setResultSet(ResultStore resultSet)
  {
    monitor.setResultSet(resultSet);
    
  }

  public void queryEnded()
  {
    monitor.queryEnded();  
  }

  //no tuples received this query
  public void callAnaysliserAnaylsisSNEECard()
  {
    anyliser.anaylsisSNEECard();
    
  }

  public void setQuery(String query)
  {
    monitor.setQuery(query);
  }
  
  public File getOutputFolder()
  {
    return outputFolder;
  }
}
