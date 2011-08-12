package uk.ac.manchester.cs.snee.manager;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.anayliser.Anaylsiser;
import uk.ac.manchester.cs.snee.manager.executer.Executer;
import uk.ac.manchester.cs.snee.manager.monitor.Monitor;
import uk.ac.manchester.cs.snee.manager.planner.Planner;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;

public class AutonomicManagerImpl implements AutonomicManager 
{
  private Anaylsiser anyliser;
  private Monitor monitor;
  private Planner planner;
  private Executer executer;
  private QueryExecutionPlan qep;
  private MetadataManager _metadataManager;
  private ArrayList<Integer> deadNodes = null;
  private int noDeadNodes = 0;
  private int adaptionCount = 1;
  //private static Logger resultsLogger = Logger.getLogger("results.autonomicManager");
  // folder for autonomic data
  private File outputFolder = new File("AutonomicManagerData");;
  //fixed parameters of autonomic calculations
  private final int numberOfTreesToUse = 10;
  
  public AutonomicManagerImpl(MetadataManager _metadataManager)
  {
    this._metadataManager = _metadataManager;    
    monitor = new Monitor(this);
    planner = new Planner(this);
    executer = new Executer(this);
  }
  
  public void initilise(SourceMetadataAbstract _metadata, QueryExecutionPlan qep, 
                        ResultStore resultSet) 
  throws SNEEException, SNEEConfigurationException, 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException
  {
    this.qep = qep;
    setupOutputFolder();
    anyliser = new Anaylsiser(this, _metadata, _metadataManager);
    monitor.setQueryPlan(qep);
    monitor.setResultSet(resultSet);
    anyliser.initilise(qep, numberOfTreesToUse);
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

  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#runStragity2(java.util.ArrayList)
   */
  @Override
  public void runFailedNodeFramework(ArrayList<String> failedNodes) 
  throws SNEEConfigurationException, OptimizationException, 
         SchemaMetadataException, TypeMappingException, 
         AgendaException, SNEEException, 
         MetadataException, 
         UnsupportedAttributeTypeException, SourceMetadataException, 
         TopologyReaderException, SNEEDataSourceException, 
         CostParametersException, SNCBException, 
         SNEECompilerException, IOException
  {
    List<Adapatation> choices = anyliser.runFailedNodeFramework(failedNodes);
    new AdapatationUtils(choices, _metadataManager.getCostParameters()).FileOutput(outputFolder);
    new AdapatationUtils(choices, _metadataManager.getCostParameters()).systemOutput();
    Adapatation finalChoice = planner.assessChoices(choices);
    new AdapatationUtils(finalChoice,  _metadataManager.getCostParameters()).FileOutputFinalChoice(outputFolder);
    executer.adapt(finalChoice);
    //newQEP.getIOT().exportAsDotFileWithFrags(fname, label, exchangesOnSites)
    //new AgendaIOTUtils( newQEP.getAgendaIOT(), newQEP.getIOT(), true).generateImage();
  }
  
  
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#runCostModels()
   */
  @Override
  public void runCostModels() throws OptimizationException 

  {   
    System.out.println("running cost model estimates");
    anyliser.runECMs();
  }
  
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#runAnyliserWithDeadNodes()
   */
  @Override
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
  
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#setDeadNodes(java.util.ArrayList)
   */
  @Override
  public void setDeadNodes(ArrayList<Integer> deadNodes)
  {
	  this.deadNodes = deadNodes;
  }
  
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#setNoDeadNodes(int)
   */
  @Override
  public void setNoDeadNodes(int noDeadNodes)
  {
	  this.noDeadNodes = noDeadNodes;
  }
  
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#getCECMEpochResult()
   */
  @Override
  public float getCECMEpochResult() 
  throws OptimizationException
  {
    return anyliser.getCECMEpochResult();
  }
  
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#getCECMAgendaResult()
   */
  @Override
  public float getCECMAgendaResult() 
  throws OptimizationException
  {
    return anyliser.getCECMAgendaResult();
  }
  
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#callAnaysliserAnaylsisSNEECard(java.util.Map)
   */
  @Override
  public void callAnaysliserAnaylsisSNEECard(Map <Integer, Integer> sneeTuplesPerEpoch)
  {
    anyliser.anaylsisSNEECard(sneeTuplesPerEpoch);
  }

  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#queryEnded()
   */
  @Override
  public void queryEnded()
  {
    monitor.queryEnded();  
  }

  //no tuples received this query
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#callAnaysliserAnaylsisSNEECard()
   */
  @Override
  public void callAnaysliserAnaylsisSNEECard()
  {
    anyliser.anaylsisSNEECard();
    
  }

  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#setQuery(java.lang.String)
   */
  @Override
  public void setQuery(String query)
  {
    monitor.setQuery(query);
  }
  
  /* (non-Javadoc)
   * @see uk.ac.manchester.cs.snee.manager.AutonomicManager#getOutputFolder()
   */
  @Override
  public File getOutputFolder()
  {
    return outputFolder;
  }  
  
  public void setListener(SNCBSerialPortReceiver mr)
  {
    monitor.addPacketReciever(mr);
  }

  /**
   * method used to simulate test data
   */
  @Override
  public void runSimulatedNodeFailure() throws OptimizationException,
      SNEEConfigurationException, SchemaMetadataException,
      TypeMappingException, AgendaException, SNEEException,
      MalformedURLException, MetadataException,
      UnsupportedAttributeTypeException, SourceMetadataException,
      TopologyReaderException, SNEEDataSourceException,
      CostParametersException, SNCBException, SNEECompilerException,
      IOException
  {
    monitor.chooseFakeNodeFailure();
  }

}
