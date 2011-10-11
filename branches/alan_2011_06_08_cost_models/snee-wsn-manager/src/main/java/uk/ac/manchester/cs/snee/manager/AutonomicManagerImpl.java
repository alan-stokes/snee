package uk.ac.manchester.cs.snee.manager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.anayliser.Anaylsiser;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationCollection;
import uk.ac.manchester.cs.snee.manager.common.AdaptationUtils;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyID;
import uk.ac.manchester.cs.snee.manager.executer.Executer;
import uk.ac.manchester.cs.snee.manager.monitor.Monitor;
import uk.ac.manchester.cs.snee.manager.planner.ChoiceAssessor;
import uk.ac.manchester.cs.snee.manager.planner.Planner;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;

public class AutonomicManagerImpl implements AutonomicManager, Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 4795008182764122006L;
  private final String sep = System.getProperty("file.separator");
  private Anaylsiser anyliser;
  private Monitor monitor;
  private Planner planner;
  private Executer executer;
  //data structures needed for manager
  private QueryExecutionPlan qep;
  private MetadataManager _metadataManager;
  private SourceMetadataAbstract _metadata;
  private QoSExpectations queryQoS;
  
  //data stores
  private ArrayList<String> deadNodes = null;
  private int noDeadNodes = 0;
  private int adaptionCount = 1;
  private String queryName = "";
  
  // folder for autonomic data
  private File outputFolder = new File("AutonomicManagerData");
  private HashMap<String, RunTimeSite> runningSites;
  
  //private final static Logger resultsLogger = Logger.getLogger(AutonomicManagerImpl.class.getName());

  //fixed parameters of autonomic calculations
  private final int numberOfTreesToUse = 48;
  
  public AutonomicManagerImpl(MetadataManager _metadataManager)
  {
    this._metadataManager = _metadataManager; 
    monitor = new Monitor(this);
    executer = new Executer(this);
  }
  
  public void initilise(SourceMetadataAbstract _metadata, QueryExecutionPlan qep, 
                        ResultStore resultSet, int queryid) 
  throws SNEEException, SNEEConfigurationException, 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException, CodeGenerationException
  {
    this.qep = qep;
    queryName = "query" + queryid;
    setupOutputFolder();
    this._metadata = _metadata;
    runningSites = new HashMap<String, RunTimeSite>();
    anyliser = new Anaylsiser(this, _metadata, _metadataManager);
    planner = new Planner(this, _metadata, _metadataManager);
    monitor.setQueryPlan(qep);
    monitor.setResultSet(resultSet);
    anyliser.initilise(qep, numberOfTreesToUse);
    setupRunningSites();
  }

  private void setupRunningSites() 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException, IOException, CodeGenerationException
  {
    Iterator<Node> siteIterator = this.getWsnTopology().getNodes().iterator();
    while(siteIterator.hasNext())
    {
      Site currentSite = (Site) siteIterator.next();
      Double energyStock = new Double(currentSite.getEnergyStock() / 1000);// (mj -> J)
      Double qepExecutionCost = anyliser.calculateQepRunningCostForSite(currentSite);
      runningSites.put(currentSite.getID(), 
                       new RunTimeSite(energyStock,currentSite,qepExecutionCost));
    }
    
    //remove OTA effects from running sites
    SensorNetworkQueryPlan sqep = (SensorNetworkQueryPlan)qep;
    Adaptation orgianlOTAProgramCost = new Adaptation(sqep, StrategyID.Orginal, 0);
    Iterator<Integer> siteIdIterator = sqep.getRT().getSiteIDs().iterator();
    while(siteIdIterator.hasNext())
    {
      Integer siteIDInt = siteIdIterator.next();
      orgianlOTAProgramCost.addReprogrammedSite(siteIDInt.toString());
    }
    orgianlOTAProgramCost.setNewQep(sqep);
    File output = new File(outputFolder + sep + "OTASection");
    output.mkdir();
    ChoiceAssessor assessor =  new ChoiceAssessor(_metadata, _metadataManager, output);
    assessor.assessChoice(orgianlOTAProgramCost, runningSites, false);
    // update running sites energy stores
    siteIdIterator = sqep.getRT().getSiteIDs().iterator();
    while(siteIdIterator.hasNext())
    {
      Integer siteIDInt = siteIdIterator.next();
      runningSites.get(siteIDInt.toString()).removeReprogrammingCostCost();
      runningSites.get(siteIDInt.toString()).resetEnergyCosts();
    }  
  }

  private void setupOutputFolder() throws SNEEConfigurationException
  {
    //sort out output folder
    String outputDir = SNEEProperties.getSetting(
        SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
        sep + queryName;
    setQueryName(queryName);
    outputFolder = new File(outputDir + sep + "AutonomicManData");
    deleteFileContents(outputFolder);
  }

  private void setupAdapatationFolder() throws SNEEConfigurationException
  {
    String outputDir = SNEEProperties.getSetting(
        SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) + sep + queryName;
    File firstOutputFolder = new File(outputDir + sep + "AutonomicManData");
    outputFolder = new File(firstOutputFolder.toString() + sep + "Adaption" + adaptionCount);
    outputFolder.mkdir();
  }

  /**
   * cleaning method
   * @param firstOutputFolder
   */
  public void deleteFileContents(File firstOutputFolder)
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

  /**
   * helper method to get topology from the qep
   * @return topology
   */
  public Topology getWsnTopology()
  {
	SensorNetworkSourceMetadata metadata = (SensorNetworkSourceMetadata) _metadata;
    Topology network = metadata.getTopology();
    return network;
  }
  
  
  /**
   * used to run failed node framework
   * @throws AutonomicManagerException 
   */
  @Override
  public void runFailedNodeFramework(ArrayList<String> failedNodes) 
  throws SNEEConfigurationException, OptimizationException, 
         SchemaMetadataException, TypeMappingException, 
         AgendaException, SNEEException, 
         MetadataException, CodeGenerationException,
         UnsupportedAttributeTypeException, SourceMetadataException, 
         TopologyReaderException, SNEEDataSourceException, 
         CostParametersException, SNCBException, 
         SNEECompilerException, IOException, AutonomicManagerException
  {
    setupAdapatationFolder();
    anyliser.updateFrameWorkStorageLocation(outputFolder);
    planner.updateStorageLocation(outputFolder);
    removeFailedNodesFromRunningNodes(failedNodes);
    recordFailedNodes(failedNodes);
    setFailedNodesToDeadInQEP(failedNodes);
    AdaptationCollection choices = anyliser.runFailedNodeStragities(failedNodes);
    if(choices.getSize() !=0)
    {
      new AdaptationUtils(choices.getAll(), _metadataManager.getCostParameters()).FileOutput(outputFolder);
      new AdaptationUtils(choices.getAll(), _metadataManager.getCostParameters()).systemOutput();
      Adaptation finalChoice = planner.assessChoices(choices);
      new AdaptationUtils(finalChoice,  _metadataManager.getCostParameters()).FileOutputFinalChoice(outputFolder);
      executer.adapt(finalChoice);
    }
    else
    {
      throw new AutonomicManagerException("strategies can not produce any adaptations");
    }
    adaptionCount++;

  }
  
  /**
   * method used to define dead olds in qep. (used in cardianlity cost models
   * @param failedNodes
   */
  private void setFailedNodesToDeadInQEP(ArrayList<String> failedNodes)
  {
    Iterator<String> failedNodesIterator = failedNodes.iterator();
    SensorNetworkQueryPlan sqep = (SensorNetworkQueryPlan) qep;
    while(failedNodesIterator.hasNext())
    {
      String failedNode = failedNodesIterator.next();
      sqep.getRT().getSite(failedNode).setisDead(true);
    }
  }

  /**
   * outputs failed nodes in a text file for easy tracking after execution
   * @param failedNodes
   * @throws IOException 
   */
  private void recordFailedNodes(ArrayList<String> failedNodes) throws IOException
  {
    File failedNodeTextFile = new File(outputFolder + sep + "failedNodesRecord");
    BufferedWriter writer = new BufferedWriter(new FileWriter(failedNodeTextFile));
    writer.write(failedNodes.toString());
    writer.flush();
    writer.close();
  }

  /**
   * removes failed nodes from the running energy measurements.
   * @param failedNodes
   */
  private void removeFailedNodesFromRunningNodes(ArrayList<String> failedNodes)
  {
  }


  @Override
  public void runCostModels() throws OptimizationException 

  {   
    System.out.println("running cost model estimates");
    anyliser.runECMs();
  }
  

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
  
  @Override
  public void setDeadNodes(ArrayList<String> deadNodes)
  {
	  this.deadNodes = deadNodes;
  }
  
  @Override
  public void setNoDeadNodes(int noDeadNodes)
  {
	  this.noDeadNodes = noDeadNodes;
  }
  
  @Override
  public float getCECMEpochResult() 
  throws OptimizationException
  {
    return anyliser.getCECMEpochResult();
  }

  @Override
  public float getCECMAgendaResult() 
  throws OptimizationException
  {
    return anyliser.getCECMAgendaResult();
  }

  @Override
  public void queryEnded()
  {
    monitor.queryEnded();  
  }


  @Override
  public void callAnaysliserAnaylsisSNEECard(Map <Integer, Integer> sneeTuplesPerEpoch)
  {
    anyliser.anaylsisSNEECard(sneeTuplesPerEpoch);
  }
  

  @Override
  public void callAnaysliserAnaylsisSNEECard()
  {
    anyliser.anaylsisSNEECard();
    
  }

  @Override
  public void setQuery(String query)
  {
    monitor.setQuery(query);
  }
  
  @Override
  public File getOutputFolder()
  {
    return outputFolder;
  }  
  
  public void setListener(SNCBSerialPortReceiver mr)
  {
    monitor.addPacketReciever(mr);
  }
  
  public HashMap<String, RunTimeSite> getRunningSites()
  {
    return runningSites;
  }
  
  /**
   * method used to simulate test data
   * @throws CodeGenerationException 
   * @throws AutonomicManagerException 
   */
  @Override
  public void runSimulatedNodeFailure() throws OptimizationException,
      SNEEConfigurationException, SchemaMetadataException,
      TypeMappingException, AgendaException, SNEEException,
      MalformedURLException, MetadataException,
      UnsupportedAttributeTypeException, SourceMetadataException,
      TopologyReaderException, SNEEDataSourceException,
      CostParametersException, SNCBException, SNEECompilerException,
      IOException, CodeGenerationException, AutonomicManagerException
  {
    monitor.chooseFakeNodeFailure();
  }

  public void setQueryName(String queryName)
  {
    this.queryName = queryName;
  }

  public String getQueryName()
  {
    return queryName;
  }
  
  public String getQueryID()
  {
    return qep.getID().substring(0, 6);
  }
  
  public int getAdaptionCount()
  {
    return adaptionCount;
  }

  @Override
  public void setQueryParams(QoSExpectations qoS)
  {
    this.setQueryQoS(qoS);
  }

  public void setQueryQoS(QoSExpectations queryQoS)
  {
    this.queryQoS = queryQoS;
  }

  public QoSExpectations getQueryQoS()
  {
    return queryQoS;
  }
  
  public int getActiveStrategies()
  {
    return anyliser.getOperatingStrategies();
  }

  @Override
  public QueryExecutionPlan getCurrentQEP() 
  {
    return qep;
  }

  public void setCurrentQEP(SensorNetworkQueryPlan newQEP)
  {
    qep = newQEP;
  }

  @Override
  public void runSimulatedNumberOfAgendaExecutionCycles(int numberofAgendaExecutionCycles)
  {
    monitor.simulateNumeriousAgendaExecutionCycles(numberofAgendaExecutionCycles);  
  }

  @Override
  public void simulateEnergyDrainofAganedaExecutionCycles(
      int fixedNumberOfAgendaExecutionCycles)
  {
    monitor.simulateNumeriousAgendaExecutionCycles(fixedNumberOfAgendaExecutionCycles);
  }

}
