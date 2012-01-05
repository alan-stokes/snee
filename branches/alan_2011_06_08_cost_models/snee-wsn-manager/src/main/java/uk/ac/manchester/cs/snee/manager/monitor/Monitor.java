package uk.ac.manchester.cs.snee.manager.monitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.sql.Types;
import java.util.Date;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.ResultStoreImpl;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerException;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;
import uk.ac.manchester.cs.snee.sncb.SerialPortMessageReceiver;

public class Monitor extends AutonomicManagerComponent implements Observer 
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2289795095102719034L;
  
  private SerialPortMessageReceiver listener;
  private ResultStore _results;
  private boolean recievedPacketsThisQuery = false;
  private String query;
  private boolean runCostModels = false;
  
  public Monitor(AutonomicManagerImpl autonomicManager) 
  {
    manager = autonomicManager;
  }
  
  public void initilise(SourceMetadataAbstract _metadata, QueryExecutionPlan qep, 
                        ResultStore resultSet) throws SNEEConfigurationException
  {
    this._metadata = _metadata;
    runCostModels = SNEEProperties.getBoolSetting(SNEEPropertyNames.RUN_COST_MODELS);
    this.qep = (SensorNetworkQueryPlan) qep;
    try
    {
      _results = new ResultStoreImpl(query, qep);
    } catch (Exception e)
    {
      _results = resultSet;
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void update(Observable obj, Object observed)
  {
    try
    {
      if (observed instanceof Output) {
        _results.add((Output) observed);
      } else if (observed instanceof List<?>) {
        _results.addAll((Collection<Output>) observed);
      }
      if(this.runCostModels)
        CECMCollection();
      
      ArrayList<RunTimeSite> energyDrainedNodes = updateEnergyMeasures();
      if(energyDrainedNodes.size() != 0)
      {
        //TODO ADD PROACTIVE FRAMEWORK
      }
    } 
    catch (Exception e)
    {
      e.printStackTrace();
    } 
  }
  
  public void simulateNumeriousAgendaExecutionCycles(int numberOfAgendaExecutionCycles)
  {
    for(int cycle = 0; cycle < numberOfAgendaExecutionCycles; cycle++)
    {
      ArrayList<RunTimeSite> energyDrainedNodes = updateEnergyMeasures();
      if(energyDrainedNodes.size() != 0)
      {
        //TODO ADD PROACTIVE FRAMEWORK
      }
    }
  }

  private ArrayList<RunTimeSite> updateEnergyMeasures()
  {
    ArrayList<RunTimeSite> proactiveFailures = new ArrayList<RunTimeSite>();
    HashMap<String, RunTimeSite> runningSites = manager.getRunningSites();
    Iterator<String> keyIterator =  runningSites.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      RunTimeSite site = runningSites.get(key);
      site.removeQEPExecutionCost();
      if(site.getCurrentEnergy() < site.getThresholdEnergy())
        proactiveFailures.add(site);
    }
    return proactiveFailures;
  }

  private void CECMCollection() throws SNEEException, SQLException
  {
    recievedPacketsThisQuery = true;
    int epochValue = 0;
    int tuplesAEpoch = 0;
    HashMap<Integer, Integer> tuplesPerEpoch = new HashMap<Integer, Integer>();
    int epochValueIndex = 0;
    ResultStoreImpl  resultStore = (ResultStoreImpl) _results;
    List<ResultSet> results = resultStore.getResults();
    //printResults(results);
    for (ResultSet rs : results) 
    {
      ResultSetMetaData metaData = rs.getMetaData();
      int numCols = metaData.getColumnCount();
      for (int i = 1; i <= numCols; i++) 
      {
        String label = metaData.getColumnLabel(i);
        if(label.equals("system.evalepoch"))
        {
          epochValueIndex = i;
        }
      }
      while (rs.next()) 
      {
        int value = (Integer) rs.getObject(epochValueIndex);
        if(value == epochValue)
        {
          tuplesAEpoch++;
        }
        else if(value != epochValue)
        {
          if(tuplesPerEpoch.containsKey(epochValue))
          {
            int pastTuplesForSameEpoch = tuplesPerEpoch.get(epochValue);
            tuplesPerEpoch.remove(epochValue);
            tuplesAEpoch += pastTuplesForSameEpoch;
          }
          
          tuplesPerEpoch.put(epochValue, tuplesAEpoch);
          epochValue = value;
          tuplesAEpoch = 1;
        }
      }
    }
    manager.callAnaysliserAnaylsisSNEECard(tuplesPerEpoch);
    
  }

  public void addPacketReciever(SNCBSerialPortReceiver mr)
  {
    listener = (SerialPortMessageReceiver) mr;
    listener.addObserver(this);
  }

  public void queryEnded()
  {
    if(!recievedPacketsThisQuery) 
    {
      manager.callAnaysliserAnaylsisSNEECard();
    }
  }

  public void queryStarting()
  {
    recievedPacketsThisQuery = false;
  }

  public void setQuery(String query)
  {
    this.query = query;
    
  }
  //Temporary code to allow notation tests without a failed node
  public void chooseFakeNodeFailure() 
  throws SNEEConfigurationException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  AgendaException, SNEEException, 
  MetadataException,  CodeGenerationException, 
  UnsupportedAttributeTypeException, SourceMetadataException, 
  TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException, 
  SNEECompilerException, IOException, AutonomicManagerException
  {
    ArrayList<String> failedNodes = new ArrayList<String>();
    Node failedNode = qep.getIOT().getNode(new Integer(2).toString());
    failedNodes.add(failedNode.getID());
    System.out.println("running fake node failure simulation");
    System.out.println("simulated failure of node 3");
    removeFailedNodesFromRunningNodes(failedNodes);
    setFailedNodesToDeadInQEP(failedNodes);
    removeNodesFromMetaData(failedNodes);
    manager.runFailedNodeFramework(failedNodes);
    
  }

  /**
   * simulates a set of failed nodes (forced from the client)
   */
  public void forceFailedNodes(ArrayList<String> failedNodes)
  throws SNEEConfigurationException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, AgendaException, SNEEException, MetadataException, 
  CodeGenerationException, UnsupportedAttributeTypeException, SourceMetadataException, 
  TopologyReaderException, SNEEDataSourceException, CostParametersException,
  SNCBException, SNEECompilerException, IOException, AutonomicManagerException
  {
    removeFailedNodesFromRunningNodes(failedNodes);
    setFailedNodesToDeadInQEP(failedNodes);
    removeNodesFromMetaData(failedNodes);
    manager.runFailedNodeFramework(failedNodes);
  }
  
  /**
   * removes failed nodes from the running energy measurements.
   * @param failedNodes
   */
  private void removeFailedNodesFromRunningNodes(ArrayList<String> failedNodes)
  {
  }
  
  /**
   * outputs failed nodes in a text file for easy tracking after execution
   * @param failedNodes
   * @throws IOException 
   */
  public void recordFailedNodes(ArrayList<String> failedNodes, File outputFolder) throws IOException
  {
    File failedNodeTextFile = new File(outputFolder + sep + "failedNodesRecord");
    BufferedWriter writer = new BufferedWriter(new FileWriter(failedNodeTextFile));
    writer.write(failedNodes.toString());
    writer.flush();
    writer.close();
  }
  
  /**
   * method used to define dead nodes in qep. (used in cardinality cost models
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
   * removes nodes from metadata topology and sources
   * @param failedNodes
   */
  private void removeNodesFromMetaData(ArrayList<String> failedNodes) 
  {
    //remove node from topology and source metadata
    Iterator<String> failedNodeIterator = failedNodes.iterator();
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) _metadata;
    while(failedNodeIterator.hasNext())
    {
      String nodeID = failedNodeIterator.next();
      sm.removeSourceSite(new Integer(nodeID));
      sm.removeNodeFromTopology(nodeID);
    }
  }

}
