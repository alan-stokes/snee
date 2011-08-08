package uk.ac.manchester.cs.snee.manager.monitor;

import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.ResultStoreImpl;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;
import uk.ac.manchester.cs.snee.sncb.SerialPortMessageReceiver;

public class Monitor implements Observer
{
  private AutonomicManager manager;
  private SerialPortMessageReceiver listener;
  private ResultStore _results;
  private boolean recievedPacketsThisQuery = false;
  private SensorNetworkQueryPlan qep;
  private String query;
  
  public Monitor(AutonomicManager autonomicManager)
  {
    manager = autonomicManager;
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
      CECMCollection();
    } 
    catch (Exception e)
    {
      e.printStackTrace();
    } 
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
    listener.addObserver(this);// TODO Auto-generated method stub
  }

  public void setResultSet(ResultStore resultSet) 
  {
    try
    {
      _results = new ResultStoreImpl(query, qep);
    } catch (Exception e)
    {
      _results = resultSet;
      e.printStackTrace();
    }
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

  public void setQueryPlan(QueryExecutionPlan qep)
  {
    this.qep = (SensorNetworkQueryPlan) qep; 
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
  MalformedURLException, MetadataException, 
  UnsupportedAttributeTypeException, SourceMetadataException, 
  TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException, SNEECompilerException
  {
    ArrayList<String> failedNodes = new ArrayList<String>();
    Node failedNode = qep.getIOT().getNode(3);
    failedNodes.add(failedNode.getID());
    System.out.println("fake node failure of node 3");
    manager.runFailedNodeFramework(failedNodes);
    
  }

}
