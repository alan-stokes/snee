package uk.ac.manchester.cs.snee.autonomicmanager;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.ResultStoreImpl;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;
import uk.ac.manchester.cs.snee.sncb.SerialPortMessageReceiver;

public class AutonomicManagerMonitor implements Observer
{
  private AutonomicManager manager;
  private SerialPortMessageReceiver listener;
  private ResultStore _results;
  private boolean recievedPacketsThisQuery = false;

  public AutonomicManagerMonitor(AutonomicManager autonomicManager)
  {
    manager = autonomicManager;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void update(Observable obj, Object observed)
  {
    try
    {
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
        else if(value > epochValue)
        {
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
    _results = resultSet;
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

}
