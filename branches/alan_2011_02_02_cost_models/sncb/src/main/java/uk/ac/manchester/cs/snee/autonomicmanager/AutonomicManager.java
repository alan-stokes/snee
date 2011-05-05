package uk.ac.manchester.cs.snee.autonomicmanager;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;

public class AutonomicManager 
{
  private AutonomicManagerAnaylsis anyliser;
  private AutonomicManagerMonitor monitor;
  private AutonomicManagerPlanner planner;
  private AutonomicManagerExecuter executer;
  private QueryExecutionPlan qep;
  private ArrayList<Integer> deadNodes = null;
  private int noDeadNodes = 0;
  private static Logger resultsLogger = 
    Logger.getLogger("results.autonomicManager");
  
  public AutonomicManager()
  {
	  anyliser = new AutonomicManagerAnaylsis(this);
	  monitor = new AutonomicManagerMonitor(this);
	  planner = new AutonomicManagerPlanner(this);
	  executer = new AutonomicManagerExecuter(this);
  }
 
  public void setQueryExecutionPlan(QueryExecutionPlan qep) throws SNEEException, SNEEConfigurationException
  {
	  this.qep = qep;
	  anyliser.initiliseCardECM(qep);
	  monitor.setQueryPlan(qep);
  }

  public void runCostModels() throws OptimizationException 
  {    
    anyliser.runECMs();
  }
  
  public void runAnyliserWithDeadNodes() throws OptimizationException
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
  
  public float getCECMEpochResult() throws OptimizationException
  {
    return anyliser.getCECMEpochResult();
  }
  
  public float getCECMAgendaResult() throws OptimizationException
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
}
