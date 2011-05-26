package uk.ac.manchester.cs.snee.autonomicmanager;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser.Anaylsiser;
import uk.ac.manchester.cs.snee.autonomicmanager.executer.Executer;
import uk.ac.manchester.cs.snee.autonomicmanager.monitor.Monitor;
import uk.ac.manchester.cs.snee.autonomicmanager.planner.Planner;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
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
  private static Logger resultsLogger = 
    Logger.getLogger("results.autonomicManager");
  private File outputFolder;
  
  public AutonomicManager()
  {
	  anyliser = new Anaylsiser(this);
	  monitor = new Monitor(this);
	  planner = new Planner(this);
	  executer = new Executer(this);
	  //set up output folder for any autonomic data structures
	  outputFolder = new File("AutonomicFolder");
	  if(outputFolder.exists())
	  {
	    File[] contents = outputFolder.listFiles();
	    for(int index = 0; index < contents.length; index++)
	    {
	      File delete = contents[index];
	      delete.delete();
	    }
	  }
	  else
	  {
	    outputFolder.mkdir();
	  }
  }
 
  public void setQueryExecutionPlan(QueryExecutionPlan qep) throws SNEEException, SNEEConfigurationException, SchemaMetadataException
  {
	  this.qep = qep;
	  anyliser.initilise(qep);
	  monitor.setQueryPlan(qep);
  }

  public void runStragity2(int failedNodeID) throws SNEEConfigurationException
  {
    SensorNetworkQueryPlan newQEP = anyliser.runStrategy2(failedNodeID);
    //newQEP.getIOT().exportAsDotFileWithFrags(fname, label, exchangesOnSites)
    new AgendaUtils( newQEP.getAgenda(), true).generateImage();
  }
  
  
  public void runCostModels() throws OptimizationException, SNEEConfigurationException
  {    
    anyliser.runECMs();
    monitor.chooseFakeNodeFailure();
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
  
  public File getOutputFolder()
  {
    return outputFolder;
  }
}
