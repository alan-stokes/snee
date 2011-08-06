package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.File;

import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;

/**
 * 
 * @author alan
 *class which encapsulates the local framework using clusters and equivalence relations
 */
public class FailedNodeFrameWorkLocal
{
  private AutonomicManager manager;
  private SensorNetworkQueryPlan qep;
  private File outputFolder;
  private String sep = System.getProperty("file.separator");
	
  public FailedNodeFrameWorkLocal(AutonomicManager autonomicManager)
  {
    this.manager = autonomicManager;
  }
	
  public void initilise(QueryExecutionPlan oldQep)
  {  
    this.qep = (SensorNetworkQueryPlan) oldQep;
    outputFolder = manager.getOutputFolder();
  }
  
  
}
