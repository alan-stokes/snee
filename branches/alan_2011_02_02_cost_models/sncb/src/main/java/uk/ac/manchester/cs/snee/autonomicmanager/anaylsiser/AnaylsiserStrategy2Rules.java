package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser;

import uk.ac.manchester.cs.snee.autonomicmanager.AutonomicManager;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

/**
 * @author stokesa6
 *class AnaylsiserStrategy2Rules encapsulates the rules designed to calculate 
 *what possible changes the autonomic manager can do to adjust for a failure of a node
 */

public class AnaylsiserStrategy2Rules
{
  private AutonomicManager manager;
  private SensorNetworkQueryPlan qep;
  
  
  /**
   * @param autonomicManager
   * the parent of this class.
   */
  public AnaylsiserStrategy2Rules(AutonomicManager autonomicManager)
  {
    this.manager = autonomicManager;
  }
  
  public void initilise(QueryExecutionPlan qep) 
  {
    this.qep = (SensorNetworkQueryPlan) qep;
  }
  
  /**
   * @param nodeID the id for the failed node of the query plan
   * @return new query plan which has now adjusted for the failed node.
   */
  public SensorNetworkQueryPlan calculateNewQEP(int nodeID)
  {
    
    return qep;
  }
}
