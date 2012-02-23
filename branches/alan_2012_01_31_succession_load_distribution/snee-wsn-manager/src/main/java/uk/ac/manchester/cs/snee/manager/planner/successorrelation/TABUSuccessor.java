package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.util.ArrayList;
import java.util.HashMap;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class TABUSuccessor extends Successor
{
  private boolean entirelyTABUed;
  private ArrayList<Integer> timesTABUed = new ArrayList<Integer>();
  private static final int maxiumumTimesLookedAt = 8;
  
  
  public TABUSuccessor(SensorNetworkQueryPlan qep, HashMap<String, RunTimeSite> newRunTimeSites,
                       boolean entirely) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    super(qep, 0, newRunTimeSites, 0);
    this.entirelyTABUed = entirely;
    setTimesTABUed(null);
  }
  
  public TABUSuccessor(SensorNetworkQueryPlan qep, HashMap<String, RunTimeSite> newRunTimeSites,
                       ArrayList<Integer> times
                      ) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    super(qep, 0, newRunTimeSites, 0);
    entirelyTABUed = false;
    this.setTimesTABUed(times);
  }
  
  public boolean isEntirelyTABUed()
  {
    return entirelyTABUed;
  }
  public void setEntirelyTABUed(boolean newState)
  {
    entirelyTABUed = newState;
  }
  

  private void setTimesTABUed(ArrayList<Integer> timesTABUed)
  {
    this.timesTABUed = timesTABUed;
  }

  public ArrayList<Integer> getTimesTABUed()
  {
    return timesTABUed;
  }
  
  public void addTimesTABUed(int TABUedTime)
  {
    timesTABUed.add(TABUedTime);
    if(timesTABUed.size() > maxiumumTimesLookedAt)
      this.entirelyTABUed = true;
  }
  
  public String toString()
  {
    return this.qep.getID();
  }

  public String getID()
  {
    return this.qep.getID();
  }
  
  public String getTimes()
  {
    if(this.entirelyTABUed)
      return "ALL";
    else
      return this.timesTABUed.toString();
  }
  
  
  
}
