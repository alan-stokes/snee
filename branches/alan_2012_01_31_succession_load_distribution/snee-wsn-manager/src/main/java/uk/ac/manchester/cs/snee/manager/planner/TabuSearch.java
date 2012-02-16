package uk.ac.manchester.cs.snee.manager.planner;

import java.util.ArrayList;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public class TabuSearch 
{
  private ArrayList<SensorNetworkQueryPlan> AlternativePlans;
	
  public TabuSearch(ArrayList<SensorNetworkQueryPlan> AlternativePlans)
  {
	  this.AlternativePlans = AlternativePlans;
  }
  
  public ArrayList<Successor> findSuccessorsPath()
  {
    return null;
  }
}
