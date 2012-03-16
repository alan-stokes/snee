package uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu;

public class StoppingCriteria
{
  private static int numberOfIterationsTillStop = 300;
  
  public static  boolean satisifiesStoppingCriteria(int interations)
  {
    if(interations >= numberOfIterationsTillStop)
      return true;
    else
      return false;
  }
}
