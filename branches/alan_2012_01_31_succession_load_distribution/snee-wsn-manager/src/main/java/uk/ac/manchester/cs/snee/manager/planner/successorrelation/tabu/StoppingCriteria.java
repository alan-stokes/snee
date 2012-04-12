package uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu;

public class StoppingCriteria
{
  private static final int numberOfIterationsTillStop = 200;
  private static final int numberOfIterationsTillStopAtInitial = 5;
  
  
  /**
   * checks if the tabu search has searched enough of the search space.
   * or that the search has ran to a point where it is stuck at the initial point. 
   * @param interations
   * @param iterationsFailedAtInitial
   * @return
   */
  public static  boolean satisifiesStoppingCriteria(int interations, 
                                                    int iterationsFailedAtInitial)
  {
    if(interations >= numberOfIterationsTillStop ||
       iterationsFailedAtInitial >= numberOfIterationsTillStopAtInitial)
      return true;
    else
      return false;
  }
}
