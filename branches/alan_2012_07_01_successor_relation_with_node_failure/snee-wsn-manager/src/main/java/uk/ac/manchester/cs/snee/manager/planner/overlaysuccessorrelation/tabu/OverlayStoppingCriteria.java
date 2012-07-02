package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.tabu;

public class OverlayStoppingCriteria
{
  private static final int numberOfIterationsTillStop = 200;
  private static final int numberOfIterationsTillStopAtInitial = 5;
  private static int currentMaxNeighbourhoodsize = 0;
  
  /**
   * checks if the tabu search has searched enough of the search space.
   * or that the search has ran to a point where it is stuck at the initial point. 
   * @param interations
   * @param iterationsFailedAtInitial
   * @param neighbourhoodSize 
   * @return
   */
  public static  boolean satisifiesStoppingCriteria(int interations, 
                                                    int iterationsFailedAtInitial,
                                                    int maxiumumSizeOfPath, 
                                                    int neighbourhoodSize)
  {
    if(currentMaxNeighbourhoodsize < neighbourhoodSize)
      currentMaxNeighbourhoodsize = neighbourhoodSize;
    
    int limit = 0;
    //determine limit
    if(maxiumumSizeOfPath <= currentMaxNeighbourhoodsize)
      limit = currentMaxNeighbourhoodsize;
    else if(maxiumumSizeOfPath > numberOfIterationsTillStopAtInitial)
      limit = maxiumumSizeOfPath;
    else
      limit = numberOfIterationsTillStopAtInitial;
    //test iterations to limit
    if(interations >= numberOfIterationsTillStop ||
       iterationsFailedAtInitial >= limit)
      return true;
    else
      return false;
  }
}
