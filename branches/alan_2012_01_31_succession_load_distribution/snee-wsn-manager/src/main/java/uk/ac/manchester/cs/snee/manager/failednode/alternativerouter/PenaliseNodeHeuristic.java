package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

public enum PenaliseNodeHeuristic
{
  //TRUE, FALSE;
  TRUE;
  
  private static int position = 0;
  
  
  public static PenaliseNodeHeuristic next()
  { 
    PenaliseNodeHeuristic[] values = (PenaliseNodeHeuristic[]) values();
    PenaliseNodeHeuristic value = values[position];
    position++;
    return value;
  }
 
  public static boolean hasNext()
  { 
    if(position < values().length)
      return true;
    else
      return false;
  }
  
  public static void resetCounter()
  {
    position = 0;
  }
}
