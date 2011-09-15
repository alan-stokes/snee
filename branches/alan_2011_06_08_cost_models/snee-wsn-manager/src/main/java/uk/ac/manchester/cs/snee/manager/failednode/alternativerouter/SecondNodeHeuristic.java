package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

public enum SecondNodeHeuristic
{
  CLOSEST_SINK, CLOSEST_ANY, RANDOM;
  private static int position = 0;
  
  
  public static SecondNodeHeuristic next()
  { 
    SecondNodeHeuristic[] values = (SecondNodeHeuristic[]) values();
    SecondNodeHeuristic value = values[position];
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
