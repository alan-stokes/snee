package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

public enum FirstNodeHeuristic
{
  SINK,RANDOM;
  
  private static int position = 0;
  
  public static FirstNodeHeuristic next()
  { 
    FirstNodeHeuristic[] values = (FirstNodeHeuristic[]) values();
    FirstNodeHeuristic value = values[position];
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
