package uk.ac.manchester.cs.snee.manager.failednodestrategies.localrepairstrategy.heursticsrouter;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;

public enum SensibleHeuristic
{
  Shortest, Random, Longest;
  private static int position = 0;
  
  
  public static SensibleHeuristic next()
  { 
    SensibleHeuristic[] values = (SensibleHeuristic[]) values();
    SensibleHeuristic value = values[position];
    position++;
    return value;
  }
 
  public static boolean hasNext() 
  throws SNEEConfigurationException
  { 
    boolean sucessor = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_SUCCESSOR);
    if(sucessor)
    {
      if(position < values().length)
        return true;
      else
        return false;
    }
    else
    {
      if(position < 1)
        return true;
      else
        return false;
    }
  }
  
  public static void resetCounter()
  {
    position = 0;
  }
  
}
