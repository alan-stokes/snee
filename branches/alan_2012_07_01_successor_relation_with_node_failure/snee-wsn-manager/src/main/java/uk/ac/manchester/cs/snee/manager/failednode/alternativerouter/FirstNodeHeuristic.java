package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;

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
