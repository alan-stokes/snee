package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

import java.util.Random;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;

public enum LinkMatrexChoiceHeuristic
{
  ENERGY, LATENCY, RANDOM, MIXED;
	
  private static int position = 0;
  
  
  public static LinkMatrexChoiceHeuristic next()
  { 
    LinkMatrexChoiceHeuristic[] values = (LinkMatrexChoiceHeuristic[]) values();
    LinkMatrexChoiceHeuristic value = values[position];
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
  
  public static LinkMatrexChoiceHeuristic ChoiceEnum()
  { 
    LinkMatrexChoiceHeuristic[] values = (LinkMatrexChoiceHeuristic[]) values();
    return values[new Random().nextInt(2)];
  }
  
  public static void resetCounter()
  {
    position = 0;
  }
}
