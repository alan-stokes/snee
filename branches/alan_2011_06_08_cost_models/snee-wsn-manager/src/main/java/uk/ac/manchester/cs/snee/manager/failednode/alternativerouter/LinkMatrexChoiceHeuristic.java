package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

import java.util.Random;

public enum LinkMatrexChoiceHeuristic
{
  ENERGY, LATENCY, RANDOM, MIXED;
  
  public static LinkMatrexChoiceHeuristic RandomEnum()
  { 
    LinkMatrexChoiceHeuristic[] values = (LinkMatrexChoiceHeuristic[]) values();
    return values[new Random().nextInt(values.length)];
  }
  
  public static LinkMatrexChoiceHeuristic ChoiceEnum()
  { 
    LinkMatrexChoiceHeuristic[] values = (LinkMatrexChoiceHeuristic[]) values();
    return values[new Random().nextInt(2)];
  }
}
