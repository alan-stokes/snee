package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser.router;

import java.util.Random;

public enum FirstNodeHeuristic
{
  SINK,RANDOM;
  
  public static FirstNodeHeuristic RandomEnum()
  { 
    FirstNodeHeuristic[] values = (FirstNodeHeuristic[]) values();
    return values[new Random().nextInt(values.length)];
  }
}
