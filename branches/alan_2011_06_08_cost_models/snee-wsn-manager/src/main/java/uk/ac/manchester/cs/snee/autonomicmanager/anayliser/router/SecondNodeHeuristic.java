package uk.ac.manchester.cs.snee.autonomicmanager.anayliser.router;

import java.util.Random;

public enum SecondNodeHeuristic
{
  CLOSEST_SINK, CLOSEST_ANY, RANDOM;
  
  public static SecondNodeHeuristic RandomEnum()
  { 
    SecondNodeHeuristic[] values = (SecondNodeHeuristic[]) values();
    return values[new Random().nextInt(values.length)];
  }
}
