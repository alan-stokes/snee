package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser.router;

import java.util.Random;

public enum Chi
{
  CLOSEST_SINK, CLOSEST_ANY, RANDOM;
  
  public static Chi RandomEnum()
  { 
    Chi[] values = (Chi[]) values();
    return values[new Random().nextInt(values.length)];
  }
}
