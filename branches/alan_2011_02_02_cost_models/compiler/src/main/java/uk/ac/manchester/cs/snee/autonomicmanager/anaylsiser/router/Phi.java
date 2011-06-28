package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser.router;

import java.util.Random;

public enum Phi
{
  SINK,RANDOM;
  
  public static Phi RandomEnum()
  { 
    Phi[] values = (Phi[]) values();
    return values[new Random().nextInt(values.length)];
  }
}
