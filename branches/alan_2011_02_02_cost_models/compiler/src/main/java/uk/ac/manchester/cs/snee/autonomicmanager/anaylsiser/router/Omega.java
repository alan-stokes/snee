package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser.router;

import java.util.Random;

public enum Omega
{
  TRUE, FALSE;
  
  public static Omega RandomEnum()
  { 
    Omega[] values = (Omega[]) values();
    return values[new Random().nextInt(values.length)];
  }
}
