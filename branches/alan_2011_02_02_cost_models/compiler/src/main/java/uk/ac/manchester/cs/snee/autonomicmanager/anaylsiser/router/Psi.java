package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser.router;

import java.util.Random;

public enum Psi
{
  ENERGY, LATENCY, RANDOM, MIXED;
  
  public static Psi RandomEnum()
  { 
    Psi[] values = (Psi[]) values();
    return values[new Random().nextInt(values.length)];
  }
}
