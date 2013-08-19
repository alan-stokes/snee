package uk.ac.manchester.snee.client.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class timetwiddleclearup
{ 
  private static File input = new File("/mnt/usb/1st1/successorTimings/query4.5/records");
  //private static File input = new File("/mnt/usb/1st1/successorTimings/query3.1/output/query3/AutonomicManData/Planner/successorRelation/records");
  public static void main (String [] args)
  {
    Long max = new Long(0);
    Long maxLifetimeSoFar = new Long(0);
    Long previous = new Long(0);
    BufferedReader in;
    try
    {
      in = new BufferedReader(new FileReader(input));
      String line = null;
      while((line = in.readLine()) != null)
      {
        String [] bits = line.split(" ");
        Long lifetime = Long.parseLong(bits[11]);
        previous = max;
        max = lifetime;
        if(max - previous !=-884 && max - previous !=884 &&
            max - previous !=-1000 && max - previous !=1000 &&
            max - previous !=-598 && max - previous !=598 &&
            max - previous !=-186 && max - previous !=186 &&
            max - previous !=-16 && max - previous !=16 &&
             max > maxLifetimeSoFar)
        {
          maxLifetimeSoFar = max;  
          System.out.println(max - previous);
        }
      }
      System.out.println(maxLifetimeSoFar);
    }
    catch (FileNotFoundException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
