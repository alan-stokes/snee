package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class tupleDataPlot
{
  public static void main (String [] args)
  {
    try
    {
      BufferedReader in = new BufferedReader(new FileReader(new File("/mnt/usb/1st1/severalNodeFailure/lifetime_results/largetopologysetseveralNodeFailureQOS2finalResults")));
      BufferedReader inBuff = new BufferedReader(new FileReader(new File("/mnt/usb/1st1/severalNodeFailure/tuples/large2/buff2")));
      BufferedWriter out = new BufferedWriter(new FileWriter(new File("/mnt/usb/1st1/severalNodeFailure/tuples/large2/data")));
    
      String inLine = "";
      while((inLine = in.readLine()) != null)
      {
        Integer inBuffLine = Integer.parseInt(inBuff.readLine());
        String [] bits = inLine.split(" ");
        int index = 1;
        String outLine = bits[0] + " ";
        while(index < bits.length)
        {
          Double tuples = Double.parseDouble(bits[index]) * inBuffLine;
          outLine = outLine.concat(tuples.toString() + " ");
          index++;
        }
        out.write(outLine + "\n");
      }
    out.flush();
    out.close();
    in.close();
    inBuff.close();
    
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
