package uk.ac.manchester.snee.client.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CondorRemovalForTests
{
  private static String sep = System.getProperty("file.separator");
  
  public static void main(String[] args)
  {
    File folder = new File("src/main/resources/condorcheck");
    File[] files = folder.listFiles();
    File outFolder = new File("src/main/resources/condorDone");
    outFolder.mkdir();
    for(int index = 0; index < files.length; index++)
    {
      File file = files[index];
      try
      {
        BufferedReader in = new BufferedReader(new FileReader(file));
        BufferedWriter out = new BufferedWriter(new FileWriter(outFolder + sep + file.getName()));
        
        String line = null;
        while((line = in.readLine()) != null)
        {
          if(line.contains("etc/common"))
          {
            String [] bits = line.split("etc/common/");
            line = bits[0] + bits[1];
          }
          out.write(line+ "\n");
        }
        out.flush();
        out.close();  
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
}
