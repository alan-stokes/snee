package uk.ac.manchester.snee.client.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CondorAddtionsForTestLocally
{
  private static String sep = System.getProperty("file.separator");
  
  public static void main(String[] args)
  {
    File folder = new File("src/main/resources/condorDone");
    File[] files = folder.listFiles();
    File outFolder = new File("src/main/resources/condorchecks");
    outFolder.mkdir();
    for(int index = 0; index < files.length; index++)
    {
      File file = files[index];
      if(file.getName().contains("physical") || file.getName().contains("snee"))
      try
      {
        BufferedReader in = new BufferedReader(new FileReader(file));
        BufferedWriter out = new BufferedWriter(new FileWriter(outFolder + sep + file.getName()));
        
        String line = null;
        while((line = in.readLine()) != null)
        {
          if(file.getName().contains("physical") && line.contains("<topology>"))
          {
            String [] bits = line.split("<topology>");
            line = bits[0] +"<topology>" + "condorchecks/" + bits[1];
          }
          if(file.getName().contains("physical") && line.contains("<site-resources>"))
          {
            String [] bits = line.split("<site-resources>");
            line = bits[0] + "<site-resources>" + "condorchecks/" + bits[1];
          }   
          if(file.getName().contains("snee") && line.contains("logical_schema = "))
          {
            String [] bits = line.split("logical_schema = ");
            line = bits[0] +"logical_schema = " + "condorchecks/" + bits[1];
          }
          if(file.getName().contains("snee") && line.contains("physical_schema = "))
          {
            String [] bits = line.split("physical_schema = ");
            line = bits[0] + "physical_schema = " + "condorchecks/" + bits[1] ;
          } 
          if(file.getName().contains("snee") && line.contains("cost_parameters_file = "))
          {
            String [] bits = line.split("cost_parameters_file = ");
            line = bits[0] + "cost_parameters_file = " + "condorchecks/" + bits[1] ;
          } 
          if(file.getName().contains("snee") && line.contains("types_file = "))
          {
            String [] bits = line.split("types_file = ");
            line = bits[0] + "types_file = " + "condorchecks/" + bits[1] ;
          } 
          if(file.getName().contains("snee") && line.contains("units_file = "))
          {
            String [] bits = line.split("units_file = ");
            line = bits[0] + "units_file = " + "condorchecks/" + bits[1] ;
          } 
          out.write(line + "\n");
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
