package PlotUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;

public class GenerateLifetimeDataFile
{
  private static BufferedWriter out = null;
  private static String sep = System.getProperty("file.separator");
  
  
  
  public static void main (String [] args)
  {
    try
    {
      //sort out hard coded files
      String filePath = "output";
      String outputPath = "/home/S06/stokesa6/Dropbox/Phd/writingStuff/myPapers/SuccessorRelationPaper/images/new lifetime measurements/lifetimes";
      //set up output
      out = new BufferedWriter(new FileWriter(new File(outputPath)));
      //start reading in inputs
      File inputFolder = new File(filePath);
      if(inputFolder.listFiles().length != 0)
      {
        File[] fileinputs = inputFolder.listFiles();
        ArrayList<String> inputs = new ArrayList<String>(fileinputs.length);
        for(int index = 0; index < fileinputs.length; index++)
        {
          inputs.add(null);
        }
        
        
        for(int index = 0; index < fileinputs.length; index++)
        {
          String filepath = fileinputs[index].getAbsolutePath();
          String[] segments = filepath.split("query");
          String segment = segments[segments.length -1];
          inputs.set(Integer.parseInt(segment) -1, filepath);
        }
        //sort out inputs so they go in order
        
        int counter = 1;
        for(int index = 0; index < inputs.size(); index++)
        {
          File inputFile = new File(inputs.get(index) + sep + "AutonomicManData");
          if(inputFile.exists())
          {
            File lifetimeFile = new File(inputFile + sep + "Planner/successorRelation/finalSolution/successorLifetimes.txt");
            if(lifetimeFile.exists())
            {
              BufferedReader in = new BufferedReader(new FileReader(lifetimeFile));
              Integer original = 0;
              Integer successor = 0;
              String line = in.readLine();
              String previousline = null;
              String [] segments = line.split(": ");
              original = Integer.parseInt(segments[1].substring(0, segments[1].length() -1));
              while((line = in.readLine()) != null)
              {
                previousline = line;
              }
              if(previousline == null)
              {
                out.write(counter + " " + original + " 0 \n");
              }
              else
              {
              segments = previousline.split(": ");
              successor = Integer.parseInt(segments[1].substring(0, segments[1].length() -1));
              out.write(counter + " " + original + " " + successor + "\n");
              }
            }
            else
            {
              out.write(counter + " XXXXXXX \n");
            }
          }
          else
          {
            out.write(counter + " XXXXXXX \n");
          }
          out.flush();
          counter++;
        }
        out.flush();
        out.close();
      }
      else
      {
        out.flush();
        out.close();
        throw new Exception("no output to read in");
      }
    
    }
    catch(Exception e)
    {
      System.out.println("something failed with message " + e.getMessage() + " with stack of");  
      e.printStackTrace();
    }
  }
}
