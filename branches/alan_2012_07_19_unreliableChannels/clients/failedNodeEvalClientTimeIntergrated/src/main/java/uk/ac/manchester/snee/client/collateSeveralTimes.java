package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;

public class collateSeveralTimes
{
  public static void main(String [] args) 
  {
    try
    {
    File mainFolder =
      new File("/mnt/usb/1st1/cspc024_phd_data/node_failure_stuff/outputForSeveralNodeFailure/output/query1/AutonomicManData");
    int numberOfNodeFailures = 8;
    
    ArrayList<Double> timesForGlobalAdaptation = new ArrayList<Double>();
    ArrayList<Double> timesForRegionalAdaptation = new ArrayList<Double>();
    int failureCounter = 0;
    int currentFailureCap = 1;
    int totalCount = 0;
    double totalseconds = 0;
    int partialPoint = 8+7+6+5+4+3+2+1;
    
    File[] list = mainFolder.listFiles();
    Iterator<File> sortedFiles = sortFiles(list).iterator();
    while(sortedFiles.hasNext())
    {
      File adpatationfolder = sortedFiles.next();
      File dataFile = 
        new File(adpatationfolder.toString() + "/Planner/assessment/Adaptations/adaptList");
      BufferedReader in = new BufferedReader(new FileReader(dataFile));
      String dataLine = in.readLine();
      in.close();
      String[] bits = dataLine.split("TimeCost ");
      String[] resultbits = bits[1].split("ms");
      resultbits[0] = resultbits[0].replace("[", "");
      Integer value = Integer.parseInt(resultbits[0]);
      Double secondValue = (double) (value/1000);
      failureCounter ++;
      totalCount ++;
      if(failureCounter == currentFailureCap)
      {
        if(totalCount > partialPoint)
        {
          totalseconds+= secondValue;
          timesForRegionalAdaptation.add(totalseconds);
          currentFailureCap++;
          failureCounter = 0;
        }
        else
        {
          totalseconds+= secondValue;
          timesForGlobalAdaptation.add(totalseconds);
          currentFailureCap++;
          failureCounter = 0;
          if(totalCount == partialPoint)
          {
            currentFailureCap = 1;
            failureCounter = 0;
            totalseconds = 0;
          }
        }
      }
    }
    System.out.println(timesForGlobalAdaptation.toString() + " : " + timesForRegionalAdaptation.toString());
    outputForPlotter(timesForGlobalAdaptation, timesForRegionalAdaptation);
    
    }catch(Exception e)
    {
      System.out.println("broke");
      e.printStackTrace();
    }
  }

  private static void outputForPlotter(
      ArrayList<Double> timesForGlobalAdaptation,
      ArrayList<Double> timesForRegionalAdaptation)
  {
    String output = "";
    int topIndex = Math.min(timesForGlobalAdaptation.size(), timesForRegionalAdaptation.size());
    for(int index =0; index < topIndex; index++)
    {
      output = output.concat(timesForGlobalAdaptation.get(index) + " " + timesForRegionalAdaptation.get(index) + " ");
    }
    System.out.println(output);
  }

  private static ArrayList<File> sortFiles(File[] list)
  {
    ArrayList<File> sorted = new ArrayList<File>();
    for(int index = 0; index < list.length; index++)
    {
      sorted.add(null);
    }
    for(int counter = 0; counter < list.length; counter++)
    {
      File possibleAdpatationfolder = list[counter];
      String [] bits = possibleAdpatationfolder.getName().split("Adaption");
      if(bits.length == 2)
      {
        sorted.set(Integer.parseInt(bits[1]), possibleAdpatationfolder);
      }
    }
    for(int index = 0; index < list.length; index++)
    {
      sorted.remove(null);
    }
    return sorted;
  }
}
