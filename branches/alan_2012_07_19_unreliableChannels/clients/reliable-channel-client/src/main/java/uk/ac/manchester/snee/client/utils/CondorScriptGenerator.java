package uk.ac.manchester.snee.client.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class CondorScriptGenerator
{
  private static ArrayList<Long> seeds = new ArrayList<Long>();
  private static String sep = System.getProperty("file.separator");
  private static BufferedWriter out; 
  private static int startKCounter = 2;
  private static int endKCoutner = 5;
  private static File queriesFile = new File("src/main/resources/testsSize30" + sep + "queries.txt");
  private static ArrayList<Integer> validIds = new ArrayList<Integer>();
  private static boolean checkIDs = true;
  
  public static void main(String[] args)
  {
    
    
    seeds.add(new Long(0));
    seeds.add(new Long(1));
    seeds.add(new Long(10));
    seeds.add(new Long(15));
    seeds.add(new Long(20));
    seeds.add(new Long(25));
    seeds.add(new Long(30));
    seeds.add(new Long(35));
    seeds.add(new Long(40));
    seeds.add(new Long(45));
    validIds.addAll(Arrays.asList(30,31,32,33,34,36));
    
    File condorFile = new File("condor");
    if(!condorFile.exists())
      condorFile.mkdir();
    else
    {
      deleteFileContents(condorFile);
    }
    ArrayList<String> queries = new ArrayList<String>();
    try
    {
      out = new BufferedWriter(new FileWriter(new File(condorFile.toString() + sep + "submit.txt")));
      BufferedReader queryReader = new BufferedReader(new FileReader(queriesFile.getAbsolutePath()));
      readInQueries(queries, queryReader);
      DecimalFormat df = new DecimalFormat("#.#");
      //normal blurb
      out.write("universe = vanilla\nexecutable = start.sh \nwhen_to_transfer_output = ON_EXIT \n" +
                "Should_Transfer_Files = YES\ntransfer_input_files = ../SNEE.jar,../jre.tar.gz \n" +
                 "Requirements = (OpSys == "+
                "\"LINUX\") \nRequest_Disk = 3000000 \n"+
                "request_memory = 2048 \n#Output = output$(Process).txt \n" +
                "#Error = error$(Process).txt \nlog = log.txt \nOutput = out.txt \n"+
                "Error = err.txt \nnotification = error \n \n");
      Iterator<Long> seedIterator = seeds.iterator();
      while(seedIterator.hasNext())
      {
        Long seed = seedIterator.next();
        int queryID = 1;
        Iterator<String> queryIterator = queries.iterator();
        while(queryIterator.hasNext())
        {
          String query = queryIterator.next();
          if(!checkIDs)
          {
            for(int index = startKCounter; index <= endKCoutner; index++)
            {
              for(double distance = 1; distance > 0.2; distance -=0.2 )
              {
	            out.write("Arguments = " + query + " " + "snee" + queryID + "." + index + ".properties" +
	                      " " + queryID + " " + seed + " " + df.format(distance) + "\ninitialdir   = query" + queryID + "." + index + 
	                      "." + seed + "." + df.format(distance) + "\nqueue \n \n");
	            //make folder for the output to be stored in.
	            File outputFolder = new File(condorFile.toString() + sep + "query" + queryID + "." + 
	                                         index + "." + seed + "." + df.format(distance));
	            outputFolder.mkdir();
              }
            }
            queryID++;
          }
          else
          {
            if(validIds.contains(queryID))
            {
              for(int index = startKCounter; index <= endKCoutner; index++)
              {
            	for(double distance = 1; distance > 0.2; distance -=0.2 )
                {
	              out.write("Arguments = " + query + " " + "snee" + queryID + "." + index + ".properties" +
	                        " " + queryID + " " + seed +  " " + df.format(distance) + " \ninitialdir   = query" + queryID + "." + index + 
	                        "." + seed + "." + df.format(distance) + "\nqueue \n \n");
	              //make folder for the output to be stored in.
	              File outputFolder = new File(condorFile.toString() + sep + "query" + queryID + "." + 
	                                           index + "." + seed + "." + df.format(distance));
	              outputFolder.mkdir();
                }
              }
            }
            queryID++;
          }
        }
      }
      out.flush();
      out.close();
      
      //make script
      out = new BufferedWriter(new FileWriter(new File(condorFile.toString() + sep + "start.sh")));
      out.write("#!/bin/bash \necho $1 \necho $2 \n" +
      		"echo $3 \necho $4 \nunzip SNEE.jar -d extracted"+
      		"\n rm -f SNEE.jar" +  
      		"\ncd extracted \n" +
          "jre1.6.0_27/bin/java uk/ac/manchester/snee/client/CondorReliableChannelClient $1 $2 $3 $4 $5\n" +
          "for d in *; do if test -d \"$d\"; then tar czf \"$d\".tgz \"$d\"; fi; done" +
          "\nmv output.tgz .. \n exit 0");
      out.flush();
      out.close();
      
    }
    catch(Exception e)
    {
      System.out.println("something broke to get here");
      System.out.println("the local error message is " + e.getLocalizedMessage());
      System.out.println("the error message is " + e.getMessage());
      System.out.println("the error stack is ");
      e.printStackTrace();
    }
  }

  /**
   * cleaning method
   * @param firstOutputFolder
   */
  public static void deleteFileContents(File firstOutputFolder)
  {
    if(firstOutputFolder.exists())
    {
      File[] contents = firstOutputFolder.listFiles();
      for(int index = 0; index < contents.length; index++)
      {
        File delete = contents[index];
        if(delete.isDirectory())
          if(delete != null && delete.listFiles().length > 0)
            deleteFileContents(delete);
          else
            delete.delete();
        else
          delete.delete();
      }
    }
    else
    {
      firstOutputFolder.mkdir();
    }  
  }
  
  private static void readInQueries(ArrayList<String> queries, BufferedReader queryReader)
  throws IOException
  {
    String line = "";
    while((line = queryReader.readLine()) != null)
    {
      line = line.replaceAll(" ", "_");
      queries.add(line);
    }
  }
}
