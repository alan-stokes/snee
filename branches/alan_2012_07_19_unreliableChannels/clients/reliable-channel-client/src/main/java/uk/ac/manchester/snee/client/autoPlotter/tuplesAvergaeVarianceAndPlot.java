package uk.ac.manchester.snee.client.autoPlotter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class tuplesAvergaeVarianceAndPlot
{
  static private HashMap<Integer, Query> queries = new HashMap<Integer, Query>();
  static private File inputFile = new File("/home/alan/Desktop/edgeResults/data/casino3/1.0Data");
  static private File outputFile = new File("/home/alan/Desktop/edgeResults/data/casino3/aggre1.0");
  static private BufferedWriter out = null;
  /**
   * main method for entering
   * @param args
   */
  public static void main(String [] args)
  {
    try
    {
    	out = new BufferedWriter(new FileWriter(outputFile));
      BufferedReader in;
      if(args.length ==1 )
        in = new BufferedReader(new FileReader(new File(args[0])));
      else
        in = new BufferedReader(new FileReader(inputFile));
      
      String line = "";
      while((line = in.readLine())!= null)
      {
        String [] bits = line.split(" ");
        if(bits.length !=0)
        {
          Integer queryID = Integer.parseInt(bits[0]);
          updateDataStore(line, queryID);
        }
      }
      outputAverageAndVarianceForQueryType();
      outputAverageAndVarianceForAllTopologies();
      out.flush();
      out.close();
      
    }
    catch (FileNotFoundException e)
    {
      System.out.println("file with name " + inputFile + " was not found");
      e.printStackTrace();
    }
    catch (IOException e)
    {
      System.out.println("file with name " + inputFile + " was not readable");
      e.printStackTrace();
    }
  }

  /**
   * determines the average and variance for all topologeis inrelavent of query type
 * @throws IOException 
   */
  private static void outputAverageAndVarianceForAllTopologies() throws IOException
  {
    HashMap<Integer ,AverageAndVarianceKlevel> avergaes = 
      new HashMap<Integer ,AverageAndVarianceKlevel>();
    Iterator<Integer> queryIterator = queries.keySet().iterator();
    while(queryIterator.hasNext())
    {
      Integer queryKey = queryIterator.next();
      Query q = queries.get(queryKey);
      HashMap<Integer, Klevel> data =  q.getData();
      Iterator<Integer> klevelIterator = data.keySet().iterator();
      while(klevelIterator.hasNext())
      {
        Integer kkey = klevelIterator.next();
        Klevel k = data.get(kkey);
        AverageAndVarianceKlevel averageKlevel;
        if(avergaes.get(kkey) == null)
          averageKlevel = new AverageAndVarianceKlevel();
        else
          averageKlevel = avergaes.get(kkey);
        
        averageKlevel.addpercentage(k.getTuplePercentage());
        avergaes.put(kkey, averageKlevel);
      }
    }
    outputAveragesFortopologyies(avergaes);
  }

    
  private static void outputAveragesFortopologyies(
      HashMap<Integer, AverageAndVarianceKlevel> avergaes) throws IOException
  {
    String output = "";
    Iterator<Integer> averagekeySet = avergaes.keySet().iterator();
    while(averagekeySet.hasNext())
    {
      Integer averageKey = averagekeySet.next();
      output = output.concat(averageKey + " ");
      AverageAndVarianceKlevel kAverage = avergaes.get(averageKey);
      output = output.concat(kAverage.getTuplePercentage() / 15 + " ");
      output = output.concat(kAverage.getMinTuplePercentage() + " ");
      output = output.concat(kAverage.getMaTuplePercentage() + " ");
      output = output.concat("\n");
    }
    System.out.println(output + "\n");
    out.write(output + "\n");
  }

  /**
   * detemrines the average and variance for the differnt query tyuples of select *, aggregation, joins
 * @throws IOException 
   */
  private static void outputAverageAndVarianceForQueryType() throws IOException
  {
    HashMap<String, HashMap<Integer ,AverageAndVarianceKlevel>> avergaes = 
      new HashMap<String, HashMap<Integer ,AverageAndVarianceKlevel>>();
    avergaes.put("star", new HashMap<Integer ,AverageAndVarianceKlevel>());
    avergaes.put("agg", new HashMap<Integer ,AverageAndVarianceKlevel>());
    avergaes.put("join", new HashMap<Integer ,AverageAndVarianceKlevel>());
    ArrayList<Integer> starIds = new ArrayList<Integer>();
    starIds.addAll(Arrays.asList(1,2,3,4,5));
    ArrayList<Integer> aggIds = new ArrayList<Integer>();
    aggIds.addAll(Arrays.asList(6,7,8,9,10));
    ArrayList<Integer> joinIds = new ArrayList<Integer>();
    joinIds.addAll(Arrays.asList(11,12,13,14,15));
    
    Iterator<Integer> queryIterator = queries.keySet().iterator();
    while(queryIterator.hasNext())
    {
      Integer queryKey = queryIterator.next();
      Query q = queries.get(queryKey);
      HashMap<Integer, Klevel> data =  q.getData();
      HashMap<Integer ,AverageAndVarianceKlevel> averages;
      if(starIds.contains(queryKey))
      {
        averages = avergaes.get("star");
      }
      else if(aggIds.contains(queryKey))
      {
        averages = avergaes.get("agg");
      }
      else
      {
        averages = avergaes.get("join");
      }
      
      Iterator<Integer> klevelIterator = data.keySet().iterator();
      while(klevelIterator.hasNext())
      {
        Integer kkey = klevelIterator.next();
        Klevel k = data.get(kkey);
        AverageAndVarianceKlevel averageKlevel;
        if(averages.get(kkey) == null)
        {
          averageKlevel = new AverageAndVarianceKlevel();
        }
        else
        {
          averageKlevel = averages.get(kkey);
        }
          
        averageKlevel.addpercentage(k.getTuplePercentage());
        averages.put(kkey, averageKlevel);
      }
    }
    outputAveragesForQueryTypes(avergaes);
  }

  private static void outputAveragesForQueryTypes(
      HashMap<String, HashMap<Integer ,AverageAndVarianceKlevel>> avergaes) throws IOException
  {
    Iterator<String> queryTypes = avergaes.keySet().iterator();
    String output = "";
    while(queryTypes.hasNext())
    {
      String queryType = queryTypes.next();
      output = output.concat(queryType + "\n");
      HashMap<Integer ,AverageAndVarianceKlevel> averages = avergaes.get(queryType);
      Iterator<Integer> averagekeySet = averages.keySet().iterator();
      while(averagekeySet.hasNext())
      {
        Integer averageKey = averagekeySet.next();
        output = output.concat(averageKey + " ");
        AverageAndVarianceKlevel kAverage = averages.get(averageKey);
        output = output.concat(kAverage.getTuplePercentage() / 5 + " ");
        output = output.concat(kAverage.getMinTuplePercentage() + " ");
        output = output.concat(kAverage.getMaTuplePercentage() + " ");
        output = output.concat("\n");
      }
    }
    System.out.println(output + "\n");
    out.write(output + "\n");
  }

  /**
   * places data within line into correct data store
   * @param line
   * @param queryID
   */
  private static void updateDataStore(String line, Integer queryID)
  {
    if(queries.get(queryID) == null)
    {
      HashMap<Integer, Klevel> data = new HashMap<Integer, Klevel>();
      String [] bits = line.split(" ");
      for(int index = 1; index < 6; index++)
      {
        Klevel newK = new Klevel(Double.parseDouble(bits[index]));
        data.put(index, newK);
      }
      Query q = new Query(data);
      queries.put(queryID, q);
    }
    else
    {
      Query q = queries.get(queryID);
      HashMap<Integer, Klevel> data = q.getData();
      String [] bits = line.split(" ");
      for(int index = 1; index < 6; index++)
      {
        Klevel newK = new Klevel(Double.parseDouble(bits[index-1]));
        data.put(index, newK);
      }
    }
  }
}
