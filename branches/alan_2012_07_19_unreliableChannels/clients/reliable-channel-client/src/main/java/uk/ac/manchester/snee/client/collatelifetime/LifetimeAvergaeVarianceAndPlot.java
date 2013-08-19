package uk.ac.manchester.snee.client.collatelifetime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class LifetimeAvergaeVarianceAndPlot
{
  static private HashMap<Integer, Query> queries = new HashMap<Integer, Query>();
  static private File inputFile = new File("/mnt/usb/1st1/Edge/edge_failure_lifetime/allValuesForAllkeKn");
  
  /**
   * main method for entering
   * @param args
   */
  public static void main(String [] args)
  {
    try
    {
      BufferedReader in;
      if(args.length ==1 )
        in = new BufferedReader(new FileReader(new File(args[0])));
      else
        in = new BufferedReader(new FileReader(inputFile));
      
      String line = "";
      while((line = in.readLine())!= null)
      {
        String [] bits = line.split("\t");
        if(bits.length !=0)
        {
          Integer queryID = Integer.parseInt(bits[0]);
          Integer klevel = Integer.parseInt(bits[2]);
          updateDataStore(line, queryID, klevel);
        }
      }
      outputAverageAndVarianceForQueryType();
      outputAverageAndVarianceForAllTopologies();
      
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
   */
  private static void outputAverageAndVarianceForAllTopologies()
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
        
        averageKlevel.addKe2Lifetime(k.getKe2Lifetime());
        averageKlevel.addKe3Lifetime(k.getKe3Lifetime());
        averageKlevel.addOptimsiticLifetime(k.getOptimsiticLifetime());
        averageKlevel.addPessimisticLifetime(k.getPessimisticLifetime());
        averageKlevel.addStaticLifetime(k.getStaticLifetime());
        avergaes.put(kkey, averageKlevel);
      }
    }
    outputAveragesFortopologyies(avergaes);
  }

    
  private static void outputAveragesFortopologyies(
      HashMap<Integer, AverageAndVarianceKlevel> avergaes)
  {
    String output = "";
    Iterator<Integer> averagekeySet = avergaes.keySet().iterator();
    while(averagekeySet.hasNext())
    {
      Integer averageKey = averagekeySet.next();
      output = output.concat(averageKey + " ");
      AverageAndVarianceKlevel kAverage = avergaes.get(averageKey);
      output = output.concat(kAverage.getKe2Lifetime() / 16 + " ");
      output = output.concat(kAverage.getMinKe2Lifetime() + " ");
      output = output.concat(kAverage.getMaxKe2Lifetime() + " ");
      output = output.concat(kAverage.getKe3Lifetime() / 16 + " ");
      output = output.concat(kAverage.getMinKe3Lifetime() + " ");
      output = output.concat(kAverage.getMaxKe3Lifetime() + " ");
      output = output.concat(kAverage.getOptimsiticLifetime() / 16 + " ");
      output = output.concat(kAverage.getMinOptimsiticLifetime() + " ");
      output = output.concat(kAverage.getMaxOptimsiticLifetime() + " ");
      output = output.concat(kAverage.getPessimisticLifetime() / 16 + " ");
      output = output.concat(kAverage.getMinPessimisticLifetime() + " ");
      output = output.concat(kAverage.getMaxPessimisticLifetime() + " ");
      output = output.concat(kAverage.getStaticLifetime() / 16 + " ");
      output = output.concat(kAverage.getMinStaticLifetime() + " ");
      output = output.concat(kAverage.getMaxStaticLifetime() + " ");
      output = output.concat("\n");
    }
    System.out.println(output + "\n");
  }

  /**
   * detemrines the average and variance for the differnt query tyuples of select *, aggregation, joins
   */
  private static void outputAverageAndVarianceForQueryType()
  {
    HashMap<String, HashMap<Integer ,AverageAndVarianceKlevel>> avergaes = 
      new HashMap<String, HashMap<Integer ,AverageAndVarianceKlevel>>();
    avergaes.put("star", new HashMap<Integer ,AverageAndVarianceKlevel>());
    avergaes.put("agg", new HashMap<Integer ,AverageAndVarianceKlevel>());
    avergaes.put("join", new HashMap<Integer ,AverageAndVarianceKlevel>());
    ArrayList<Integer> starIds = new ArrayList<Integer>();
    starIds.addAll(Arrays.asList(1,2,3,4,5,6));
    ArrayList<Integer> aggIds = new ArrayList<Integer>();
    aggIds.addAll(Arrays.asList(30,31,32,33,34,36));
    ArrayList<Integer> joinIds = new ArrayList<Integer>();
    joinIds.addAll(Arrays.asList(60,64,67,68));
    
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
          
        averageKlevel.addKe2Lifetime(k.getKe2Lifetime());
        averageKlevel.addKe3Lifetime(k.getKe3Lifetime());
        averageKlevel.addOptimsiticLifetime(k.getOptimsiticLifetime());
        averageKlevel.addPessimisticLifetime(k.getPessimisticLifetime());
        averageKlevel.addStaticLifetime(k.getStaticLifetime());
        averages.put(kkey, averageKlevel);
      }
    }
    outputAveragesForQueryTypes(avergaes);
  }

  private static void outputAveragesForQueryTypes(
      HashMap<String, HashMap<Integer ,AverageAndVarianceKlevel>> avergaes)
  {
    Iterator<String> queryTypes = avergaes.keySet().iterator();
    String output = "";
    while(queryTypes.hasNext())
    {
      String queryType = queryTypes.next();
      output = output.concat(queryType + " ");
      HashMap<Integer ,AverageAndVarianceKlevel> averages = avergaes.get(queryType);
      Iterator<Integer> averagekeySet = averages.keySet().iterator();
      while(averagekeySet.hasNext())
      {
        Integer averageKey = averagekeySet.next();
        output = output.concat(averageKey + " ");
        AverageAndVarianceKlevel kAverage = averages.get(averageKey);
        if(queryType.equals("star") || queryType.equals("agg"))
        {
          output = output.concat(kAverage.getKe2Lifetime() / 6 + " ");
          output = output.concat(kAverage.getMinKe2Lifetime() + " ");
          output = output.concat(kAverage.getMaxKe2Lifetime() + " ");
          output = output.concat(kAverage.getKe3Lifetime() / 6 + " ");
          output = output.concat(kAverage.getMinKe3Lifetime() + " ");
          output = output.concat(kAverage.getMaxKe3Lifetime() + " ");
          output = output.concat(kAverage.getOptimsiticLifetime() / 6 + " ");
          output = output.concat(kAverage.getMinOptimsiticLifetime() + " ");
          output = output.concat(kAverage.getMaxOptimsiticLifetime() + " ");
          output = output.concat(kAverage.getPessimisticLifetime() / 6 + " ");
          output = output.concat(kAverage.getMinPessimisticLifetime() + " ");
          output = output.concat(kAverage.getMaxPessimisticLifetime() + " ");
          output = output.concat(kAverage.getStaticLifetime() / 6+ " ");
          output = output.concat(kAverage.getMinStaticLifetime() + " ");
          output = output.concat(kAverage.getMaxStaticLifetime() + " ");
        }
        else
        {
          output = output.concat(kAverage.getKe2Lifetime() / 4 + " ");
          output = output.concat(kAverage.getMinKe2Lifetime() + " ");
          output = output.concat(kAverage.getMaxKe2Lifetime() + " ");
          output = output.concat(kAverage.getKe3Lifetime() / 4 + " ");
          output = output.concat(kAverage.getMinKe3Lifetime() + " ");
          output = output.concat(kAverage.getMaxKe3Lifetime() + " ");
          output = output.concat(kAverage.getOptimsiticLifetime() / 4 + " ");
          output = output.concat(kAverage.getMinOptimsiticLifetime() + " ");
          output = output.concat(kAverage.getMaxOptimsiticLifetime() + " ");
          output = output.concat(kAverage.getPessimisticLifetime() / 4 + " ");
          output = output.concat(kAverage.getMinPessimisticLifetime() + " ");
          output = output.concat(kAverage.getMaxPessimisticLifetime() + " ");
          output = output.concat(kAverage.getStaticLifetime()/ 4 + " ");
          output = output.concat(kAverage.getMinStaticLifetime() + " ");
          output = output.concat(kAverage.getMaxStaticLifetime() + " ");
        }
        output = output.concat("\n");
      }
    }
    System.out.println(output + "\n");
  }

  /**
   * places data within line into correct data store
   * @param line
   * @param queryID
   * @param klevel
   */
  private static void updateDataStore(String line, Integer queryID, Integer klevel)
  {
    if(queries.get(queryID) == null)
    {
      HashMap<Integer, Klevel> data = new HashMap<Integer, Klevel>();
      Klevel newK = new Klevel(line);
      data.put(klevel, newK);
      Query q = new Query(data);
      queries.put(queryID, q);
    }
    else
    {
      Query q = queries.get(queryID);
      HashMap<Integer, Klevel> data = q.getData();
      Klevel newK = new Klevel(line);
      data.put(klevel, newK);
    }
  }
}
