package uk.ac.manchester.snee.client.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;

public class collateResults
{
  
  private static String sep = System.getProperty("file.separator");
  
  public static void main(String [] args)
  {
    try
    {
      File root = new File("/local/BigSubmit/BigSubmit2/");
      HashMap<String, Query> data = new HashMap<String, Query>();
      File[] listedFiles = root.listFiles();
      int max = listedFiles.length;
      for(int counter = 0; counter < max; counter++)
      {
        File folder = listedFiles[counter];
        if(folder.isDirectory())
        {
          String[] firstBits =  folder.toString().split("/");
          String fileName = firstBits[firstBits.length -1];
          
        // Process extractor = Runtime.getRuntime().exec(new String("tar -xf /local/BigSubmit/" + fileName + "/output.tgz")); 
        // extractor.waitFor();
         
          String[] bits = fileName.split("\\.");
          File outputTuplesFile = null;
          if(bits.length == 4)
          {
            outputTuplesFile = new File(folder.toString() + sep + "output/" + bits[0] + sep+
                                        "AutonomicManData" + sep + "executer" + sep +
                                         "1.0" + sep + "tupleOutput");
          }
          else
          {
            outputTuplesFile = new File(folder.toString() + sep + "output/" + bits[0] + sep + 
                                        "AutonomicManData" + sep + "executer" + sep + bits[3] + 
                                         "." + bits[4] + sep + "tupleOutput");
          }
          if(outputTuplesFile.exists())
          {
            BufferedReader in = new BufferedReader(new FileReader(outputTuplesFile));
            ArrayList<String> tuples = locateCorrectArrayList(data, bits);
            System.out.println("reading in file for "+  bits[0]);
            readInData(in, tuples);
            in.close();
          }
        }
      }
      
      System.out.println("starting output");
      outputData(data);
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  private static void outputData(  HashMap<String, Query> data)
  throws IOException
  {
    HashMap<String, Query> averagedData = new HashMap<String, Query>();
    File distanceOutput = null;
    File output = new File("/local/BigSubmit/output");
    output.mkdir();
    Iterator<String> queryKeys = data.keySet().iterator();
    while(queryKeys.hasNext())
    {
      String queryKey = queryKeys.next();
      Query query = new Query(new HashMap<String, Distance>());
      averagedData.put(queryKey, query);
      File queryOutput = new File(output.toString() + sep + queryKey);
      queryOutput.mkdir();
      HashMap<String, Distance> distances = data.get(queryKey).getData();
      Iterator<String> distanceKeys = distances.keySet().iterator();
      while(distanceKeys.hasNext())
      {
        String distanceKey = distanceKeys.next();
        Distance d = new Distance(new HashMap<String, Klevel>());
        query.getData().put(distanceKey, d);
        distanceOutput = new File(queryOutput.toString() + sep + distanceKey);
        distanceOutput.mkdir();
        HashMap<String, Klevel> kLevels = distances.get(distanceKey).getData();
        Iterator<String> klevelKeys = kLevels.keySet().iterator();
        while(klevelKeys.hasNext())
        {
          String kKey = klevelKeys.next();
          Klevel k = new Klevel(new HashMap<String, Seed>());
          d.getData().put(kKey, k);
          absorbSeeds(kKey, kLevels, distanceOutput, k);
        }
      }
    }
    data = null;
    queryKeys = averagedData.keySet().iterator();
    while(queryKeys.hasNext())
    {
      String queryKey = queryKeys.next();
      if(queryKey.equals("query68"))
        System.out.println();
      File queryOutput = new File(output.toString() + sep + queryKey);
      HashMap<String, Distance> distances = averagedData.get(queryKey).getData();
      Iterator<String> distanceKeys = distances.keySet().iterator();
      while(distanceKeys.hasNext())
      {
        String distanceKey = distanceKeys.next();
        distanceOutput = new File(queryOutput.toString() + sep + distanceKey);
        HashMap<String, Klevel> kLevels = distances.get(distanceKey).getData();
        outputAveragedKLevel(kLevels, distanceOutput);
      }
    }
    
    
  }

  private static void absorbSeeds(String kKey, HashMap<String, Klevel> kLevels,
                                  File distanceOutput, Klevel averagedK)
  throws IOException
  {
    HashMapList<String, Integer> allTuples = new HashMapList<String, Integer>();
    Integer maxValues = 0;
    
    HashMap<String, Seed> seeds = kLevels.get(kKey).getData();
    Iterator<String> seedKeys = seeds.keySet().iterator();
    File kLevel = new File(distanceOutput.toString() + sep + kKey);
    kLevel.mkdir();
    
    boolean first = true;
    while(seedKeys.hasNext())
    {
      String seedKey = seedKeys.next();
      ArrayList<String> tuples = seeds.get(seedKey).getTuples();
      Iterator<String> tupleIterator = tuples.iterator();
      while(tupleIterator.hasNext())
      {
        String tupleLine = tupleIterator.next();
        String [] bits = tupleLine.split(" ");
        if(first)
        {
          maxValues = Integer.parseInt(bits[1]);
          first = false;
        }
        allTuples.addWithDuplicates("o-" + bits[0], Integer.parseInt(bits[2]));
        allTuples.addWithDuplicates("a-" + bits[0], Integer.parseInt(bits[3]));
        allTuples.addWithDuplicates("oat-" + bits[0], Integer.parseInt(bits[5]));
        allTuples.addWithDuplicates("aat-" + bits[0], Integer.parseInt(bits[4]));
      }
    }
    
    outputAveragedTuples(allTuples, maxValues, kLevel, averagedK, kKey);
  }

  private static void outputAveragedKLevel(HashMap<String, Klevel> ks, File outputFolder) 
  throws IOException
  {
    ArrayList<Double> oPercentage = new ArrayList<Double>();
    for(int index =0; index <6; index ++)
      oPercentage.add(null);
    ArrayList<Double> aPercentage = new ArrayList<Double>();
    for(int index =0; index <6; index ++)
      aPercentage.add(null);
    ArrayList<Double> oCPercentage = new ArrayList<Double>();
    for(int index =0; index <6; index ++)
      oCPercentage.add(null);
    ArrayList<Double> aCPercentage = new ArrayList<Double>();
    for(int index =0; index <6; index ++)
      aCPercentage.add(null);
    
    BufferedWriter outTop = new BufferedWriter(new FileWriter(outputFolder.toString() + sep + "percentages"));
    Iterator<String> kKeys = ks.keySet().iterator();
    while(kKeys.hasNext())
    {
      String kKey = kKeys.next();
      Klevel k = ks.get(kKey);
      File kLevel = new File(outputFolder.toString() + sep + kKey);
      BufferedWriter out = new BufferedWriter(new FileWriter(kLevel.toString() + sep + "percentages"));
      Seed kAvergaeTuples = k.getData().get(kKey);
      Iterator<Double> tuples = kAvergaeTuples.getoAverage().iterator();
      tuples.next();
      while(tuples.hasNext())
      {
        Double tuple = tuples.next();
        oPercentage.set(Integer.parseInt(kKey),(tuple / kAvergaeTuples.getMax()) * 100);
      }
      tuples = kAvergaeTuples.getaAverage().iterator();
      tuples.next();
      while(tuples.hasNext())
      {
        Double tuple = tuples.next();
        aPercentage.set(Integer.parseInt(kKey),(tuple / kAvergaeTuples.getMax()) * 100);
      }
      tuples = kAvergaeTuples.getaAAverage().iterator();
      tuples.next();
      while(tuples.hasNext())
      {
        Double tuple = tuples.next();
        aCPercentage.set(Integer.parseInt(kKey),(tuple / (kAvergaeTuples.getMax() * 9)) * 100);
      }
      tuples = kAvergaeTuples.getoAAverage().iterator();
      tuples.next();
      while(tuples.hasNext())
      {
        Double tuple = tuples.next();
        oCPercentage.set(Integer.parseInt(kKey),(tuple / (kAvergaeTuples.getMax() * 9)) * 100);
      }
      
     
      DecimalFormat format = new DecimalFormat("#.##");
      out.write(kKey + " " + format.format(oPercentage.get(Integer.parseInt(kKey))) + " " + 
                format.format(aPercentage.get(Integer.parseInt(kKey))) + "\n");
      out.flush();
      out.close();
    }
    
    Iterator<Double> oiterator = oPercentage.iterator();
    Iterator<Double> aiterator = aPercentage.iterator();
    Iterator<Double> oCiterator = oCPercentage.iterator();
    Iterator<Double> aCiterator = aCPercentage.iterator();
    oiterator.next();
    oiterator.next();
    aiterator.next();
    aiterator.next();
    oCiterator.next();
    oCiterator.next();
    aCiterator.next();
    aCiterator.next();
    
    int index = 2;
    while(oiterator.hasNext())
    {
      Double o = oiterator.next();
      if(o == null)
        o = 0.0;
      Double a = aiterator.next();
      if(a == null)
        a = 0.0;
      Double oA = oCiterator.next();
      if(oA == null)
        oA = 0.0;
      Double aA = aCiterator.next();
      if(aA == null)
        aA = 0.0;
      DecimalFormat format = new DecimalFormat("#.##");
      outTop.write(index + " " + format.format(o) + " " + format.format(a) +  " " + format.format(oA) +
                   " " + format.format(aA) + "\n");
      index++;
    }
    outTop.flush();
    outTop.close();
  }

  private static void outputAveragedTuples(HashMapList<String, Integer> allTuples, 
                                           Integer maxValues, File kLevelOutput,
                                           Klevel k, String kKey)
  throws IOException
  {
    Iterator<String> keys = allTuples.keySet().iterator();
    ArrayList<Double> oAveragedTuples = new ArrayList<Double>(50);
    ArrayList<Double> aAveragedTuples = new ArrayList<Double>(50);
    ArrayList<Double> oSDTuples = new ArrayList<Double>(50);
    ArrayList<Double> aSDTuples = new ArrayList<Double>(50);
    ArrayList<Double> aAGConTuples = new ArrayList<Double>(50);
    ArrayList<Double> oAGConTuples = new ArrayList<Double>(50);
    ArrayList<Double> oAggSDTuples = new ArrayList<Double>(50);
    ArrayList<Double> aAggSDTuples = new ArrayList<Double>(50);
    for(int index = 0; index <= 50; index++)
    {
      oAveragedTuples.add(null);
      aAveragedTuples.add(null);
      oSDTuples.add(null);
      aSDTuples.add(null);
      aAGConTuples.add(null);
      oAGConTuples.add(null);
      oAggSDTuples.add(null);
      aAggSDTuples.add(null);
    }    
    
    while(keys.hasNext())
    {
      String key = keys.next();
      String [] bits = key.split("-");
      int index = Integer.parseInt(bits[1]);
      Double average = 0.0;
      Iterator<Integer> tuples = allTuples.get(key).iterator();
      while(tuples.hasNext())
      {
        Integer tuple = tuples.next();
        average += tuple;
      }
      average = average / allTuples.get(key).size();
      double SD = findStandardDeviation(allTuples.get(key));
      
      if(bits[0].equals("o"))
      {
        oAveragedTuples.set(index, average);
        oSDTuples.set(index,  SD);
      }
      else if(bits[0].equals("a"))
      {
        aAveragedTuples.set(index, average);
        aSDTuples.set(index,  SD);
      }
      else if(bits[0].equals("oat"))
      {
        oAGConTuples.set(index, average);
        oAggSDTuples.set(index,  SD);
      }
      else
      {
        aAGConTuples.set(index, average);
        aAggSDTuples.set(index,  SD);
      }
      
    }
    Seed average = new Seed();
    average.setaAverage(aAveragedTuples);
    average.setoAverage(oAveragedTuples);
    average.setaAAverage(aAGConTuples);
    average.setoAAverage(oAGConTuples);
    average.setMax(maxValues);
    k.getData().put(kKey, average);
    
    
    BufferedWriter out = new BufferedWriter(new FileWriter(kLevelOutput.toString() + sep + "avergaedTuples"));
    Iterator<Double> adata = aAveragedTuples.iterator();
    Iterator<Double> odata = oAveragedTuples.iterator();
    adata.next();
    odata.next();
    int counter = 1;
    DecimalFormat format = new DecimalFormat("#.##");
    while(adata.hasNext())
    {
      Double a = adata.next();
      Double o = odata.next();
      out.write(counter + " " + maxValues + " " + format.format(o) + " " + format.format(a) + "\n");
    }
    out.flush();
    out.close();
    
    out = new BufferedWriter(new FileWriter(kLevelOutput.toString() + sep + "SDTuples"));
    adata = aSDTuples.iterator();
    odata = oSDTuples.iterator();
    adata.next();
    odata.next();
    counter = 1;
    while(adata.hasNext())
    {
      Double a = adata.next();
      Double o = odata.next();
      out.write(counter + " " + maxValues + " " + format.format(o) + " " + format.format(a) + "\n");
    }
    double aAverageSD = 0.0;
    double oAverageSD = 0.0;
    Iterator<Double> tuples = aSDTuples.iterator();
    tuples.next();
    
    while(tuples.hasNext())
    {
      Double tuple = tuples.next();
      aAverageSD += tuple;
    }
    aAverageSD = aAverageSD / aSDTuples.size();
    tuples = oSDTuples.iterator();
    tuples.next();
    while(tuples.hasNext())
    {
      Double tuple = tuples.next();
      oAverageSD += tuple;
    }
    oAverageSD = oAverageSD / oSDTuples.size();
    
    out.write(format.format(oAverageSD) + " " + format.format(aAverageSD) + "\n");
    out.flush();
    out.close();
    
    out = new BufferedWriter(new FileWriter(kLevelOutput.toString() + sep + "SDAggTuples"));
    adata = aAggSDTuples.iterator();
    odata = oAggSDTuples.iterator();
    adata.next();
    odata.next();
    counter = 1;
    while(adata.hasNext())
    {
      Double a = adata.next();
      Double o = odata.next();
      out.write(counter + " " + maxValues + " " + format.format(o) + " " + format.format(a) + "\n");
    }
    aAverageSD = 0.0;
    oAverageSD = 0.0;
    tuples = aAggSDTuples.iterator();
    tuples.next();
    
    while(tuples.hasNext())
    {
      Double tuple = tuples.next();
      aAverageSD += tuple;
    }
    aAverageSD = aAverageSD / aSDTuples.size();
    tuples = oSDTuples.iterator();
    tuples.next();
    while(tuples.hasNext())
    {
      Double tuple = tuples.next();
      oAverageSD += tuple;
    }
    oAverageSD = oAverageSD / oSDTuples.size();
    
    out.write(format.format(oAverageSD) + " " + format.format(aAverageSD) + "\n");
    out.flush();
    out.close();
    
    out = new BufferedWriter(new FileWriter(kLevelOutput.toString() + sep + "AggAvergaedTuples"));
    adata = aAGConTuples.iterator();
    odata = oAGConTuples.iterator();
    adata.next();
    odata.next();
    counter = 1;
    format = new DecimalFormat("#.##");
    while(adata.hasNext())
    {
      Double a = adata.next();
      Double o = odata.next();
      out.write(counter + " " + maxValues + " " + format.format(o) + " " + format.format(a) + "\n");
    }
    out.flush();
    out.close();
    
    
  }

  private static void readInData(BufferedReader in, ArrayList<String> tuples) 
  throws IOException
  {
    String line = null;
    while((line = in.readLine()) != null)
    {
      tuples.add(line);
    }
  }

  private static ArrayList<String> locateCorrectArrayList(HashMap<String, Query> data, String[] bits)
  {
    Query query = data.get(bits[0]);
    if(query == null)
    {
      query = new Query(new HashMap<String, Distance>());
      data.put(bits[0], query);
      Distance d = new Distance(new HashMap<String, Klevel>());
      String key = null;
      if(bits.length == 4)
        key = "1.0";
      else
        key = bits[3] + "." + bits[4];
      query.getData().put(key, d);
      Klevel k = new Klevel(new HashMap<String, Seed>());
      key = bits[1];
      d.getData().put(key, k);
      Seed s = new Seed();
      key = bits[2];
      k.getData().put(key, s);
      return s.getTuples();
    }
    else
    {
      String key = null;
      if(bits.length == 4)
        key = "1.0";
      else
        key = bits[3] + "." + bits[4];
      Distance d = query.getData().get(key);
      if(d == null)
      {
        d = new Distance(new HashMap<String, Klevel>());
        query.getData().put(key, d);
        key = "";
        Klevel k = new Klevel(new HashMap<String, Seed>());
        key = bits[1];
        d.getData().put(key, k);
        Seed s = new Seed();
        key = bits[2];
        k.getData().put(key, s);
        return s.getTuples();
      }
      else
      {
        key = bits[1];
        Klevel k = d.getData().get(key);
        if(k ==null)
        {
          k = new Klevel(new HashMap<String, Seed>());
          key = bits[1];
          d.getData().put(key, k);
          Seed s = new Seed();
          key = bits[2];
          k.getData().put(key, s);
          return s.getTuples();
        }
        else
        {
          Seed s = new Seed();
          key = bits[2];
          k.getData().put(key, s);
          return s.getTuples();
        }
      }
    }
  }
  
  public static double findStandardDeviation(ArrayList<Integer> array)
  {
    double mean = findMean(array);
    double d1 = 0;
    double d2 = 0;
    double sum = 0;
    for(int i = 0; i < array.size(); i++)
    {
      d2 = (mean - array.get(i))*(mean - array.get(i));
      d1 = d2 + d1;
    }
    return Math.sqrt((d1/(array.size()-1)));
  }
  
  public static double findMean(ArrayList<Integer> array)
  {
    double total = 0;
    for(int i = 0; i < array.size(); i++)
    {
      total = total + array.get(i);
    }
    return total/array.size();
  }

  
  

}
