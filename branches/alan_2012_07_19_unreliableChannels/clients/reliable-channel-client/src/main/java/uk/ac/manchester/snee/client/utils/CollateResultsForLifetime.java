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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;

public class CollateResultsForLifetime
{
  
  private static String sep = System.getProperty("file.separator");
  
  public static void main(String [] args)
  {
    try
    {
      File root = new File("/mnt/usb/1st1/EdgeLifetime/");
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
          outputTuplesFile = new File(folder.toString() + sep + "out.txt");
          BufferedReader in = new BufferedReader(new FileReader(outputTuplesFile));
          ArrayList<String> tuples = locateCorrectArrayList(data, bits);
          System.out.println("reading in file for "+  bits[0]);
          readInData(in, tuples);
          in.close();
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
    File output = new File("/local/output");
    output.mkdir();
    Iterator<String> queryKeysIterator = data.keySet().iterator();
    while(queryKeysIterator.hasNext())
    {
      String queryKey = queryKeysIterator.next();
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
    ArrayList<String> queryKeys = new ArrayList<String>(averagedData.keySet());
    Collections.sort(queryKeys);
    queryKeysIterator = queryKeys.iterator();
    while(queryKeysIterator.hasNext())
    {
      String queryKey = queryKeysIterator.next();
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
      outputCollectedFile(distances,output, queryKey);
    }
    
    
  }

  private static void outputCollectedFile(HashMap<String, Distance> distances,
                                          File output, String queryKey) 
  throws IOException
  {
    Iterator<String> distanceKeys = distances.keySet().iterator();
    while(distanceKeys.hasNext())
    {
      while(distanceKeys.hasNext())
      {
        String distanceKey = distanceKeys.next();
        BufferedWriter out = new BufferedWriter(new FileWriter(output.toString() + sep + distanceKey, true));
        HashMap<String, Klevel> kLevels = distances.get(distanceKey).getData();
        ArrayList<String> kkeys = new ArrayList<String>(kLevels.keySet());
        Collections.sort(kkeys);
        Iterator<String> keysIterator = kkeys.iterator();
        while(keysIterator.hasNext())
        {
          String kKey = keysIterator.next();
          Klevel k = kLevels.get(kKey);
          Seed kAvergaeTuples = k.getData().get(kKey);
          DecimalFormat format = new DecimalFormat("#.##");
          if(kAvergaeTuples.getaAverage().get(0) != 0)
            out.write(queryKey + " " + kKey + " " + format.format(kAvergaeTuples.getaAverage().get(0)) + " " + 
                      format.format(kAvergaeTuples.getoAverage().get(0)) + "\n");
        }
        out.flush();
        out.close();
      }
    }
  }

  private static void absorbSeeds(String kKey, HashMap<String, Klevel> kLevels,
                                  File distanceOutput, Klevel averagedK)
  throws IOException
  {
    HashMapList<String, Double> allTuples = new HashMapList<String, Double>();
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
      allTuples.addWithDuplicates("o", Double.parseDouble(tuples.get(0)));
      allTuples.addWithDuplicates("a", Double.parseDouble(tuples.get(1)));
    }
    
    outputAveragedTuples(allTuples, maxValues, kLevel, averagedK, kKey);
  }

  private static void outputAveragedKLevel(HashMap<String, Klevel> ks, File outputFolder) 
  throws IOException
  {
    BufferedWriter outTop = new BufferedWriter(new FileWriter(outputFolder.toString() + sep + "percentages"));
    Iterator<String> kKeys = ks.keySet().iterator();
    while(kKeys.hasNext())
    {
      String kKey = kKeys.next();
      Klevel k = ks.get(kKey);
      File kLevel = new File(outputFolder.toString() + sep + kKey);
      BufferedWriter out = new BufferedWriter(new FileWriter(kLevel.toString() + sep + "percentages"));
      Seed kAvergaeTuples = k.getData().get(kKey);
      DecimalFormat format = new DecimalFormat("#.##");
      out.write(kKey + " " + format.format(kAvergaeTuples.getaAverage().get(0)) + " " + 
          format.format(kAvergaeTuples.getoAverage().get(0)) + "\n");
      out.flush();
      out.close();
    }
  }

  private static void outputAveragedTuples(HashMapList<String, Double> allTuples, 
                                           Integer maxValues, File kLevelOutput,
                                           Klevel k, String kKey)
  throws IOException
  {
    Iterator<String> keys = allTuples.keySet().iterator();
    ArrayList<Double> oAveragedTuples = new ArrayList<Double>(50);
    ArrayList<Double> aAveragedTuples = new ArrayList<Double>(50);
    
    while(keys.hasNext())
    {
      String key = keys.next();
      Double average = 0.0;
      Iterator<Double> tuples = allTuples.get(key).iterator();
      while(tuples.hasNext())
      {
        Double tuple = tuples.next();
        average += tuple;
      }
      average = average / allTuples.get(key).size();
      //double SD = findStandardDeviation(allTuples.get(key));
      
      if(key.equals("o"))
      {
        oAveragedTuples.add(average);
       // oSDTuples.set(index,  SD);
      }
      else if(key.equals("a"))
      {
        aAveragedTuples.add(average);
      // aSDTuples.set(index,  SD);
      }
    }
    Seed average = new Seed();
    average.setaAverage(aAveragedTuples);
    average.setoAverage(oAveragedTuples);
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
    
    
  }

  private static void readInData(BufferedReader in, ArrayList<String> tuples) 
  throws IOException
  {
    String line = null;
    while((line = in.readLine()) != null)
    {
      if(line.contains("new robust"))
      {
        String [] results = line.split(" = ");
        tuples.add(results[1]);
      }
    }
    if(tuples.size() == 0)
    {
      tuples.add("0.0");
      tuples.add("0.0");
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
