package uk.ac.manchester.snee.client.collateUnreliableLifetime;

import java.io.BufferedReader;
	import java.io.BufferedWriter;
	import java.io.File;
import java.io.FileNotFoundException;
	import java.io.FileReader;
	import java.io.FileWriter;
	import java.io.IOException;
import java.util.ArrayList;
	import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

	
public class Collate {
	
	 
	  private static String sep = System.getProperty("file.separator");
	  private static File root = new File("/mnt/usb/1st1/Edge/edge_falure_unreliable/condor_edge_ulifetimeEstimateHeavyLogicalEdgesk2");
	  private static File output = new File("/mnt/usb/1st1/Edge/edge_falure_unreliable/condor_edge_ulifetimeEstimateHeavyLogicalEdgesk2Results");
	 
	  public static void main(String [] args)
	  {
	    try
	    {
	      HashMap<String, Query> data = new HashMap<String, Query>();
	      File[] listedFiles = root.listFiles();
	      int max = listedFiles.length;
	      for(int counter = 0; counter < max; counter++)
	      {
	        File queryFolder = listedFiles[counter];
	        if(queryFolder.isDirectory())
	        {
	          String[] bits = queryFolder.getName().split("\\.");
	          String fileName = bits[0];
	          int klevel = Integer.parseInt(bits[1]);
	          int nodeFailures = Integer.parseInt(bits[2]);
	          double distance = Double.parseDouble(bits[3]);
	          Double lifetime = 0.0;
	          if(distance == 1)
	          {
	            System.out.println("reading " + queryFolder.toString());
	            lifetime = readInLifetime(queryFolder);
	            storeInCorrectPoint(lifetime, klevel, nodeFailures, fileName, data);
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

	  private static void storeInCorrectPoint(Double lifetime, Integer klevel,
        Integer nodeFailures, String fileName, HashMap<String, Query> data)
    {
	    Query query = data.get(fileName);
      if(query == null)
      {
        query = new Query(new HashMap<String, Klevel>());
        data.put(fileName, query);
        Klevel k = new Klevel(new HashMap<String, Double>());
        query.getData().put(klevel.toString(), k);
        k.getData().put(nodeFailures.toString(), lifetime);
      }
      else
      {
        Klevel k = query.getData().get(klevel.toString());
        if(k == null)
        {
          k = new Klevel(new HashMap<String, Double>());
          query.getData().put(klevel.toString(), k);
          k.getData().put(nodeFailures.toString(), lifetime);
        }
        else
        {
          query.getData().put(klevel.toString(), k);
          k.getData().put(nodeFailures.toString(), lifetime);
        }
      } 
    }

    private static Double readInLifetime(File queryFolder)
	  throws IOException
    {
      BufferedReader in = new BufferedReader(new FileReader(
          new File(queryFolder + sep + "out.txt")));
      String line = null;
      while((line = in.readLine()) != null)
      {
        if(line.split("new lifetime = ").length == 2)
        {
          in.close();
          return Double.parseDouble(line.split("new lifetime = ")[1]);
        }
      }
      in.close();
      return null;
    }

    private static void outputData(HashMap<String, Query> data)
	  throws IOException
	  {
	    output.mkdir();
	    BufferedWriter out = 
	      new BufferedWriter(new FileWriter(new File(output.toString() + sep + "results")));
	    Iterator<String> queryKeys = queryidSort(data.keySet()).iterator();
	    while(queryKeys.hasNext())
	    {
  	    String queryKey = queryKeys.next();
  	    HashMap<String, Klevel> klevel = data.get(queryKey).getData();
  	    Iterator<String> klevelKeys = sort(klevel.keySet()).iterator();
  	    while(klevelKeys.hasNext())
  	    {
  	      String klevelKey = klevelKeys.next();
  	      HashMap<String, Double> kdata = klevel.get(klevelKey).getData();
  	      Iterator<String> failureKeys = sort(kdata.keySet()).iterator();
  	      String text = queryKey + " " + klevelKey + " ";
  	      while(failureKeys.hasNext())
  	      {
  	        String failureCount = failureKeys.next();
  	        Double lifetime = kdata.get(failureCount);
  	        text = text.concat(lifetime + " ");
  	      }
  	      out.write(text + "\n");
  	    }
	    }
	    out.flush();
	    out.close();
	  }

    
    private static ArrayList<String> queryidSort(Set<String> keys)
    {
      ArrayList<String> newkeys = new ArrayList<String>();
      for(int index =0; index < 90; index++)
      {
        newkeys.add(null);
      }
      Iterator<String> keyIterator = keys.iterator();
      while(keyIterator.hasNext())
      {
        String key = keyIterator.next();
        int position = Integer.parseInt(key.split("query")[1]);
        newkeys.set(position, key);
      }
      for(int index =0; index < 90; index++)
      {
        newkeys.remove(null);
      }
      return newkeys;
    }
    
    private static ArrayList<String> sort(Set<String> keys)
    {
      ArrayList<String> newkeys = new ArrayList<String>();
      for(int index =0; index < 90; index++)
      {
        newkeys.add(null);
      }
      Iterator<String> keyIterator = keys.iterator();
      while(keyIterator.hasNext())
      {
        String key = keyIterator.next();
        int position = Integer.parseInt(key);
        newkeys.set(position, key);
      }
      for(int index =0; index < 90; index++)
      {
        newkeys.remove(null);
      }
      return newkeys;
    }

	}

