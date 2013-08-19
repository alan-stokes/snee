package uk.ac.manchester.snee.client.collate;

import java.io.BufferedReader;
	import java.io.BufferedWriter;
	import java.io.File;
	import java.io.FileReader;
	import java.io.FileWriter;
	import java.io.IOException;
	import java.util.HashMap;
import java.util.Iterator;

	
public class Collate {
	
	 
	  private static String sep = System.getProperty("file.separator");
	  private static File root = new File("/mnt/usb/1st1/finaltuples/casino/condor_edge_Tuples_Casinoex3Results");
	  private static File output = new File("/mnt/usb/1st1/finaltuples/casino/condor_edge_Tuples_Casinoex3Collated");
	 
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
	          String[] firstBits =  queryFolder.toString().split("/");
	          String fileName = firstBits[firstBits.length -1];
	         
	          String[] bits = fileName.split("\\.");
	          File[] distanceListedFiles = queryFolder.listFiles();
  		      int distanceMax = distanceListedFiles.length;
  		      for(int distanceCounter = 0; distanceCounter < distanceMax; distanceCounter++)
  		      {
  		        File resultsFolder = distanceListedFiles[distanceCounter];
  		        if(resultsFolder.isDirectory())
  		        {
  		    	    File results = new File(resultsFolder.toString() + sep + "percentages");
  	            BufferedReader in = new BufferedReader(new FileReader(results));
  	            HashMap<String, Values> tuples = locateCorrectArrayList(data, bits, resultsFolder.getName());
  	            System.out.println("reading in file for "+  bits[0]);
  	            readInData(in, tuples);
  	            in.close();
  		        }
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

	  private static void outputData(HashMap<String, Query> data)
	  throws IOException
	  {
	    output.mkdir();
	    HashMap<String, BufferedWriter> distanceFiles = new HashMap<String, BufferedWriter>();
	    Iterator<String> queryKeys = data.keySet().iterator();
	    String queryKey = queryKeys.next();
	    HashMap<String, Distance> distances = data.get(queryKey).getData();
	    Iterator<String> distanceKeys = distances.keySet().iterator();
	    while(distanceKeys.hasNext())
	    {
	      String distanceKey = distanceKeys.next();
	      File distanceFile = new File( output.toString() +sep + distanceKey);
	      BufferedWriter out = new BufferedWriter(new FileWriter(distanceFile));
	      distanceFiles.put(distanceKey, out);
	    }
	    queryKeys = data.keySet().iterator();
	    while(queryKeys.hasNext())
	    {
	      queryKey = queryKeys.next();
	      distances = data.get(queryKey).getData();
	      distanceKeys = distances.keySet().iterator();
	      while(distanceKeys.hasNext())
	      {
	        String distanceKey = distanceKeys.next();
	        Distance distance = distances.get(distanceKey);
	        writeDistance(distance, distanceFiles.get(distanceKey), queryKey);
	      }
	    } 
	   Iterator<String> writerKeys = distanceFiles.keySet().iterator();
	   while(writerKeys.hasNext())
	   {
		 BufferedWriter writer = distanceFiles.get(writerKeys.next());
		 writer.flush();
		 writer.close();
	   }
	  }

    private static void writeDistance(Distance distance, BufferedWriter out, String queryKey) throws IOException
	{
	  HashMap<String, Klevel> data = distance.getData();
	  Iterator<String> keyIterator = data.keySet().iterator();
	  while(keyIterator.hasNext())
	  {
	    String key = keyIterator.next();
		Klevel klevel = data.get(key);
		HashMap<String, Values> kdata = klevel.getData();
		Iterator<String> klevelKeys = kdata.keySet().iterator();
		while(klevelKeys.hasNext())
		{
		  String klevelKey = klevelKeys.next();
		  Values values = kdata.get(klevelKey);
		  String [] bits = queryKey.split("query");
		  out.write("query-" + bits[1] + " " + klevelKey + " " + values.getStaticPercentage() + " " + values.getAdaptivePercentage() +
				    " " + values.getAggreStaticPercentage() + " " + values.getAggreAdaptivePercentage() + "\n");
		}
	  }
		
	}

	private static void readInData(BufferedReader in, HashMap<String, Values> tuples)
	  throws IOException
	  {
	    String line = null;
	    while((line = in.readLine()) != null)
	    {
	      String [] bits = line.split(" ");
	      Values value = new Values();
	      value.setStaticPercentage(Double.parseDouble(bits[1]));
	      value.setAdaptivePercentage(Double.parseDouble(bits[2]));
	      value.setAggreStaticPercentage(Double.parseDouble(bits[3]));
	      value.setAggreAdaptivePercentage(Double.parseDouble(bits[4]));
	      tuples.put(bits[0], value);
	    }
	  }

	  private static HashMap<String, Values> locateCorrectArrayList(HashMap<String, Query> data, String[] bits, String distance)
	  {
	    Query query = data.get(bits[0]);
	    if(query == null)
	    {
	      query = new Query(new HashMap<String, Distance>());
	      data.put(bits[0], query);
	      Distance d = new Distance(new HashMap<String, Klevel>());
	      query.getData().put(distance, d);
	      Klevel k = new Klevel(new HashMap<String, Values>());
	      d.getData().put(distance, k);
	      return k.getData();
	    }
	    else
	    {
	      Distance d = query.getData().get(distance);
	      if(d == null)
	      {
	        d = new Distance(new HashMap<String, Klevel>());
	        query.getData().put(distance, d);
	        Klevel k = new Klevel(new HashMap<String, Values>());
	        d.getData().put(distance, k);
	        return k.getData();
	      }
	      else
	      {
	        Klevel k = d.getData().get(distance);
	        if(k ==null)
	        {
	          k = new Klevel(new HashMap<String, Values>());
	          d.getData().put(distance, k);
	          return k.getData();
	        }
	        else
	        {
	          return k.getData();
	        }
	      }
	    }
	  }
	}

