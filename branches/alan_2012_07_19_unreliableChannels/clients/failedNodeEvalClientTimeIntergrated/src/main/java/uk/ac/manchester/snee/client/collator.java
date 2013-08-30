package uk.ac.manchester.snee.client;
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


public class collator
{
  private static File inputFolder = new File("/media/Data/smalltopologysetseveralnodefailure");
  private static File outputFolder = new File("/media/Data/smalltopologysetseveralnodefailurefinalResultsjoin");
  private static HashMap<String, ArrayList<String>> gData = new HashMap<String, ArrayList<String>>();
  private static HashMap<String, ArrayList<String>> pData = new HashMap<String, ArrayList<String>>();
 
  public static void main(String [] args)
  {
    File[] queriesInFolder = inputFolder.listFiles();
    for(int folderIndex =0; folderIndex < queriesInFolder.length; folderIndex++)
    {
      File queryFolder = queriesInFolder[folderIndex];
      if(queryFolder.isDirectory())
      {
        try
        {
          BufferedReader in = new BufferedReader(new FileReader(queryFolder + "/out.txt"));
          String line = "";
          int recorded = 0;
          ArrayList<String> gDataQ = new ArrayList<String>();
          ArrayList<String> pDataQ = new ArrayList<String>();
          for(int index = 0; index < 8; index++)
          {
            gDataQ.add("");
            pDataQ.add("");
          }
          boolean passedBlurb = false;
          while((line = in.readLine())!= null)
          {
            if(line.equals("initisling client"))
              passedBlurb = true;
            Double lifetime = convertToNumber(line);
            if(lifetime != null && passedBlurb)
            {
              if(recorded < 8)
              {
                gDataQ.set(recorded, line);
              }
              else
              {
                pDataQ.set(recorded - 8, line);
              }
              recorded++;
            }
          }
          gData.put(queryFolder.getName(), gDataQ);
          pData.put(queryFolder.getName(), pDataQ);
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
    outputData();
  }

  private static Double convertToNumber(String line)
  {
   try
   {
     return Double.parseDouble(line);
   }
   catch(Exception e)
   {
     return null;
   }
  }

  private static void outputData()
  {
    try
    {
      BufferedWriter out = new BufferedWriter(new FileWriter(outputFolder));
     ArrayList<String> sortedKeys = sort(gData.keySet());
     Iterator<String> sortedKeysIterator = sortedKeys.iterator();
     while(sortedKeysIterator.hasNext())
     {
       String key = sortedKeysIterator.next();
       String line = key + " ";
       ArrayList<String> gDataQ = gData.get(key);
       ArrayList<String> pDataQ = pData.get(key);
       Iterator<String> qDataQIterator = gDataQ.iterator();
       Iterator<String> pDataQIterator = pDataQ.iterator();
       while(qDataQIterator.hasNext())
       {
         line = line.concat(qDataQIterator.next() + " ");
       }
       while(pDataQIterator.hasNext())
       {
         line = line.concat(pDataQIterator.next() + " ");
       }
       out.write(line + "\n");
     }
     out.flush();
     out.close();
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  
  }

  private static ArrayList<String> sort(Set<String> keySet)
  {
   ArrayList<String> sorted = new ArrayList<String>();
   for(int index = 0; index < 90; index++)
   {
     sorted.add(null);
   }
    Iterator<String> keys = keySet.iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      String [] bits = key.split("query");
      sorted.set(Integer.parseInt(bits[1]), key);
    }
    for(int index = 0; index < 90; index++)
    {
      sorted.remove(null);
    }
    return sorted;
  }
}