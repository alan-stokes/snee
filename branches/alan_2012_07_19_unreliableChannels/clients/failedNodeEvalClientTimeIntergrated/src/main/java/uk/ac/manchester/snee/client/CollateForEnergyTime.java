package uk.ac.manchester.snee.client;

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
import java.util.Set;

public class CollateForEnergyTime
{
  private static File inputFolder = new File("/mnt/usb/1st1/severalNodeFailure/largetopologysetseveralNodeFailureQoSset2final");
  private static File outputFolder = new File("/mnt/usb/1st1/severalNodeFailure/largetopologysetseveralNodeFailureQoSset2finalResultsTEL");
  private static HashMap<String, adaptationStore> gData = new HashMap<String, adaptationStore>();
  private static HashMap<String, adaptationStore> pData = new HashMap<String, adaptationStore>();
  private static String sep = System.getProperty("file.separator");
  private static ArrayList<Integer> gPoints = new ArrayList<Integer>();
  private static ArrayList<Integer> pPoints = new ArrayList<Integer>();
  
  
  public static void main(String [] args)
  {
    try
    {
      File[] queriesInFolder = inputFolder.listFiles();
      gPoints.addAll(Arrays.asList(1,3,6,10,15,21,28,36));
      pPoints.addAll(Arrays.asList(37, 39, 42, 46, 51, 57, 64, 72));
      
      for(int folderIndex =0; folderIndex < queriesInFolder.length; folderIndex++)
      {
        File queryFolder = queriesInFolder[folderIndex];
        if(queryFolder.isDirectory())
        {
          System.out.println("processing query " + queryFolder.getName());
          File autonomicFolder = new File(queryFolder.toString()+sep+"output"+sep+queryFolder.getName()+sep+"AutonomicManData");
          File[] adaptations = autonomicFolder.listFiles();
          if(adaptations != null)
          {
            for(int AdaptationIndex =0; AdaptationIndex < adaptations.length; AdaptationIndex++)
            {
              File adaptationFolder = adaptations[AdaptationIndex];
              if(adaptationFolder.getName().split("Adapt").length != 1)
              {
                Integer adaptationID = Integer.parseInt(adaptationFolder.getName().split("Adaption")[1]);
                if(adaptationID < 37)
                {
                  if(gPoints.contains(adaptationID))
                  {
                    System.out.println("adding data for G at query " + queryFolder.getName() + " for adaptation " + adaptationID);
                    adaptationStore store = gData.get(queryFolder.getName());
                    if(store == null)
                      store = new adaptationStore();
                    addData(store, adaptationID, adaptationFolder, gPoints, queryFolder, gData);
                  }
                }
                else
                {
                  if(pPoints.contains(adaptationID))
                  {
                    System.out.println("adding data for P at query " + queryFolder.getName() + " for adaptation " + adaptationID);
                    adaptationStore store = pData.get(queryFolder.getName());
                    if(store == null)
                      store = new adaptationStore();
                    addData(store, adaptationID, adaptationFolder, pPoints, queryFolder, pData);
                  }
                }
              }
            }
          }
        }
      }
    outputData();
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


  private static void outputData() throws IOException
  {
    if(!outputFolder.exists())
      outputFolder.mkdir();
      
    BufferedWriter outE = new BufferedWriter(new FileWriter(outputFolder.toString() + sep + "energy"));
    BufferedWriter outL = new BufferedWriter(new FileWriter(outputFolder.toString() + sep + "life"));
    BufferedWriter outT = new BufferedWriter(new FileWriter(outputFolder.toString() + sep + "time"));
    
    Iterator<String> queryKeysIterator = sort(gData.keySet()).iterator();
    while(queryKeysIterator.hasNext())
    {
      String queryID = queryKeysIterator.next();
      String outputE = queryID + " ";
      String outputL = queryID + " ";
      String outputT = queryID + " ";
      adaptationStore gStore = gData.get(queryID);
      adaptationStore pStore = pData.get(queryID);
      Iterator<Double> gEnergyIterator = gStore.getEnergyCost().iterator();
      Iterator<Double> pEnergyIterator = pStore.getEnergyCost().iterator();
      Iterator<Double> gTimeIterator = gStore.getTimeCost().iterator();
      Iterator<Double> pTimeIterator = pStore.getTimeCost().iterator();
      Iterator<Double> gLifeIterator = gStore.getLifetime().iterator();
      Iterator<Double> pLifeIterator = pStore.getLifetime().iterator();
      
      Double energy = 0.0;
      while(gEnergyIterator.hasNext())
      {
        Double energyT = gEnergyIterator.next();
        if(energyT != null)
        energy += energyT;
        outputE = outputE.concat(energy.toString() + " ");
      }
      energy = 0.0;
      while(pEnergyIterator.hasNext())
      {
        Double energyT = pEnergyIterator.next();
        if(energyT != null)
          energy +=energyT;
        outputE = outputE.concat(energy.toString() + " ");
      }
      outE.write(outputE + "\n");
      
      Double time = 0.0;
      while(gTimeIterator.hasNext())
      {
        Double energyT = gTimeIterator.next();
        if(energyT != null)
          time += energyT;
        outputT = outputT.concat(time.toString() + " ");
      }
      time = 0.0;
      while(pTimeIterator.hasNext())
      {
        Double energyT = pTimeIterator.next();
        if(energyT != null)
          time += energyT;
        outputT = outputT.concat(time.toString() + " ");
      }
      outT.write(outputT + "\n");
      
      while(gLifeIterator.hasNext())
      {
        Double energyT = gLifeIterator.next();
        if(energyT != null)
        {
          Double life = energyT;
          outputL = outputL.concat(life.toString() + " ");
        }
      }
      while(pLifeIterator.hasNext())
      {
        Double energyT = pLifeIterator.next();
        if(energyT != null)
        {
          Double life = energyT;
          outputL = outputL.concat(life.toString() + " ");
        }
      }
      
      outL.write(outputL + "\n");
    }
    
    outE.flush();
    outE.close();
    outL.flush();
    outL.close();
    outT.flush();
    outT.close();
  }


  private static void addData(adaptationStore store, Integer adaptationID,
                              File adaptationFolder, ArrayList<Integer> points,
                              File queryFolder, HashMap<String, adaptationStore> data) 
  throws IOException
  {
      File dataFile = new File(adaptationFolder.toString() + sep +
          "Planner" + sep + "assessment" +sep +  
          "Adaptations" + sep + "adaptList");
      if(dataFile.exists())
      {
        BufferedReader in = new BufferedReader(new FileReader(dataFile));
        String line = in.readLine();
        String [] dataBits = line.split("Cost");
        dataBits[1]=  dataBits[1].replace("[", "");
        dataBits[1]= dataBits[1].split("j")[0].split(" ")[1];
        dataBits[2]=  dataBits[2].replace("[", "");
        dataBits[2]= dataBits[2].split("ms")[0].split(" ")[1];
        dataBits[3]= dataBits[3].split("Estimate")[1].split(" ms")[0].split(" ")[1].replace("[", "");
        store.setEnergy(points.indexOf(adaptationID), 
        Double.parseDouble(dataBits[1]));
        store.setTime(points.indexOf(adaptationID),
        Double.parseDouble(dataBits[2])/1000);
        store.setLife(points.indexOf(adaptationID),
        Double.parseDouble(dataBits[3]));
        data.put(queryFolder.getName(), store);
        in.close();
      }
  }
  
  private static ArrayList<String> sort(Set<String> list)
  {
    ArrayList<String> sorted = new ArrayList<String>();
    for(int index = 0; index < 90; index++)
    {
      sorted.add(null);
    }
    Iterator<String> keys = list.iterator();
    while(keys.hasNext())
    {
      String possibleAdpatationfolder = keys.next();
      String [] bits = possibleAdpatationfolder.split("query");
      sorted.set(Integer.parseInt(bits[1]), possibleAdpatationfolder);
    }
    for(int index = 0; index < 90; index++)
    {
      sorted.remove(null);
    }
    return sorted;
  }
}
