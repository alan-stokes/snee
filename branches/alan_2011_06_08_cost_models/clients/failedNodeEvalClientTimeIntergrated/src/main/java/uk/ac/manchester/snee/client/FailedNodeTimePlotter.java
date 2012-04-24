package uk.ac.manchester.snee.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;

public class FailedNodeTimePlotter implements Serializable
{

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2846057863098603796L;
  private static final Logger log = Logger.getLogger(FailedNodeTimePlotter.class.getName());
  private File outputFolder = null;
  private int queryID = 1;
  private String sep = System.getProperty("file.separator");
  private File lifetimePlotFile = null;
  private File energyPlotFile = null;
  private File timePlotFile = null;
  private File nodeFailures = null;
  private double lifetimeYMax = 0;
  private int numberOfAdaptations = 0;
  private BufferedWriter lifetimeWriter;
  private BufferedWriter failureWriter;
  private BufferedWriter energyWriter;
  private BufferedWriter timeWriter;
  private ArrayList<ArrayList<String>> failedNodesGlobal = new ArrayList<ArrayList<String>>();
  private ArrayList<ArrayList<String>> failedNodesPartial = new ArrayList<ArrayList<String>>();
  private ArrayList<ArrayList<String>> failedNodesLocal = new ArrayList<ArrayList<String>>();
  private ArrayList<ArrayList<String>> failedNodesBest = new ArrayList<ArrayList<String>>();
  private ArrayList<ArrayList<String>> failedNodesOrginial = new ArrayList<ArrayList<String>>();
  private ArrayList<Double> globalLifetimes = new ArrayList<Double>();
  private ArrayList<Double> partialLifetimes = new ArrayList<Double>();
  private ArrayList<Double> localLifetimes = new ArrayList<Double>();
  private ArrayList<Double> mixedLifetimes = new ArrayList<Double>();
  private ArrayList<Double> orgininalLifetimes = new ArrayList<Double>();
  private HashMapList<Integer, Long> globalTimes = new HashMapList<Integer, Long>();
  private HashMapList<Integer, Long> partialTimes = new HashMapList<Integer, Long>();
  private HashMapList<Integer, Long> localTimes = new HashMapList<Integer, Long>();
  private HashMapList<Integer, Long> mixedTimes = new HashMapList<Integer, Long>();
  private HashMapList<Integer, Double> globalenergy = new HashMapList<Integer, Double>();
  private HashMapList<Integer, Double> partialenergy = new HashMapList<Integer, Double>();
  private HashMapList<Integer, Double> localenergy = new HashMapList<Integer, Double>();
  private HashMapList<Integer, Double> mixedenergy = new HashMapList<Integer, Double>();
//  private ArrayList<Double> globalTupleProduction = new ArrayList<Double>();
 // private ArrayList<Double> partialTupleProduction = new ArrayList<Double>();
 // private ArrayList<Double> localTupleProduction = new ArrayList<Double>();
 // private ArrayList<Double> mixedTupleProduction = new ArrayList<Double>();

  
  
  public boolean addOrginialLifetime(Double e, ArrayList<String> nodesFailed)
  {
    failedNodesOrginial.add(nodesFailed);
    return orgininalLifetimes.add(e);
  }
  
  
  public boolean addGlobalLifetime(Double e, ArrayList<String> nodesFailed)
  {
    failedNodesGlobal.add(nodesFailed);
    return globalLifetimes.add(e);
  }
  
  public boolean addPartialLifetime(Double e, ArrayList<String> nodesFailed)
  {
    failedNodesPartial.add(nodesFailed);    
    return partialLifetimes.add(e);
  }
  
  public boolean addLocalLifetime(Double e, ArrayList<String> nodesFailed)
  {
    failedNodesLocal.add(nodesFailed);
    return localLifetimes.add(e);
  }
  
  public boolean addBestLifetime(Double e, ArrayList<String> nodesFailed)
  {
    failedNodesBest.add(nodesFailed);
    return mixedLifetimes.add(e);
  }
  
  
  
  public void writeLifetimes(int testID) throws IOException
  {
    Iterator<Double> globalLifetimeIterator = globalLifetimes.iterator();
    Iterator<Double> partialLifetimeIterator = partialLifetimes.iterator();
    Iterator<Double> localLifetimeIterator = localLifetimes.iterator();
    Iterator<Double> mixedLifetimeIterator = mixedLifetimes.iterator();
    Iterator<ArrayList<String>> globalFailureIterator = failedNodesGlobal.iterator();
    Iterator<ArrayList<String>> partialFailureIterator = failedNodesPartial.iterator();
    Iterator<ArrayList<String>> localFailureIterator = failedNodesLocal.iterator();
    Iterator<ArrayList<String>> mixedFailureIterator = failedNodesBest.iterator();
    
    
    
    DecimalFormat df = new DecimalFormat("#.#####");
    int counter = 1;
    lifetimeWriter.close();
    lifetimeWriter = new BufferedWriter(new FileWriter(lifetimePlotFile));
    failureWriter.close();
    failureWriter = new BufferedWriter(new FileWriter(nodeFailures));
    try
    {
      while(globalLifetimeIterator.hasNext() || partialLifetimeIterator.hasNext() || 
            localLifetimeIterator.hasNext() || mixedLifetimeIterator.hasNext())
      {
        Double globalLife;
        Double partialLife;
        Double localLife;
        Double mixedLife;
        
        ArrayList<String> globalFails = null;
        ArrayList<String> partialFails = null;
        ArrayList<String> localFails = null;
        ArrayList<String> bestFails = null;
        
        
        
        if(globalLifetimeIterator.hasNext())
          globalLife = globalLifetimeIterator.next();
        else
          globalLife = 0.0;
        if(partialLifetimeIterator.hasNext())
          partialLife = partialLifetimeIterator.next();
        else
          partialLife = 0.0;
        if(localLifetimeIterator.hasNext())
          localLife = localLifetimeIterator.next();
        else
          localLife = 0.0;
        if(mixedLifetimeIterator.hasNext())
          mixedLife = mixedLifetimeIterator.next();
        else
          mixedLife = 0.0;
        
        
        
        if(globalFailureIterator.hasNext())
          globalFails = globalFailureIterator.next();
        else
          globalFails = new ArrayList<String>();
        if(partialFailureIterator.hasNext())
          partialFails = partialFailureIterator.next();
        else
          partialFails = new ArrayList<String>();
        if(localFailureIterator.hasNext())
          localFails = localFailureIterator.next();
        else
          localFails = new ArrayList<String>();
        if(mixedLifetimeIterator.hasNext())
          bestFails = mixedFailureIterator.next();
        else
          bestFails = new ArrayList<String>();
        
        
        log.fatal(counter + " " + df.format(globalLife / 1000) + " " + df.format(partialLife/ 1000) + 
                  " " + df.format(localLife/ 1000) +  " " + df.format(mixedLife/ 1000));
        lifetimeWriter.write(counter + " " + df.format(globalLife / 1000) + " " + 
                             df.format(partialLife / 1000) + " " + df.format(localLife / 1000) +
                             " " + df.format(mixedLife / 1000));
        lifetimeWriter.newLine();
        lifetimeWriter.flush();
        lifetimeYMax = Math.max(lifetimeYMax, globalLife / 1000);
        lifetimeYMax = Math.max(lifetimeYMax, partialLife / 1000);
        lifetimeYMax = Math.max(lifetimeYMax, localLife / 1000);
        lifetimeYMax = Math.max(lifetimeYMax, mixedLife / 1000);
        
        failureWriter.write(counter + globalFails.toString() + "  " + partialFails.toString() + "  " +
                            localFails.toString() + "  " + bestFails.toString());
        failureWriter.newLine();
        failureWriter.flush();
        
        counter++;
      }
      lifetimeWriter.flush();
      lifetimeWriter.close();
      failureWriter.flush();
      failureWriter.close();
      
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
    }
    generatePlots(testID);
  }
  
  
  public FailedNodeTimePlotter (File outputFolder, AutonomicManagerImpl manager, int queryid) 
  throws IOException
  {
    this.outputFolder = outputFolder;
    this.outputFolder = new File("plots");
    if(this.outputFolder.exists())
    {
      manager.deleteFileContents(outputFolder);
      this.outputFolder.mkdir();
    }
    else
      this.outputFolder.mkdir();
    
    lifetimePlotFile = new File(this.outputFolder.toString() + sep + "query" + queryid + "lifetimePlot.tex");
    nodeFailures = new File(this.outputFolder.toString() + sep + "query" + queryid + "NodeFailures.tex");
    energyPlotFile = new File(this.outputFolder.toString() + sep + "query" + queryid + "energy");
    timePlotFile = new File(this.outputFolder.toString() + sep + "query" + queryid + "times");
    
    if(lifetimePlotFile.exists())
    {
      lifetimePlotFile.delete();
      lifetimePlotFile.createNewFile();
    }
    if(nodeFailures.exists())
    {
      nodeFailures.delete();
      nodeFailures.createNewFile();
    }
    if(energyPlotFile.exists())
    {
      energyPlotFile.delete();
      energyPlotFile.createNewFile();
    }
    if(timePlotFile.exists())
    {
      timePlotFile.delete();
      timePlotFile.createNewFile();
    }
    
    lifetimeWriter = new BufferedWriter(new FileWriter(lifetimePlotFile));
    failureWriter = new BufferedWriter(new FileWriter(nodeFailures));
    energyWriter = new BufferedWriter(new FileWriter(energyPlotFile));
    timeWriter = new BufferedWriter(new FileWriter(timePlotFile));
  }
  
  public FailedNodeTimePlotter (File outputFolder) 
  throws IOException
  {
    this.outputFolder = outputFolder;
    this.outputFolder = new File("plots");
    if(this.outputFolder.exists())
    {
      deleteFileContents(outputFolder);
      this.outputFolder.mkdir();
    }
    else
      this.outputFolder.mkdir();
    
    lifetimePlotFile = new File(this.outputFolder.toString() + sep + "query1" + "lifetimePlot.tex");
    nodeFailures = new File(this.outputFolder.toString() + sep + "query1" + "NodeFailures.tex");
    energyPlotFile = new File(this.outputFolder.toString() + sep + "query1" +  "energy");
    timePlotFile = new File(this.outputFolder.toString() + sep + "query1" +  "times");
    
    if(lifetimePlotFile.exists())
    {
      lifetimePlotFile.delete();
      lifetimePlotFile.createNewFile();
    }
    if(nodeFailures.exists())
    {
      nodeFailures.delete();
      nodeFailures.createNewFile();
    }
    if(energyPlotFile.exists())
    {
      energyPlotFile.delete();
      energyPlotFile.createNewFile();
    }
    if(timePlotFile.exists())
    {
      timePlotFile.delete();
      timePlotFile.createNewFile();
    }
    
    lifetimeWriter = new BufferedWriter(new FileWriter(lifetimePlotFile));
    failureWriter = new BufferedWriter(new FileWriter(nodeFailures));
    energyWriter = new BufferedWriter(new FileWriter(energyPlotFile));
    timeWriter = new BufferedWriter(new FileWriter(timePlotFile));
  }
  
  
  
  public void plot(Adaptation global, Adaptation partial, Adaptation local, double currentLifetime, int testID) 
  throws 
  IOException, OptimizationException
  {
    DecimalFormat df = new DecimalFormat("#.#####");
    lifetimeWriter.write(queryID +  "_(" + global.getNewQep().getAgendaIOT().getBufferingFactor() + ") ");
    if(global != null)
    {
      lifetimeWriter.write(df.format(global.getLifetimeEstimate() / 1000) + " ");
      lifetimeYMax = Math.max(lifetimeYMax, global.getLifetimeEstimate() / 1000);
    }
    if(partial != null)
    {
      lifetimeWriter.write(df.format(partial.getLifetimeEstimate() / 1000) + " ");
      lifetimeYMax = Math.max(lifetimeYMax, partial.getLifetimeEstimate() / 1000);
    }
    if(local != null)
    {
      lifetimeWriter.write(df.format((local.getLifetimeEstimate() / 1000) + currentLifetime) + " ");
      lifetimeYMax = Math.max(lifetimeYMax, (local.getLifetimeEstimate() / 1000) + currentLifetime);
    }
    
    lifetimeWriter.flush();
    
    generatePlots(testID);
  }
  
  private void generatePlots(int testID)
  {
    try
    {
      if(numberOfAdaptations == 3)
      {
        Runtime rt = Runtime.getRuntime();
        File bash = new File("src/main/resources/bashScript/plotGnuplot3");
        String location = bash.getAbsolutePath();
        String [] commandArg = new String[9];
        commandArg[0] = location;
        commandArg[1] = "node failures";
        commandArg[2] = "lifetime (1000 s)";
        commandArg[3] = new Integer(testID+1).toString();
        commandArg[4] = new Double(this.lifetimeYMax).toString();
        commandArg[5] = outputFolder.getAbsolutePath() + sep + "query" + queryID+ "lifetime";
        commandArg[6] = lifetimePlotFile.getAbsolutePath();
        commandArg[7] = "global";
        commandArg[8] = "partial";
        rt.exec(commandArg);
      }
      else
      {
        //lifetime
        Runtime rt = Runtime.getRuntime();
        File bash = new File("src/main/resources/bashScript/plotGnuplot2");
        String location = bash.getAbsolutePath();
        String [] commandArg = new String[10];
        commandArg[0] = location;
        commandArg[1] = "node failures";
        commandArg[2] = "lifetime (1000 s)";
        commandArg[3] = new Integer(testID+1).toString();
        commandArg[4] = new Double(this.lifetimeYMax).toString();
        commandArg[5] = outputFolder.getAbsolutePath() + sep + "query" + queryID+ "lifetime";
        commandArg[6] = lifetimePlotFile.getAbsolutePath();
        commandArg[7] = "global";
        commandArg[8] = "partial";
        commandArg[9] = "local";
        rt.exec(commandArg);
      }
      queryID ++;
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(0);
    }// TODO Auto-generated method stub
    
  }

  private static void deleteFileContents(File firstOutputFolder)
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

  public void plot(Adaptation global, Adaptation partial, Adaptation local) 
  throws IOException, OptimizationException
  {
    this.plot(global, partial, local, 0, 1);
  }

  public void endPlotLine() 
  throws IOException
  {
    lifetimeWriter.write("\n");  
    lifetimeWriter.flush();
  }

  public void newWriters(int queryid) throws IOException
  {
    lifetimePlotFile = new File(this.outputFolder.toString() + sep + "query" + queryid + "lifetimePlot.tex");
    nodeFailures = new File(this.outputFolder.toString() + sep + "query" + queryid + "NodeFailures.tex");
    energyPlotFile = new File(this.outputFolder.toString() + sep + "query1" +  "energy");
    timePlotFile = new File(this.outputFolder.toString() + sep + "query1" +  "times");
    
    
    if(lifetimePlotFile.exists())
    {
      lifetimePlotFile.delete();
      lifetimePlotFile.createNewFile();
    }
    
    if(nodeFailures.exists())
    {
      nodeFailures.delete();
      nodeFailures.createNewFile();
    }
    if(energyPlotFile.exists())
    {
      energyPlotFile.delete();
      energyPlotFile.createNewFile();
    }
    if(timePlotFile.exists())
    {
      timePlotFile.delete();
      timePlotFile.createNewFile();
    }
    
    lifetimeWriter = new BufferedWriter(new FileWriter(lifetimePlotFile));
    failureWriter = new BufferedWriter(new FileWriter(nodeFailures));
    energyWriter = new BufferedWriter(new FileWriter(energyPlotFile));
    timeWriter = new BufferedWriter(new FileWriter(timePlotFile));
   
    globalLifetimes.clear();
    partialLifetimes.clear();
    localLifetimes.clear();
    mixedLifetimes.clear();
    this.globalenergy.clear();
    this.partialenergy.clear();
    this.localenergy.clear();
    this.globalTimes.clear();
    this.partialTimes.clear();
    this.localTimes.clear();
  }


  /**
   * could be refractured to non repeated.
   * @param testID
   * @throws IOException
   */
  public void writeEnergiesAndTimes(int testID) throws IOException
  {
    Iterator<Integer> globalenergyKeyIterator = globalenergy.keySet().iterator();
    Iterator<Integer> partialenergyKeyIterator = partialenergy.keySet().iterator();
    Iterator<Integer> localenergyKeyIterator = localenergy.keySet().iterator();
    Iterator<Integer> mixedenergyKeyIterator = mixedenergy.keySet().iterator();
    Iterator<Integer> globalTimeKeyIterator = globalTimes.keySet().iterator();
    Iterator<Integer> partialTimeKeyIterator = partialTimes.keySet().iterator();
    Iterator<Integer> localTimeKeyIterator = localTimes.keySet().iterator();
    Iterator<Integer> mixedTimeKeyIterator = mixedTimes.keySet().iterator();
    
    
    energyWriter.close();
    energyWriter = new BufferedWriter(new FileWriter(energyPlotFile));
    timeWriter.close();
    timeWriter = new BufferedWriter(new FileWriter(timePlotFile));
    
    try
    {
      while(globalenergyKeyIterator.hasNext())
      {
        Integer globalKey = globalenergyKeyIterator.next();
        energyWriter.write(globalKey + " ");
        ArrayList<Double> energies = globalenergy.get(globalKey);
        Iterator<Double> energiesIterator = energies.iterator();
        while(energiesIterator.hasNext())
        {
          Double energy = energiesIterator.next();
          energyWriter.write(energy + " ");
        }
        energyWriter.newLine();
      }
      
      energyWriter.newLine();
      energyWriter.newLine();
      
      while(partialenergyKeyIterator.hasNext())
      {
        Integer partialKey = partialenergyKeyIterator.next();
        energyWriter.write(partialKey + " ");
        ArrayList<Double> energies = partialenergy.get(partialKey);
        Iterator<Double> energiesIterator = energies.iterator();
        while(energiesIterator.hasNext())
        {
          Double energy = energiesIterator.next();
          energyWriter.write(energy + " ");
        }
        energyWriter.newLine();
      }
      
      energyWriter.newLine();
      energyWriter.newLine();
      
      while(localenergyKeyIterator.hasNext())
      {
        Integer localKey = localenergyKeyIterator.next();
        energyWriter.write(localKey + " ");
        ArrayList<Double> energies = localenergy.get(localKey);
        Iterator<Double> energiesIterator = energies.iterator();
        while(energiesIterator.hasNext())
        {
          Double energy = energiesIterator.next();
          energyWriter.write(energy + " ");
        }
        energyWriter.newLine();
      }
      
      energyWriter.newLine();
      energyWriter.newLine();
      
      while(localenergyKeyIterator.hasNext())
      {
        Integer localKey = localenergyKeyIterator.next();
        energyWriter.write(localKey + " ");
        ArrayList<Double> energies = localenergy.get(localKey);
        Iterator<Double> energiesIterator = energies.iterator();
        while(energiesIterator.hasNext())
        {
          Double energy = energiesIterator.next();
          energyWriter.write(energy + " ");
        }
        energyWriter.newLine();
      }
      
      energyWriter.newLine();
      energyWriter.newLine();
      
      while(mixedenergyKeyIterator.hasNext())
      {
        Integer mixedKey = mixedenergyKeyIterator.next();
        energyWriter.write(mixedKey + " ");
        ArrayList<Double> energies = mixedenergy.get(mixedKey);
        Iterator<Double> energiesIterator = energies.iterator();
        while(energiesIterator.hasNext())
        {
          Double energy = energiesIterator.next();
          energyWriter.write(energy + " ");
        }
        energyWriter.newLine();
      }
      energyWriter.flush();
      energyWriter.close();
      
      //times
      
      while(globalTimeKeyIterator.hasNext())
      {
        Integer globalKey = globalTimeKeyIterator.next();
        timeWriter.write(globalKey + " ");
        ArrayList<Long> times = globalTimes.get(globalKey);
        Iterator<Long> timesIterator = times.iterator();
        while(timesIterator.hasNext())
        {
          Long time = timesIterator.next();
          timeWriter.write(time + " ");
        }
        timeWriter.newLine();
      }
      
      timeWriter.newLine();
      timeWriter.newLine();
      
      while(partialTimeKeyIterator.hasNext())
      {
        Integer partialKey = partialTimeKeyIterator.next();
        timeWriter.write(partialKey + " ");
        ArrayList<Long> times = partialTimes.get(partialKey);
        Iterator<Long> timesIterator = times.iterator();
        while(timesIterator.hasNext())
        {
          Long time = timesIterator.next();
          timeWriter.write(time + " ");
        }
        timeWriter.newLine();
      }
      
      timeWriter.newLine();
      timeWriter.newLine();
      
      while(localTimeKeyIterator.hasNext())
      {
        Integer localKey = localTimeKeyIterator.next();
        timeWriter.write(localKey + " ");
        ArrayList<Long> times = localTimes.get(localKey);
        Iterator<Long> timesIterator = times.iterator();
        while(timesIterator.hasNext())
        {
          Long time = timesIterator.next();
          timeWriter.write(time + " ");
        }
        timeWriter.newLine();
      }
      
      timeWriter.newLine();
      timeWriter.newLine();
      
      while(localTimeKeyIterator.hasNext())
      {
        Integer localKey = localTimeKeyIterator.next();
        timeWriter.write(localKey + " ");
        ArrayList<Long> times = localTimes.get(localKey);
        Iterator<Long> timesIterator = times.iterator();
        while(timesIterator.hasNext())
        {
          Long time = timesIterator.next();
          timeWriter.write(time + " ");
        }
        timeWriter.newLine();
      }
      
      timeWriter.newLine();
      timeWriter.newLine();
      
      while(mixedTimeKeyIterator.hasNext())
      {
        Integer mixedKey = mixedTimeKeyIterator.next();
        timeWriter.write(mixedKey + " ");
        ArrayList<Long> times = mixedTimes.get(mixedKey);
        Iterator<Long> timesIterator = times.iterator();
        while(timesIterator.hasNext())
        {
          Long time = timesIterator.next();
          timeWriter.write(time + " ");
        }
        timeWriter.newLine();
      }
      timeWriter.flush();
      timeWriter.close();
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
    }
    generatePlots(testID); 
    
  }


  public void addGlobalEnergy(Double energyCost, int failedID)
  {
    this.globalenergy.add(failedID, energyCost);
  }


  public void addGlobalTime(Long timeCost, int failedID)
  {
   this.globalTimes.add(failedID, timeCost);
  }


  public void addPartialEnergy(Double energyCost, int failedID)
  {
    this.partialenergy.add(failedID, energyCost);
  }


  public void addPartialTime(Long timeCost, int failedID)
  {
    this.partialTimes.add(failedID, timeCost);
  }


  public void addLocalEnergy(Double energyCost, int failedID)
  {
    this.localenergy.add(failedID, energyCost);
  }


  public void addLocalTime(Long timeCost, int failedID)
  {
    this.localTimes.add(failedID, timeCost);
  }


  public void addBestEnergy(Double energyCost, int failedID)
  {
    this.mixedenergy.add(failedID, energyCost);
  }


  public void addBestTime(Long timeCost, int failedID)
  {
    this.mixedTimes.add(failedID, timeCost);
  }
  
}
