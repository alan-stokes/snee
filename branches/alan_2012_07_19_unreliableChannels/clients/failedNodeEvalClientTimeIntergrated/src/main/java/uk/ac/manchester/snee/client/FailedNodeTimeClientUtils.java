package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;

public class FailedNodeTimeClientUtils
{
  private static String sep = System.getProperty("file.separator");
  private Adaptation global = null;
  private Adaptation partial = null;
  private Adaptation local = null;
  private Adaptation best = null;
  
  private FailedNodeTimePlotter plot = null;
  
  public FailedNodeTimeClientUtils()
  {
    try
    {
      plot = new FailedNodeTimePlotter(new File("plots"));
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void newPlotters(int queryID) throws IOException
  {
    plot.newWriters(queryID);
  }
  
  private void sortout(ArrayList<Adaptation> adaptations, boolean bestAd)
  {
    
    if(bestAd)
    {
      Iterator<Adaptation> adaptIterator = adaptations.iterator();
      while(adaptIterator.hasNext())
      {
        Adaptation ad = adaptIterator.next();
        best = ad;
      }
    }
    else
    {
      Iterator<Adaptation> adaptIterator = adaptations.iterator();
      while(adaptIterator.hasNext())
      {
        Adaptation ad = adaptIterator.next();
        if(ad.getOverallID().contains("Global"))
          global = ad;
        else if(ad.getOverallID().contains("Partial"))
          partial = ad;
        else if(ad.getOverallID().contains("Local"))
          local = ad;
        else if(ad.getOverallID().contains("Orginal"))
        {/*original = ad;*/}
        else
          throw new NoSuchElementException("there is no overall id");
      } 
    }
  }

  public ArrayList<Adaptation> readInObjects(File inputFolder)
  {
    try 
    {  
      ArrayList<Adaptation> adapts = new ArrayList<Adaptation>();
      
      ObjectInputStream inputStream = null;
      File [] files = inputFolder.listFiles();
      if(files != null)
      {
        for(int fileIndex = 0; fileIndex < files.length; fileIndex++)
        {
          //Construct the ObjectInputStream object
          inputStream = new ObjectInputStream(new FileInputStream(files[fileIndex]));
            
          Object obj = null;
            
          obj = inputStream.readObject();
          if (obj instanceof Adaptation) 
          {
            Adaptation ad = (Adaptation) obj;
            adapts.add(ad);
          }     
        }
      }
      return adapts;
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(0);
      return null;
    }
  }
  
  public void updateRecoveryFile(int queryid) throws IOException
  {
    File folder = new File("recovery"); 
    String path = folder.getAbsolutePath();
    //added to allow recovery from crash
    BufferedWriter recoverWriter = new BufferedWriter(new FileWriter(new File(path + "/recovery.tex")));
    
    recoverWriter.write(queryid + "\n");
    recoverWriter.flush();
    recoverWriter.close();
  }
  
  public void checkRecoveryFile(int queryid) throws IOException
  {
    //added to allow recovery from crash
    File folder = new File("recovery"); 
    String path = folder.getAbsolutePath();
    File recoveryFile = new File(path + "/recovery.tex");
    if(recoveryFile.exists())
    {
      BufferedReader recoveryTest = new BufferedReader(new FileReader(recoveryFile));
      String recoveryQueryIdLine = recoveryTest.readLine();
      String recoverQueryTestLine = recoveryTest.readLine();
      System.out.println("recovery text located with query test value = " +  recoveryQueryIdLine + " and has test no = " + recoverQueryTestLine);
      queryid = Integer.parseInt(recoveryQueryIdLine);
      if(queryid == 0)
      {
        deleteAllFilesInResultsFolder(folder);
        recoveryFile.createNewFile();
      }
    }
    else
    {
      System.out.println("create file recovery.tex with 2 lines each containing the number 0");
      folder.mkdir();    
    }
  }

  private void deleteAllFilesInResultsFolder(File folder)
  {
    File [] filesInFolder = folder.listFiles();
    for(int fileIndex = 0; fileIndex < filesInFolder.length; fileIndex++)
    {
      filesInFolder[fileIndex].delete();
    }
  }
  
  public void writeErrorToFile(Exception e, BufferedWriter failedOutput, int queryid,
                                      int testNo) throws IOException
  {
    System.out.println("tests failed for query " + queryid + "  going onto query " + (queryid + 1));
    failedOutput.write(queryid + " | " + testNo + "          |         " + e.getMessage() + "\n\n" );
    StackTraceElement[] trace = e.getStackTrace();
    for(int index = 0; index < trace.length; index++)
    {
      failedOutput.write(trace[index].toString() + "\n");
    }
    failedOutput.write("\n\n");
    failedOutput.flush();
  }
  
  public BufferedWriter createFailedTestListWriter() throws IOException
  {
    File folder = new File("recovery"); 
    File file = new File(folder + sep + "failedTests");
    return new BufferedWriter(new FileWriter(file));
  }

  public void plotOrginial(int queryid, int testNo) throws IOException, OptimizationException
  {
   // File inputFolder = new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "OTASection" +  sep + "storedObjects");
   // ArrayList<Adaptation> orginal = this.readInObjects(inputFolder);
    //DecimalFormat df = new DecimalFormat("#.#####");
   // System.out.println(df.format(orginal.get(0).getLifetimeEstimate()));
   // plot.addGlobalLifetime(orginal.get(0).getLifetimeEstimate(), new ArrayList<String>());
   // plot.addPartialLifetime(orginal.get(0).getLifetimeEstimate(), new ArrayList<String>());
   // plot.addLocalLifetime(orginal.get(0).getLifetimeEstimate(), new ArrayList<String>());
   // plot.addBestLifetime(orginal.get(0).getLifetimeEstimate(), new ArrayList<String>());
    
  }

  public Adaptation getGlobal()
  {
    return global;
  }
  
  public Adaptation getPartial()
  {
    return partial;
  }
  
  public Adaptation getLocal()
  {
    return local;
  }

  public void storeAdaptation(int queryid, int testid, double currentLifetime, PlotterEnum which, ArrayList<String> fails)
  {
    File inputFolder = new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + testid + sep + "Planner" +  sep + "storedObjects");
    ArrayList<Adaptation> adaptations = this.readInObjects(inputFolder);
    if(which == PlotterEnum.ALL)
      this.sortout(adaptations, true);
    else
      this.sortout(adaptations, false);
    
    DecimalFormat df = new DecimalFormat("#.#####");
    if(which == PlotterEnum.GLOBAL && global != null)
    {
      Double overallLifetime = new Double(global.getLifetimeEstimate().doubleValue() + currentLifetime);
      System.out.println(df.format( overallLifetime));
      plot.addGlobalLifetime(global.getLifetimeEstimate() + currentLifetime, fails);
    }
    else if(which == PlotterEnum.PARTIAL && partial != null)
    {
      Double overallLifetime = new Double(partial.getLifetimeEstimate().doubleValue() + currentLifetime);
      System.out.println(df.format(overallLifetime ));
      plot.addPartialLifetime(partial.getLifetimeEstimate() + currentLifetime, fails);
    }
    else if(which == PlotterEnum.LOCAL & local != null)
    {
      Double overallLifetime = new Double(local.getLifetimeEstimate().doubleValue() + currentLifetime);
      System.out.println(df.format(overallLifetime / 1000));
      plot.addLocalLifetime(local.getLifetimeEstimate() + currentLifetime, fails);
    }
    else if(which == PlotterEnum.ALL & best != null)
    {
      Double overallLifetime = new Double(best.getLifetimeEstimate().doubleValue() + currentLifetime);
      System.out.println(df.format(overallLifetime / 1000));
      plot.addBestLifetime(best.getLifetimeEstimate() + currentLifetime, fails);
    }
  }

  public void plotTopology(int testID) throws IOException
  {
    plot.writeLifetimes(testID);
  }
}
