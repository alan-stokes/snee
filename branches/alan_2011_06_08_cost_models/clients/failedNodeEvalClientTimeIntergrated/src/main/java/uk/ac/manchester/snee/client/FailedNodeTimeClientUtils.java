package uk.ac.manchester.snee.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
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
  
  private void sortout(ArrayList<Adaptation> adaptations)
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

  public ArrayList<Adaptation> readInObjects(File inputFolder)
  {
    try 
    {  
      ArrayList<Adaptation> adapts = new ArrayList<Adaptation>();
      
      ObjectInputStream inputStream = null;
      File [] files = inputFolder.listFiles();
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
    File inputFolder = new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "OTASection" +  sep + "storedObjects");
    ArrayList<Adaptation> orginal = this.readInObjects(inputFolder);
    plot.addGlobalLifetime(orginal.get(0).getLifetimeEstimate());
    plot.addPartialLifetime(orginal.get(0).getLifetimeEstimate());
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

  public void storeAdaptation(int queryid, int testid, double currentLifetime, PlotterEnum which)
  {
    File inputFolder = new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + testid + sep + "Planner" +  sep + "storedObjects");
    ArrayList<Adaptation> adaptations = this.readInObjects(inputFolder);
    this.sortout(adaptations);
    if(which == PlotterEnum.GLOBAL)
    {
      plot.addGlobalLifetime(global.getLifetimeEstimate() + currentLifetime);
    }
    else if(which == PlotterEnum.PARTIAL)
    {
      plot.addPartialLifetime(partial.getLifetimeEstimate() + currentLifetime);
    }
    else if(which == PlotterEnum.LOCAL)
    {
      
    }
  }

  public void plotTopology() throws IOException
  {
    plot.writeLifetimes();
    
  }
  
  
}
