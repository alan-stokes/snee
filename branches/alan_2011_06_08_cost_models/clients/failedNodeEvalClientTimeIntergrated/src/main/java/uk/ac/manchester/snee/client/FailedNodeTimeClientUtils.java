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
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyUtils;

public class FailedNodeTimeClientUtils
{
  private static String sep = System.getProperty("file.separator");
  private BufferedWriter latexCore = null;
  private Adaptation global = null;
  private Adaptation partial = null;
  private Adaptation local = null;
  //private Adaptation original = null;
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
  
  public void writeLatexCore() throws IOException
  {
    File latexCoreF = new File("LatexSections");
    if(!latexCoreF.exists())
    {
      latexCoreF.mkdir();
    }
    else
    {
      deleteFileContents(latexCoreF);
    }
    latexCoreF = new File(latexCoreF.toString() + sep + "core.tex");
    latexCore = new BufferedWriter(new FileWriter(latexCoreF));
    latexCore.write("\\documentclass[landscape, 10pt]{report} \n\\usepackage[landscape]{geometry} \n " +
                    "\\usepackage{graphicx} \n \\usepackage{subfig} \n \\usepackage[cm]{fullpage} \n" + 
                    "\\begin{document}  \n");
    latexCore.flush();  
  }
  
  public void updateLatexCore(int queryid, int testID) 
  throws IOException, OptimizationException, SchemaMetadataException
  {
    latexCore.write("\\input{query" + queryid + "-" + testID + "} \n \\clearpage \n");
    latexCore.flush();  
    File inputFolder = new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
                                testID + sep + "Planner" + sep + "storedObjects");
    writeLatex(inputFolder, queryid, testID);
  }
  
  public void writeLatex(File inputFolder, int queryid, int testID) 
  throws IOException, OptimizationException, SchemaMetadataException
  {
    ArrayList<Adaptation> adaptations = readInObjects(inputFolder);
    Iterator<Adaptation> adIterator = adaptations.iterator();
    BufferedWriter out = new BufferedWriter(new FileWriter("LatexSections" + sep + "query" + queryid + "-" + testID + ".tex", false));
    out.write("\\begin{table} \n \\renewcommand\\thetable{} \n \\begin{tabular}{|p{3.4cm}" +
        "|p{2.2cm}|p{2.2cm}|p{2.2cm}|p{2.7cm}|p{2.5cm}|p{2.5cm}|p{1.5cm}|p{3cm}|} \n \\hline \n");
    out.write("Adaptation ID & AD Time & AD energy & QEP Cost & lifetime (node) & cycles burned " +
        "\\& \\newline missed & cycles left & tuples left \\& \\newline missed & changes \\\\ \n " +
        "\\hline \n");
    DecimalFormat df = new DecimalFormat("#.#####");
    Long agendaTime = new Long(0);
    Adaptation ad = null;
    
    while(adIterator.hasNext())
    {
      ad = adIterator.next();
      agendaTime = ad.getNewQep().getAgendaIOT().getLength_bms(false);
      CardinalityEstimatedCostModel tupleModel = new CardinalityEstimatedCostModel(ad.getNewQep());
      tupleModel.runModel();
      out.write(ad.getOverallID() + " & " + df.format(ad.getTimeCost() /1000/60) + "m & " + 
                df.format(ad.getEnergyCost()) + "J & " + df.format(ad.getRuntimeCost()) + "J & " + 
                df.format((ad.getLifetimeEstimate() /1000/60))  + "m ( " + 
                ad.getNodeIdWhichEndsQuery() + " ) & " +
                df.format(ad.getEnergyCost() / ad.getRuntimeCost()) + "cycles \\newline \\newline " +
                df.format(ad.getTimeCost() / agendaTime) + "cycles & " +
                df.format(ad.getLifetimeEstimate() / agendaTime) + "cycles & " +
                df.format(tupleModel.returnAgendaExecutionResult() * (ad.getLifetimeEstimate() / agendaTime)) 
                + " tuples  \\newline \\newline " + 
                df.format((ad.getTimeCost() / agendaTime) * tupleModel.returnAgendaExecutionResult()) + 
                " tuples & " +
                "rep " + ad.getReprogrammingSites().toString() + " \\newline " + 
                "Red " + ad.getRedirectedionSites().toString() + " \\newline " +
                "dea " + ad.getDeactivationSites().toString() + " \\newline " +
                "act " + ad.getActivateSites().toString() + " \\newline " + 
                "tem " + ad.getTemporalSites().toString() + " \\\\ \n \\hline \n");
    }
    String id = queryid + "-" + testID;
    out.write("\\end{tabular} \n \\caption{query " + id + " with failed nodes " + 
              ad.getFailedNodes().toString() + "} \n");
    out.write("\\end{table} \n\n\n");
    
    out.flush();
    moveImagesToLatexFolder(out, adaptations, testID, queryid);
  }
  
  public void plotAdaptations(int queryid, int testID, double currentLifetime, PlotterEnum whichToPlot) 
  throws IOException, OptimizationException
  {
    File inputFolder = new File("output" + sep + "query" + queryid + sep + "AutonomicManData" + sep + "Adaption" + 
        testID + sep + "Planner" + sep + "storedObjects");
    ArrayList<Adaptation> adaptations = readInObjects(inputFolder);
    sortout(adaptations);
    if(whichToPlot.toString().equals(PlotterEnum.ALL.toString()))
    {
      plot.plot(global, partial, local, currentLifetime);
      plot.endPlotLine();
    }
    else if(whichToPlot.toString().equals(PlotterEnum.GLOBAL.toString()))
    {
      plot.plot(global, null, null, currentLifetime);
    }
    else if(whichToPlot.toString().equals(PlotterEnum.PARTIAL.toString()))
    {
      plot.plot(null, partial, null, currentLifetime);
    }
    else if(whichToPlot.toString().equals(PlotterEnum.LOCAL.toString()))
    {
      plot.plot(null, null, local, currentLifetime);
    }
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
  
  private void moveImagesToLatexFolder(BufferedWriter out, ArrayList<Adaptation> adaptations, 
                                       int testID, int queryID)
  throws IOException, SchemaMetadataException
  {
    Iterator<Adaptation> adaptationIterator = adaptations.iterator();
    String id = queryID + "-" + testID;
    File outputImageFolder = new File("LatexSections" + sep + id);
    outputImageFolder.mkdir();
    out.write("\\begin{figure}[hp] \n ");
    
    
    Adaptation adapt = null;
    while(adaptationIterator.hasNext())
    {
      adapt = adaptationIterator.next();
      new RTUtils(adapt.getNewQep().getRT()).exportAsDotFile(outputImageFolder.toString() + sep + 
                                                             adapt.getOverallID() + "RT", false);
      out.write("\\subfloat[] [" + adapt.getOverallID() + "RT"  + 
                "]{\\includegraphics[scale=0.35]{" + "." + sep + id + sep +
                adapt.getOverallID() + "RT"+ "}} \n \\hspace{20pt}");
      out.flush();
    }
    
    new TopologyUtils(adapt.getNewQep().getRT().getNetwork()).exportAsDOTFile(outputImageFolder.toString() + sep + "topology", "topology", true);
    out.write("\\subfloat[] [Topology"  + 
        "]{\\includegraphics[scale=0.25]{" + "." + sep + id + sep + "topology"+ "}} \n");
    
    out.write("\\end{figure} \n\n\n");
    out.flush();
    out.close();
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
    plot.plot(orginal.get(0), orginal.get(0), null);
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

  public void endPlotLine() 
  throws IOException
  {
    plot.endPlotLine();
  }
  
  
}
