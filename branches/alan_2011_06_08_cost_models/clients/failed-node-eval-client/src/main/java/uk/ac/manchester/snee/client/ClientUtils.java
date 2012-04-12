package uk.ac.manchester.snee.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
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

public class ClientUtils
{
  private static String sep = System.getProperty("file.separator");
  private BufferedWriter latexCore = null;
  private Adaptation global = null;
  private Adaptation partial = null;
  private Adaptation local = null;
  private Adaptation original = null;
  private Plotter plot = null;
  
  public ClientUtils()
  {
    try
    {
      plot = new Plotter(new File("plots"));
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
      if(ad != null)
      {
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
    }
    String id = queryid + "-" + testID;
    out.write("\\end{tabular} \n \\caption{query " + id + " with failed nodes " + 
              ad.getFailedNodes().toString() + "} \n");
    out.write("\\end{table} \n\n\n");
    
    out.flush();
    moveImagesToLatexFolder(out, adaptations, testID, queryid);
    sortout(adaptations);
    plot.plot(global, partial, local);
    global = null;
    partial = null;
    local = null;
  }
  
  private void sortout(ArrayList<Adaptation> adaptations)
  {
    Iterator<Adaptation> adaptIterator = adaptations.iterator();
    while(adaptIterator.hasNext())
    {
      Adaptation ad = adaptIterator.next();
      if(ad != null)
      {
      if(ad.getOverallID().contains("Global"))
        global = ad;
      else if(ad.getOverallID().contains("Partial"))
        partial = ad;
      else if(ad.getOverallID().contains("Local"))
        local = ad;
      else if(ad.getOverallID().contains("Orginal"))
      {original = ad;}
      else
        throw new NoSuchElementException("there is no overall id");
      }
    } 
  }

  public ArrayList<Adaptation> readInObjects(File inputFolder)
  {
    System.out.println("reading in adapation objects");
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
      System.out.println("finished reading objects");
      this.sortout(adapts);
      adapts.clear();
      adapts.add(this.original);
      if(local != null)
        adapts.add(local);
      if(partial != null)
        adapts.add(partial);
      if(global != null)
        adapts.add(global);
      return adapts;
    }
    catch(Exception e)
    {
      System.out.println("failure in reading in objects. error is " + e.getMessage());
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
      if(adapt != null)
      {
      new RTUtils(adapt.getNewQep().getRT()).exportAsDotFile(outputImageFolder.toString() + sep + 
                                                             adapt.getOverallID() + "RT", false);
      out.write("\\subfloat[] [" + adapt.getOverallID() + "RT"  + 
                "]{\\includegraphics[scale=0.35]{" + "." + sep + id + sep +
                adapt.getOverallID() + "RT"+ "}} \n \\hspace{20pt}");
      out.flush();
      }
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
}
