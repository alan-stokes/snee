package uk.ac.manchester.cs.snee.manager.planner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyUtils;

public class PlannerUtils
{
   private List<Adaptation> adaptations;
   private File outputFolder = null;
   private String sep = System.getProperty("file.separator");
   private AutonomicManagerImpl manager;
    
   public PlannerUtils(List<Adaptation> adaptations, AutonomicManagerImpl manager)
   {
     this.adaptations = adaptations;
     this.manager = manager;
   }
   
   public void printLatexDocument(Adaptation orginal, Adaptation best, boolean append) throws IOException
   { 
     try
     {
       makeFolder();
       generateLatex(orginal, best, append); 
     }
     catch(Exception e)
     {
       System.out.println(e.getMessage());
       e.printStackTrace();
     }
   }

  private void generateLatex(Adaptation orginal, Adaptation best, boolean append)
  throws 
  IOException, OptimizationException, SchemaMetadataException
  {
    Iterator<Adaptation> adIterator = adaptations.iterator();
    BufferedWriter out = new BufferedWriter(new FileWriter(outputFolder, append));
    out.write("\\begin{table} \n \\renewcommand\\thetable{} \n \\begin{tabular}{|p{3.4cm}|p{2.2cm}|p{2.2cm}|p{2.2cm}|p{2.7cm}|p{2.5cm}|p{2.5cm}|p{1.5cm}|p{3cm}|} \n \\hline \n");
    out.write("Adaptation ID & AD Time & AD energy & QEP Cost & lifetime (node) & cycles burned & cycles left & tuples & changes \\\\ \n \\hline \n");
    DecimalFormat df = new DecimalFormat("#.#####");
    Long agendaTime = best.getNewQep().getAgendaIOT().getLength_bms(false);

    while(adIterator.hasNext())
    {
      Adaptation ad = adIterator.next();
      CardinalityEstimatedCostModel tupleModel = new CardinalityEstimatedCostModel(ad.getNewQep());
      tupleModel.runModel();
      out.write(ad.getOverallID() + " & " + df.format(ad.getTimeCost()) + "ms & " + 
                df.format(ad.getEnergyCost()) + "J & " + df.format(ad.getRuntimeCost()) + "J & " + 
                df.format((ad.getLifetimeEstimate() /1000/60))  + "m ( " + 
                ad.getNodeIdWhichEndsQuery() + " ) & " +
                df.format(ad.getEnergyCost() / ad.getRuntimeCost()) + "cycles & " +
                df.format(ad.getLifetimeEstimate() / agendaTime) + "cycles & " +
                df.format(tupleModel.returnAgendaExecutionResult() * 
                          (ad.getLifetimeEstimate() / agendaTime)) + " tuples & " +
                "rep " + ad.getReprogrammingSites().toString() + " \\newline " + 
                "Red " + ad.getRedirectedionSites().toString() + " \\newline " +
                "dea " + ad.getDeactivationSites().toString() + " \\newline " +
                "act " + ad.getActivateSites().toString() + " \\newline " + 
                "tem " + ad.getTemporalSites().toString() + " \\\\ \n \\hline \n");
    }
    CardinalityEstimatedCostModel tupleModel = new CardinalityEstimatedCostModel(orginal.getNewQep());
    tupleModel.runModel();
    out.write("Orginal & 0ms  & 0 J & " + df.format(orginal.getRuntimeCost()) + "J & " +
              df.format((orginal.getLifetimeEstimate() /1000/60))  + "m ( " + 
              orginal.getNodeIdWhichEndsQuery() + " ) & " +
              df.format(orginal.getEnergyCost() / orginal.getRuntimeCost()) + "cycles & " +
              df.format(orginal.getLifetimeEstimate() / agendaTime) + "cycles & " +
              df.format(tupleModel.returnAgendaExecutionResult() * 
              (orginal.getLifetimeEstimate() / agendaTime)) + " tuples & \\\\ \n \\hline \n");
    
    String id = manager.getQueryID() + "-" + manager.getAdaptionCount();
    out.write("\\end{tabular} \n \\caption{query " + id + " with faled nodes " + 
              best.getFailedNodes().toString() + " \\newline Best Adaptation is the " +
              best.getOverallID() + "} \n");
    out.write("\\end{table} \n\n\n");
    out.flush();
    moveImagesToLatexFolder(out);
  }

  private void moveImagesToLatexFolder(BufferedWriter out)
  throws IOException, SchemaMetadataException
  {
    Iterator<Adaptation> adaptationIterator = adaptations.iterator();
    String id = manager.getQueryID() + "-" + manager.getAdaptionCount();
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
    
    new RTUtils(adapt.getOldQep().getRT()).exportAsDotFile(outputImageFolder.toString() + sep + 
        "orginalRT", false);
    out.write("\\subfloat[] [OrginalRT"  + 
        "]{\\includegraphics[scale=0.25]{" + "." + sep + id + sep + "orginalRT"+ "}} \\hspace{20pt} \n");
    
    new TopologyUtils(adapt.getNewQep().getRT().getNetwork()).exportAsDOTFile(outputImageFolder.toString() + sep + "topology", "topology", true);
    out.write("\\subfloat[] [Topology"  + 
        "]{\\includegraphics[scale=0.25]{" + "." + sep + id + sep + "topology"+ "}} \n");
    
    out.write("\\end{figure} \n\n\n");
    out.flush();
    out.close();
  }

  private void makeFolder()
  {
    outputFolder = new File("LatexSections");
    if(!outputFolder.exists())
    {
      outputFolder.mkdir();
    }
    String id = manager.getQueryID() + "-" + manager.getAdaptionCount();
    outputFolder = new File(outputFolder.toString() + sep + id + ".tex");
  }
}
