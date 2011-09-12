package uk.ac.manchester.cs.snee.manager.planner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;

public class PlannerUtils
{
   private List<Adaptation> adaptations;
   private File outputFolder = null;
   private String sep = System.getProperty("file.separator");
    
   public PlannerUtils(List<Adaptation> adaptations)
   {
     this.adaptations = adaptations;
   }
   
   public void printLatexDocument(Adaptation best, String id) throws IOException
   { 
     try
     {
       makeFolder(id);
       generateLatex(id, best); 
     }
     catch(Exception e)
     {
       System.out.println(e.getMessage());
       e.printStackTrace();
     }
   }

  private void generateLatex(String id, Adaptation best) throws IOException
  {
    Iterator<Adaptation> adIterator = adaptations.iterator();
    BufferedWriter out = new BufferedWriter(new FileWriter(outputFolder));
    out.write("\\begin{table} \n \\begin{tabular}{|p{4cm}|p{2.5cm}|p{2.5cm}|p{2.5cm}|p{4cm}|p{5cm}|} \n \\hline \n");
    out.write("Adaptation ID & AD Time & AD energy & QEP Cost & lifetime & changes \\\\ \n \\hline \n");
    DecimalFormat df = new DecimalFormat("#.#####");

    while(adIterator.hasNext())
    {
      Adaptation ad = adIterator.next();
      out.write(ad.getOverallID() + " & " + df.format(ad.getTimeCost()) + "ms & " + 
                df.format(ad.getEnergyCost()) + "J & " + df.format(ad.getRuntimeCost()) + "J & " + 
                df.format((ad.getLifetimeEstimate() /1000/60))  + "m & " +
                "rep " + ad.getReprogrammingSites().toString() + " \\newline " + 
                "Red " + ad.getRedirectedionSites().toString() + " \\newline " +
                "dea " + ad.getDeactivationSites().toString() + " \\newline " +
                "act " + ad.getActivateSites().toString() + " \\newline " + 
                "tem " + ad.getTemporalSites().toString() + " \\\\ \n \\hline \n");
    }
    out.write("\\end{tabular} \n \\caption{query " + id + " with faled nodes " + 
              best.getFailedNodes().toString() + " \\newline Best Adaptation is the " +
              best.getOverallID() + "} \n");
    out.write("\\end{table} \n\n\n");
    out.flush();
    moveImagesToLatexFolder(id, out);
  }

  private void moveImagesToLatexFolder(String id, BufferedWriter out)
  throws IOException
  {
    Iterator<Adaptation> adaptationIterator = adaptations.iterator();
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
        "]{\\includegraphics[scale=0.25]{" + "." + sep + id + sep + "orginalRT"+ "}} \n");
    
    out.write("\\end{figure} \n\n\n");
    out.flush();
    out.close();
  }

  private void makeFolder(String id)
  {
    outputFolder = new File("LatexSections");
    if(!outputFolder.exists())
    {
      outputFolder.mkdir();
    }
    outputFolder = new File(outputFolder.toString() + sep + id + ".tex");
  }
}
