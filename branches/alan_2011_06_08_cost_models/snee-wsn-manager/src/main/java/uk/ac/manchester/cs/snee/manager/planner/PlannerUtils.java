package uk.ac.manchester.cs.snee.manager.planner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
     makeFolder(id);
     generateLatex(id, best); 
   }

  private void generateLatex(String id, Adaptation best) throws IOException
  {
    Iterator<Adaptation> adIterator = adaptations.iterator();
    BufferedWriter out = new BufferedWriter(new FileWriter(outputFolder));
    out.write("\\begin{table} \n \\begin{tabular}{|p{4cm}|p{3cm}|p{4cm}|p{4cm}|p{5cm}|} \n \\hline \n");
    out.write("Adaptation ID & AD Time & AD energy & lifetime & changes \\\\ \n");
    while(adIterator.hasNext())
    {
      Adaptation ad = adIterator.next();
      out.write(ad.getOverallID() + " & " + ad.getTimeCost() + "ms & " + 
                ad.getEnergyCost() + "J & " + (ad.getLifetimeEstimate() /1000/60)  + "m & " +
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
