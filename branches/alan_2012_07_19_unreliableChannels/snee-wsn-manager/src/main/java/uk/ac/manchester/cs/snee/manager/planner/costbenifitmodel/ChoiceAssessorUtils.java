package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class ChoiceAssessorUtils extends RTUtils
{
   private HashMap<String, RunTimeSite> runningSites;
   
   public ChoiceAssessorUtils(HashMap<String, RunTimeSite> runningSites, RT rt)
   {
     super(rt);
     this.runningSites = runningSites;
   }
   
   public void exportRTWithEnergies(String fname, String label)
   { 
     DecimalFormat df = new DecimalFormat("#.#####");
   try {
       final PrintWriter out = new PrintWriter(new BufferedWriter(
         new FileWriter(fname)));
       String edgeSymbol;
     out.println("digraph \"" + this.rt.getID() + "\" {");
     edgeSymbol = "->";
       out.println("size = \"8.5,11\";"); // do not exceed size A4
       out.println("label = \"" + label + "\";");
       out.println("rankdir=\"BT\";");
       Iterator<Site> siteIter = 
         this.tree.nodeIterator(TraversalOrder.POST_ORDER);
       while (siteIter.hasNext()) {
       final Site currentSite = siteIter.next();
       out.print(currentSite.getID() + " [fontsize=20 ");
       if (currentSite.isSource()) {
         out.print("shape = doublecircle ");
       }
       out.print("label = \"" + currentSite.getID());
       out.print("\\n energy stock: " + df.format(runningSites.get(currentSite.getID()).getCurrentEnergy()) + "\\n ");
       out.print("QEP Cost: " + df.format(runningSites.get(currentSite.getID()).getQepExecutionCost()) + "\\n ");
       out.print("Adapt Cost: " + df.format(runningSites.get(currentSite.getID()).getCurrentAdaptationEnergyCost()) + "\\n ");
//         out.print(currentSite.getFragmentsString() + "\\n");
//         out.print(currentSite.getExchangeComponentsString());
       out.println("\"];");
       }
       //traverse the edges now
       siteIter = this.tree.nodeIterator(TraversalOrder.POST_ORDER);
       while (siteIter.hasNext()) {
       final Site currentSite = siteIter.next();
       if (currentSite.getOutputs().length==0)
         continue;
       Site parentSite = (Site) currentSite.getOutput(0);
       
       out.print("\"" + currentSite.getID() + "\""
           + edgeSymbol + "\""
           + parentSite.getID() + "\" ");

//       if (this.displayLinkProperties) {
//         //TODO: find a more elegant way of rounding a double to 2 decimal places
//         out.print("[label = \"radio loss =" 
//         + (Math.round(e.getRadioLossCost() * 100))
//         / 100 + "\\n ");
//       out.print("energy = " + e.getEnergyCost() + "\\n");
//       out.print("latency = " + e.getLatencyCost() + "\"");
//       } else {
           out.print("[");
//       }

       out.println("style = dashed]; ");
     }       out.println("}");
       out.close();

   }
   catch (Exception e) 
   {
   }
   }
  
}
