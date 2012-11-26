package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.LogicalOverlayNetworkHierarchy;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class ChannelModelUtils
{
  private ArrayList<ChannelModelSite> modelSites;
  private LogicalOverlayNetworkHierarchy logicaloverlayNetwork;
  private RT routingTree;
  private String sep = System.getProperty("file.separator");
  
  public ChannelModelUtils(ArrayList<ChannelModelSite> modelSites, 
                           LogicalOverlayNetworkHierarchy logicaloverlayNetwork)
  {
    this.modelSites = modelSites;
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    this.routingTree = logicaloverlayNetwork.getQep().getRT();
  }
  
  public void plotPacketRates(int iteration, File superFolder)
  {
    try
    {
      String edgeSymbol = "->";
      //create writer
      PrintWriter out = new PrintWriter(new BufferedWriter( new FileWriter(superFolder + sep + "iteration" + iteration)));
      //create blurb
      out.println("digraph \"" + (String) routingTree.getID() + "\" {");
      out.println("label = \"" + routingTree.getID() + "\";");
      out.println("rankdir=\"BT\";");
      out.println("compound=\"true\";");
      Iterator<Site> siteIter = 
        this.routingTree.siteIterator(TraversalOrder.POST_ORDER);
      while (siteIter.hasNext()) 
      {
        final Site currentSite = siteIter.next();
        out.print(currentSite.getID() + " [fontsize=20 ");
        if (currentSite.isSource()) 
        {
          out.print("shape = doublecircle ");
        }
        out.print("label = \"" + currentSite.getID());
      }
      out.println("\"];");
      //traverse the edges now
      siteIter = this.routingTree.siteIterator(TraversalOrder.POST_ORDER);
      while (siteIter.hasNext()) {
      final Site currentSite = siteIter.next();
      if (currentSite.getOutputs().length==0)
        continue;
      Site parentSite = (Site) currentSite.getOutput(0);
      
      out.print("\"" + currentSite.getID() + "\""
          + edgeSymbol + "\""
          + parentSite.getID() + "\" ");
          out.print("[");
      out.println("style = dashed]; ");
    }       out.println("}");
      out.close();
    }
    catch(IOException e)
    {
      System.out.println("Export failed: " + e.toString());
      System.err.println("Export failed: " + e.toString());
    }
  }
}
