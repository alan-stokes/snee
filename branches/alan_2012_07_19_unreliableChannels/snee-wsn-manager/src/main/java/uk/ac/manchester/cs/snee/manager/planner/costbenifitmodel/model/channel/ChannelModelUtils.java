package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
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
        //cluster head
        final Site currentSite = siteIter.next();
        out.println("subgraph cluster_s" + currentSite.getID() + " {");
        out.println("style=\"rounded,dotted\"");
        out.println("color=blue;");
        if( this.routingTree.getRoot().getID().equals(currentSite.getID()))
        {
          String eqNode = currentSite.getID();
          out.print(eqNode + " [fontsize=20 ");
          ChannelModelSite site = this.modelSites.get(Integer.parseInt(eqNode));
          HashMapList<String, NoiseDataStore> noiseValues = site.getNoiseExpValues();
          Iterator<String> keyIterator = noiseValues.keySet().iterator();
          String noiseOutput = "";
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            ArrayList<NoiseDataStore> noiseValuesForPacketID = noiseValues.get(key);
            Iterator<NoiseDataStore> noiseIterator = noiseValuesForPacketID.iterator();
            while(noiseIterator.hasNext())
            {
              NoiseDataStore noiseValue = noiseIterator.next(); 
              noiseOutput = noiseOutput.concat(noiseValue.toString() + ", ");
            }
            noiseOutput = noiseOutput.concat("\\n\\n");
          }
          out.print("label = \"" + eqNode + "\\n ");
          out.print(noiseOutput);
          out.println("\"];");
        }
        else
        {
        //rest of nodes in logical node
        Iterator<String> eqNodesIterator = 
          logicaloverlayNetwork.getActiveEquivilentNodes(currentSite.getID()).iterator();
        while(eqNodesIterator.hasNext())
        {
          String eqNode = eqNodesIterator.next();
          out.print(eqNode + " [fontsize=20 ");
          ChannelModelSite site = this.modelSites.get(Integer.parseInt(eqNode));
          String format = site.packetRecievedStringBoolFormat();
          HashMapList<String, NoiseDataStore> noiseValues = site.getNoiseExpValues();
          Iterator<String> keyIterator = noiseValues.keySet().iterator();
          String noiseOutput = "";
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            ArrayList<NoiseDataStore> noiseValuesForPacketID = noiseValues.get(key);
            Iterator<NoiseDataStore> noiseIterator = noiseValuesForPacketID.iterator();
            while(noiseIterator.hasNext())
            {
              NoiseDataStore noiseValue = noiseIterator.next(); 
              noiseOutput = noiseOutput.concat(key + " " + noiseValue.toString() + ", ");
            }
            noiseOutput = noiseOutput.concat("\\n\\n");
          }
          out.print("label = \"" + eqNode + "\\n " + format + "\\n");
          out.print(noiseOutput);
          out.println("\"];");
        }
        out.println("}");
        }
      }
      
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
