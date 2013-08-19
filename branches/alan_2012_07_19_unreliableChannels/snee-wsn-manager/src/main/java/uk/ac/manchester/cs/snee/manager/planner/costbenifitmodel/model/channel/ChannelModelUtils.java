package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.cxf.common.util.SortedArraySet;

import uk.ac.manchester.cs.snee.common.graph.Node;
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
      HashMap<String, HashMap<String, Double>> totalPercentages = new HashMap<String, HashMap<String, Double>>();
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
          HashMap<String,Integer> lost = new HashMap<String,Integer>();
          HashMap<String,Integer> transmitted = new HashMap<String,Integer>();
          HashMap<String,Integer> recieved = new HashMap<String,Integer>();
          HashMap<String, HashMap<Integer, Boolean>> totalRecieved = new HashMap<String, HashMap<Integer,Boolean>>();
          HashMap<String, HashMap<Integer, Boolean>> totallost = new HashMap<String, HashMap<Integer,Boolean>>();
          String noiseOutput = "";
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            ArrayList<NoiseDataStore> noiseValuesForPacketID = noiseValues.get(key);
            Iterator<NoiseDataStore> noiseIterator = noiseValuesForPacketID.iterator();
            while(noiseIterator.hasNext())
            {
              NoiseDataStore noiseValue = noiseIterator.next(); 
              if(transmitted.get(noiseValue.source()) == null)
                transmitted.put(noiseValue.source(),1);
              else
                transmitted.put(noiseValue.source(), transmitted.get(noiseValue.source()) +1);
              
              if(!noiseValue.wasRecieved())
                if(lost.get(noiseValue.source()) == null)
                  lost.put(noiseValue.source(),1);
                else
                  lost.put(noiseValue.source(), lost.get(noiseValue.source()) +1);
              else
              {
                  if(recieved.get(noiseValue.source()) == null)
                    recieved.put(noiseValue.source(),1);
                  else
                    recieved.put(noiseValue.source(), recieved.get(noiseValue.source()) +1);
              }
              noiseOutput = noiseOutput.concat(noiseValue.toString() + ", ");
              int position = Integer.parseInt(key.split("-")[1]);
              if(totalRecieved.get(noiseValue.sourceID())==null)
              {
                HashMap<Integer, Boolean> total = new HashMap<Integer, Boolean>();
                total.put(position, noiseValue.wasRecieved());
                totalRecieved.put(noiseValue.sourceID(), total);
              }
              else
              {
                HashMap<Integer, Boolean> total = totalRecieved.get(noiseValue.sourceID());
                total.put(position, noiseValue.wasRecieved());
              }
            }
            HashMap<String, Integer> totalrecieved = new HashMap<String, Integer>();
            HashMap<String, Integer> totalLost = new HashMap<String, Integer>();
            HashMap<String, Double> percentage = new HashMap<String, Double>();
            Iterator<Node> inputs = this.routingTree.getSite(logicaloverlayNetwork.getClusterHeadFor(site.toString())).getInputsList().iterator();
            while(inputs.hasNext())
            {
              calculatetotals(totalrecieved, totalRecieved, totalLost, totallost, inputs.next().toString());
            }
            calculatePercentages(totalrecieved, totalLost, percentage);
            
            noiseOutput = noiseOutput.concat("TRAN=" + transmitted + " LOST=" + lost +
                                             "REC=" + recieved + "totalR "+ totalrecieved + 
                                             "totalL" +totalLost+ "totalP" + percentage + "\\n\\n");
            totalPercentages.put(site.toString(), percentage);
          }
          out.print("label = \"" + eqNode + "\\n ");
          out.print(noiseOutput);
          out.println("\"];");
        }
        else
        {
        //rest of nodes in logical node
        Set<String> eq = new SortedArraySet<String>();
        eq.addAll(logicaloverlayNetwork.getActiveEquivilentNodes(currentSite.getID()));
        Iterator<String> eqNodesIterator = eq.iterator();
          
        while(eqNodesIterator.hasNext())
        {
          String eqNode = eqNodesIterator.next();
          out.print(eqNode + " [fontsize=20 ");
          ChannelModelSite site = this.modelSites.get(Integer.parseInt(eqNode));
          String format = site.packetRecievedStringBoolFormat();
          HashMapList<String, NoiseDataStore> noiseValues = site.getNoiseExpValues();
          Iterator<String> keyIterator = noiseValues.keySet().iterator();
          String noiseOutput = "";
          HashMap<String,Integer> lost = new HashMap<String,Integer>();
          HashMap<String,Integer> transmitted = new HashMap<String,Integer>();
          HashMap<String,Integer> recieved = new HashMap<String,Integer>();
          HashMap<String, HashMap<Integer, Boolean>> totalRecieved = new HashMap<String, HashMap<Integer,Boolean>>();
          HashMap<String, HashMap<Integer, Boolean>> totallost = new HashMap<String, HashMap<Integer,Boolean>>();
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            ArrayList<NoiseDataStore> noiseValuesForPacketID = noiseValues.get(key);
            Iterator<NoiseDataStore> noiseIterator = noiseValuesForPacketID.iterator();
            while(noiseIterator.hasNext())
            {
              NoiseDataStore noiseValue = noiseIterator.next(); 
              if(transmitted.get(noiseValue.source()) == null)
                transmitted.put(noiseValue.source(),1);
              else
                transmitted.put(noiseValue.source(), transmitted.get(noiseValue.source()) +1);
              if(!noiseValue.wasRecieved())
                if(lost.get(noiseValue.source()) == null)
                  lost.put(noiseValue.source(),1);
                else
                  lost.put(noiseValue.source(), lost.get(noiseValue.source()) +1);
              else
              {
                  if(recieved.get(noiseValue.source()) == null)
                    recieved.put(noiseValue.source(), 1);
                  else
                    recieved.put(noiseValue.source(), recieved.get(noiseValue.source()) +1);
              }
              noiseOutput = noiseOutput.concat(key + " " + noiseValue.toString() + ", ");
              int position = Integer.parseInt(key.split("-")[1]);
              if(totalRecieved.get(noiseValue.sourceID())==null)
              {
                HashMap<Integer, Boolean> total = new HashMap<Integer, Boolean>();
                total.put(position, noiseValue.wasRecieved());
                totalRecieved.put(noiseValue.sourceID(), total);
              }
              else
              {
                HashMap<Integer, Boolean> total = totalRecieved.get(noiseValue.sourceID());
                if(total.get(position) == null || total.get(position) == false)
                total.put(position, noiseValue.wasRecieved());
              }
            }
            HashMap<String, Integer> totalrecieved = new HashMap<String, Integer>();
            HashMap<String, Integer> totalLost = new HashMap<String, Integer>();
            HashMap<String, Double> percentage = new HashMap<String, Double>();
            Iterator<Node> inputs = this.routingTree.getSite(logicaloverlayNetwork.getClusterHeadFor(site.toString())).getInputsList().iterator();
            while(inputs.hasNext())
            {
              calculatetotals(totalrecieved, totalRecieved, totalLost, totallost, inputs.next().toString());
            }
            calculatePercentages(totalrecieved, totalLost, percentage);
            noiseOutput = noiseOutput.concat("TRAN=" + transmitted + " LOST=" + lost +
                                             "REC=" + recieved + "totalR "+ totalrecieved + 
                                             "totalL" +totalLost+ "totalP" + percentage + "\\n\\n");
            totalPercentages.put(site.toString(), percentage);
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
      
      out = new PrintWriter(new BufferedWriter(new FileWriter(new File(superFolder + sep + "iterationPercentages" + iteration))));
      out.write("1 " + totalPercentages.get("6").get("19188") + "\n");
      out.write("3 " + totalPercentages.get("2").get("21206") + "\n");
      out.write("4 " + totalPercentages.get("11").get("22223") + "\n");
      out.write("5 " + totalPercentages.get("5").get("242511") + "\n");
      out.write("7 " + totalPercentages.get("1").get("52627") + "\n");
      out.write("8 " + totalPercentages.get("13").get("12829") + "\n");
      out.write("9 " + totalPercentages.get("0").get("323313") + "\n");
      out.flush();
      out.close();
    }
    catch(IOException e)
    {
      System.out.println("Export failed: " + e.toString());
      System.err.println("Export failed: " + e.toString());
    }
  }

  private void calculatePercentages(HashMap<String, Integer> totalrecieved,
      HashMap<String, Integer> totalLost,
      HashMap<String, Double> percentage)
  {
    Iterator<String> iterator = totalrecieved.keySet().iterator();
    while(iterator.hasNext())
    {
      String key = iterator.next();
      Integer packetsLost = totalLost.get(key);
      Integer packetsRecieved = totalrecieved.get(key);
      Integer max = 100;
      Double percentageValue = (max.doubleValue() / packetsRecieved.doubleValue())*packetsLost.doubleValue();
      percentage.put(key, percentageValue);
    }
    
  }
  /*
  private void calculatetotals(HashMap<String, Integer> totalrecieved,
      HashMap<String, HashMap<Integer, Boolean>> totalRecieved2, 
      HashMap<String, Integer> totalLost, 
      HashMap<String, HashMap<Integer, Boolean>> totallost2, String input)
  {
    int recievedCounter = 0;
    int lostCounter = 0;
    String keyID = "";
    Iterator<String> keys = totalRecieved2.keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      HashMap<Integer, Boolean> results = totalRecieved2.get(key);
      Iterator<Integer> Resultkeys = results.keySet().iterator();
      while(Resultkeys.hasNext())
      {
        Integer position = Resultkeys.next();
        if(results.get(position) && 
            this.logicaloverlayNetwork.getActiveNodesInRankedOrder(input).contains(key))
          recievedCounter++;
        else if(!results.get(position) && 
            this.logicaloverlayNetwork.getActiveNodesInRankedOrder(input).contains(key))
          lostCounter++;
      }
    }
    keys = totalRecieved2.keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      if(this.logicaloverlayNetwork.getActiveNodesInRankedOrder(input).contains(key))
        keyID = keyID.concat(key);
    }
    totalrecieved.put(keyID, recievedCounter);
    totalLost.put(keyID, lostCounter);
    recievedCounter = 0;
    lostCounter = 0;
  }
  */

  private void calculatetotals(HashMap<String, Integer> totalrecieved,
      HashMap<String, HashMap<Integer, Boolean>> totalRecieved2, 
      HashMap<String, Integer> totalLost, 
      HashMap<String, HashMap<Integer, Boolean>> totallost2, String input)
  {
    int recievedCounter = 0;
    int lostCounter = 0;
    String keyID = "";
    HashMap<Integer, Boolean> combined = new HashMap<Integer, Boolean>();
    Iterator<String> keys = totalRecieved2.keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      HashMap<Integer, Boolean> results = totalRecieved2.get(key);
      Iterator<Integer> Resultkeys = results.keySet().iterator();
      while(Resultkeys.hasNext())
      {
        Integer position = Resultkeys.next();
        if(results.get(position)  && 
           this.logicaloverlayNetwork.getActiveNodesInRankedOrder(input).contains(key))
          combined.put(position, true);
        else if (!results.get(position)  && 
           this.logicaloverlayNetwork.getActiveNodesInRankedOrder(input).contains(key))
          combined.put(position, false);
      }
    }
    Iterator<Integer> comkeys = combined.keySet().iterator();
    while(comkeys.hasNext())
    {
      Integer key = comkeys.next();
      if(combined.get(key))
        recievedCounter++;
      else
        lostCounter++;
    }
    keys = totalRecieved2.keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      if(this.logicaloverlayNetwork.getActiveNodesInRankedOrder(input).contains(key))
        keyID = keyID.concat(key);
    }
    
    totalrecieved.put(keyID, recievedCounter);
    totalLost.put(keyID, lostCounter);
    recievedCounter = 0;
    lostCounter = 0;
  }
}
