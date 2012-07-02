package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragment;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class LogicalOverlayNetworkUtils
{

  private String sep = System.getProperty("file.separator");
  
  public LogicalOverlayNetworkUtils()
  {
    
  }
  
  
  public void storeOverlayAsFile(LogicalOverlayNetwork overlay, File outputFile) 
  throws FileNotFoundException, IOException
  {
    if(!outputFile.exists())
      outputFile.mkdir();
    ObjectOutputStream outputStream = 
      new ObjectOutputStream(new FileOutputStream(outputFile.toString() + sep + overlay.getId()));
    outputStream.writeObject(overlay);
    outputStream.flush();
    outputStream.close();
  }
  
  public void storeOverlayAsTextFile(LogicalOverlayNetwork overlay, File outputFile)
  throws FileNotFoundException, IOException
  {
    BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
    Iterator<String> keys = overlay.getKeySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      ArrayList<String> eqNodes = overlay.getEquivilentNodes(key);
      out.write("[" + key + "] : [ " + eqNodes.toString() + " ] ");
      out.newLine();
    }
    out.flush();
    out.close();
  }
  
  public LogicalOverlayNetwork retrieveOverlayFromFile(File outputFile, String id) 
  throws IOException
  {
    ObjectInputStream inputStream = null;
    File file = new File(outputFile + sep + id);
    inputStream = new ObjectInputStream(new FileInputStream(file));
        
    Object obj = null;
    //try reading in object
    try
    {
      obj = inputStream.readObject();
    }
    catch (ClassNotFoundException e)
    {
     throw new IOException(e.getLocalizedMessage());
    }
    
    //if its of the correct format, return overlay
    if (obj instanceof LogicalOverlayNetwork) 
    {
      LogicalOverlayNetwork overlay = (LogicalOverlayNetwork) obj;
      return overlay;
    }   
    return null;
  }


  public void storeSetAsTextFile(ArrayList<LogicalOverlayNetwork> setsOfClusters, File outputFile)
  throws IOException
  {
    BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
    Iterator<LogicalOverlayNetwork> iterator = setsOfClusters.iterator();
    int id = 1;
    while(iterator.hasNext())
    {
      LogicalOverlayNetwork overlay = iterator.next();
      Iterator<String> keys = overlay.getKeySet().iterator();
      out.write(new Integer(id).toString());
      while(keys.hasNext())
      {
        String key = keys.next();
        ArrayList<String> eqNodes = overlay.getEquivilentNodes(key);
        out.write(" [" + key + "] : [ " + eqNodes.toString() + " ] ");
        out.newLine();
      }
      out.newLine();
      id++;
    }
    out.flush();
    out.close();
    
  }
  
  public void exportAsADotFile(IOT iot, LogicalOverlayNetwork overlay, String fileName)
  {
    try
    {
    //create writer
    PrintWriter out = new PrintWriter(new BufferedWriter( new FileWriter(fileName)));
    //create blurb
    out.println("digraph \"" + (String) iot.getID() + "\" {");
    out.println("label = \"" + iot.getID() + "\";");
    out.println("rankdir=\"BT\";");
    out.println("compound=\"true\";");
    //itterate though sites in routing tree (depth to shallow)
    //Iterator<Site> siteIterator = iot.siteIterator();
    Iterator<String> siteIterator = overlay.siteIdIterator();
    while(siteIterator.hasNext())
    {//for each site do blurb and then go though fragments
      String currentSiteString = siteIterator.next();
      Iterator<Site> allSitesIterator = iot.getAllSites().iterator();
      Site currentSite = null;
      while(allSitesIterator.hasNext() && currentSite == null)
      {
        Site site = allSitesIterator.next();
        if(site.getID().equals(currentSiteString))
          currentSite = site;
      }
      if(currentSite != null)
      {
        out.println("subgraph cluster_s" + currentSite.getID() + " {");
        out.println("style=\"rounded,dotted\"");
        out.println("color=blue;");
        //get fragments within site
        Iterator<InstanceFragment> fragmentIterator = iot.getInstanceFragments().iterator();
        while(fragmentIterator.hasNext())
        {
          InstanceFragment currentFrag = fragmentIterator.next();
          if(currentFrag.getSite().getID().equals(currentSite.getID()))
          { //produce blurb
            out.println("subgraph cluster_f" + currentFrag.getID() + " {");
            out.println("style=\"rounded,dashed\"");
            out.println("color=red;");
            //go though operators printing ids
            Iterator<InstanceOperator> operatorIterator = currentFrag.operatorIterator(TraversalOrder.POST_ORDER);
            while(operatorIterator.hasNext())
            {
              InstanceOperator currentOperator = operatorIterator.next();
              out.println("\"" + currentOperator.getID() + "\" ;");
            }
            //output bottom blurb of a cluster
            out.println("fontsize=9;");
            out.println("fontcolor=red");
            out.println("labelloc=t;");
            out.println("labeljust=r;");
            out.println("label =\"frag " + currentFrag.getID() + "\"");
            out.println("}"); 
          }
        }
        //add exchanges if needed
          HashSet<InstanceExchangePart> exchangeParts = currentSite.getInstanceExchangeComponents();
          Iterator<InstanceExchangePart> exchangePartsIterator = exchangeParts.iterator();
          while(exchangePartsIterator.hasNext())
          {
            InstanceExchangePart exchangePart = exchangePartsIterator.next();
              out.println("\"" + exchangePart.getID() + "\" ;");
          }
      
        //output bottom blurb of a site
        out.println("fontsize=9;");
        out.println("fontcolor=red");
        out.println("labelloc=t;");
        out.println("labeljust=r;");
        out.println("label =\"site " + currentSite.getID() + "\"");
        out.println("}");  
      }
    }
    //get operator edges
    String edgeSymbol = "->";
    Iterator<String> i = iot.getOperatorTree().getEdges().keySet().iterator();
    while (i.hasNext()) 
    {
      Edge e = iot.getOperatorTree().getEdges().get((String) i.next());
      out.println("\"" + e.getSourceID()
      + "\"" + edgeSymbol + "\""
      + e.getDestID() + "\" ");
    }
    out.println("}");
    out.flush();
    out.close();
  }
  catch(IOException e)
  {
    System.out.println("Export failed: " + e.toString());
    System.err.println("Export failed: " + e.toString());
  }
  
  }
  
}
