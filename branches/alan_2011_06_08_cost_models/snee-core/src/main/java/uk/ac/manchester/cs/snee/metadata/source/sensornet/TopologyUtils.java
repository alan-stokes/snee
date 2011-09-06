package uk.ac.manchester.cs.snee.metadata.source.sensornet;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

public class TopologyUtils 
{
  Logger logger = Logger.getLogger(TopologyUtils.class.getName());
  private Topology network;
  
  public TopologyUtils(Topology network)
  {
    this.network = network;
  }
  
  public void exportAsDOTFile(String fname) 
  throws SchemaMetadataException 
  {
    exportAsDOTFile(fname, "");
  }
  
  /**
   * Exports the graph as a file in the DOT language used by GraphViz.
   * @see http://www.graphviz.org/
   * 
   * @param fname   the name of the output file
   * @throws SchemaMetadataException 
   */
  public void exportAsDOTFile(String fname, String label) 
  throws SchemaMetadataException {
    if (logger.isDebugEnabled())
      logger.debug("ENTER exportAsDOTFile() with " + fname + " " +
          label);
    try {
      PrintWriter out = new PrintWriter(new BufferedWriter(
          new FileWriter(fname)));

      String edgeSymbol;
      if (network.isDirected()) {
        out.println("digraph \"" + (String) network.getName() + "\" {");
        edgeSymbol = "->";
      } else {
        out.println("graph \"" + (String) network.getName() + "\" {");
        edgeSymbol = "--";
      }

      out.println("label = \"" + label + "\";");
      out.println("rankdir=\"BT\";");

      //traverse the edges now
      List<Edge> edgesAlreadyOutputted = new ArrayList<Edge>();
      Iterator<String> i = network.getEdges().keySet().iterator();
      while (i.hasNext()) {
        Edge e = network.getEdges().get((String) i.next());
        if(!alreadyOutputted(edgesAlreadyOutputted, e))
        {
          out.println("\"" + network.getAllNodes().get(e.getSourceID()).getID()
            + "\"" + edgeSymbol + "\""
            + network.getAllNodes().get(e.getDestID()).getID() + "\" [arrowhead = \"both\"] ");
          edgesAlreadyOutputted.add(e);
        }
      }
      out.println("}");
      out.close();
    } catch (IOException e) {
      logger.warn("Unable to write to file " + fname);
    }
    if (logger.isDebugEnabled())
      logger.debug("RETURN");
  }

  private boolean alreadyOutputted(List<Edge> edgesAlreadyOutputted, Edge edge)
  {
    Iterator<Edge> edgeIterator = edgesAlreadyOutputted.iterator();
    while(edgeIterator.hasNext())
    {
      Edge currentEdge = edgeIterator.next();
      if((currentEdge.getDestID().equals(edge.getSourceID())) &&
         (edge.getDestID().equals(currentEdge.getSourceID())))
         return true;
    }
    return false;
  }
  
}
