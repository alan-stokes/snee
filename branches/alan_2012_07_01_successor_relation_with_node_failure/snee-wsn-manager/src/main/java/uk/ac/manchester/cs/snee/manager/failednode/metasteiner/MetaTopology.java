package uk.ac.manchester.cs.snee.manager.failednode.metasteiner;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.manager.failednode.alternativerouter.HeuristicSet;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class MetaTopology implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -8179651442599719941L;
  
  Topology basicTopology;

  public MetaTopology(Topology top)
  {
    this.basicTopology = top;
  }
  
  /**
   * calculates shortest path between source and destination sites, based off 
   * measurements defined in the heuristicSet
   * @param sourceID
   * @param destID
   * @param set
   * @return
   */
  public Path getShortestPath(String sourceID, String destID, HeuristicSet set)
  {
    final HashMap<String, Double> distance = new HashMap<String, Double>();
    final HashMap<String, String> previous = new HashMap<String, String>();
    final HashSet<String> shortestDistanceFound = new HashSet<String>();
    final HashSet<String> shortestDistanceNotFound = new HashSet<String>();

    final Iterator<String> i = this.basicTopology.getAllNodes().keySet().iterator();
    while (i.hasNext()) {
        final String vid = i.next();
        distance.put(vid, new Double(Double.POSITIVE_INFINITY));
        shortestDistanceNotFound.add(vid);
    }
    distance.put(sourceID, new Double(0));

    //find the next closest node
    while (!shortestDistanceNotFound.isEmpty()) {

        final String nextClosestVertexID = dijkstra_getNextClosestNodeID(shortestDistanceNotFound,
            distance);
        if (nextClosestVertexID == null) {
      break;
        }

        shortestDistanceNotFound.remove(nextClosestVertexID);
        shortestDistanceFound.add(nextClosestVertexID);

        final Node[] nextClosestNodes = this.basicTopology.getAllNodes().get(nextClosestVertexID)
          .getOutputs();
        for (Node element : nextClosestNodes) {
      final Site n = (Site) element;
      final RadioLink e = (RadioLink) this.basicTopology.getEdges().get(basicTopology
        .generateEdgeID(nextClosestVertexID, n.getID()));
      if(e != null)
      {
      final double try_d = (distance.get(nextClosestVertexID))
        .doubleValue()
        + e.getCost(set.getEdgeChoice(e.getID()));
      final double current_d = (distance.get(n.getID()))
        .doubleValue();
      if (try_d < current_d) {
          distance.put(n.getID(), new Double(try_d));
          previous.put(n.getID(), nextClosestVertexID); //?
      }
      }
        }
        
    }

    Path path = new Path((Site) this.basicTopology.getAllNodes().get(destID));
    
    if (previous.containsKey(destID)) {
        String siteID = destID;
        while (previous.get(siteID) != null) {
          siteID = previous.get(siteID);
          path.prepend(basicTopology.getSite(siteID));
        }
    } else if (!sourceID.equals(destID)) {
        path = null;
    }
    return path;
  }
  
  /**
   * calculates next node to use
   * @param shortestDistanceNotFound
   * @param distance
   * @return
   */
  private String dijkstra_getNextClosestNodeID(
      HashSet<String> shortestDistanceNotFound, HashMap<String, Double> distance)
  {
    Double nextClosestDist = new Double(Double.POSITIVE_INFINITY);
    String nextClosestNodeID = null;
    final Iterator<String> j = shortestDistanceNotFound.iterator();
    while (j.hasNext()) 
    {
      final String jid = (String) j.next();
      if ((distance.get(jid)).compareTo(nextClosestDist) < 0) 
      {
        nextClosestDist = distance.get(jid);
        nextClosestNodeID = jid;
      }
      if((distance.get(jid)).compareTo(nextClosestDist) == 0)
      {
        Random random = new Random();
        int choice = random.nextInt(100);
        if(choice <= 50)
        {
          nextClosestDist = distance.get(jid);
          nextClosestNodeID = jid;
        }
      }
    }
    return nextClosestNodeID;
  }
  
  /**
   * calculates next node to use
   * @param shortestDistanceNotFound
   * @param distance
   * @return
   */
  private String dijkstra_getNextLongestNodeID(
      HashSet<String> LongestDistanceNotFound, HashMap<String, Double> distance)
  {
    Double nextClosestDist = new Double(Double.POSITIVE_INFINITY);
    String nextClosestNodeID = null;
    final Iterator<String> j = LongestDistanceNotFound.iterator();
    while (j.hasNext()) 
    {
      final String jid = (String) j.next();
      if ((distance.get(jid)).compareTo(nextClosestDist) > 0) 
      {
        nextClosestDist = distance.get(jid);
        nextClosestNodeID = jid;
      }
      if((distance.get(jid)).compareTo(nextClosestDist) == 0)
      {
        Random random = new Random();
        int choice = random.nextInt(100);
        if(choice <= 50)
        {
          nextClosestDist = distance.get(jid);
          nextClosestNodeID = jid;
        }
      }
    }
    return nextClosestNodeID;
  }
  
  /**
   * calculates next node to use
   * @param shortestDistanceNotFound
   * @param distance
   * @return
   */
  private String dijkstra_getNextRandomNodeID(
      HashSet<String> randomDistanceNotFound, HashMap<String, Double> distance)
  {
    Random random = new Random();
    int choice = random.nextInt(randomDistanceNotFound.size());
    Iterator<String> nodeIDIterator = randomDistanceNotFound.iterator();
    int counter = 0;
    while(counter < choice)
    {
      nodeIDIterator.next();
    }
    return nodeIDIterator.next();
  }

  /**
   * get cost of edge in relation to some cost defined in heuristicSet
   * @param sourceSite
   * @param destinationSite
   * @param set
   * @return
   */
  public double getEdgeCost(Site sourceSite, Site destinationSite, HeuristicSet set)
  {
    RadioLink edge = basicTopology.getRadioLink(sourceSite, destinationSite);
    return edge.getCost(set.getEdgeChoice(edge.getID()));  
  }
  
  public Topology getBasicTopology()
  {
    return basicTopology;
  }

  public Path getLongPath(String sourceID, String destID, HeuristicSet set)
  {
    final HashMap<String, Double> distance = new HashMap<String, Double>();
    final HashMap<String, String> previous = new HashMap<String, String>();
    final HashSet<String> longestDistanceFound = new HashSet<String>();
    final HashSet<String> longestDistanceNotFound = new HashSet<String>();

    final Iterator<String> i = this.basicTopology.getAllNodes().keySet().iterator();
    while (i.hasNext()) {
        final String vid = i.next();
        distance.put(vid, new Double(Double.POSITIVE_INFINITY));
        longestDistanceNotFound.add(vid);
    }
    distance.put(sourceID, new Double(0));

    //find the next closest node
    while (!longestDistanceNotFound.isEmpty()) {

        final String nextLongestVertexID =  dijkstra_getNextLongestNodeID(longestDistanceNotFound,
            distance);
        if (nextLongestVertexID == null) {
      break;
        }

        longestDistanceNotFound.remove(nextLongestVertexID);
        longestDistanceFound.add(nextLongestVertexID);

        final Node[] nextClosestNodes = this.basicTopology.getAllNodes().get(nextLongestVertexID)
          .getOutputs();
        for (Node element : nextClosestNodes) {
      final Site n = (Site) element;
      final RadioLink e = (RadioLink) this.basicTopology.getEdges().get(basicTopology
        .generateEdgeID(nextLongestVertexID, n.getID()));
      if(e != null)
      {
      final double try_d = (distance.get(nextLongestVertexID))
        .doubleValue()
        + e.getCost(set.getEdgeChoice(e.getID()));
      final double current_d = (distance.get(n.getID()))
        .doubleValue();
      if (try_d < current_d) {
          distance.put(n.getID(), new Double(try_d));
          previous.put(n.getID(), nextLongestVertexID); //?
      }
      }
        }
        
    }

    Path path = new Path((Site) this.basicTopology.getAllNodes().get(destID));
    
    if (previous.containsKey(destID)) {
        String siteID = destID;
        while (previous.get(siteID) != null) {
          siteID = previous.get(siteID);
          path.prepend(basicTopology.getSite(siteID));
        }
    } else if (!sourceID.equals(destID)) {
        path = null;
    }
    return path;
  }

  public Path getRandomPath(String sourceID, String destID, HeuristicSet set)
  {
    final HashMap<String, Double> distance = new HashMap<String, Double>();
    final HashMap<String, String> previous = new HashMap<String, String>();
    final HashSet<String> randomDistanceFound = new HashSet<String>();
    final HashSet<String> randomDistanceNotFound = new HashSet<String>();

    final Iterator<String> i = this.basicTopology.getAllNodes().keySet().iterator();
    while (i.hasNext()) {
        final String vid = i.next();
        distance.put(vid, new Double(Double.POSITIVE_INFINITY));
        randomDistanceNotFound.add(vid);
    }
    distance.put(sourceID, new Double(0));

    //find the next closest node
    while (!randomDistanceNotFound.isEmpty()) {

        final String nextClosestVertexID = dijkstra_getNextRandomNodeID(randomDistanceNotFound,
            distance);
        if (nextClosestVertexID == null) {
      break;
        }

        randomDistanceNotFound.remove(nextClosestVertexID);
        randomDistanceFound.add(nextClosestVertexID);

        final Node[] nextClosestNodes = this.basicTopology.getAllNodes().get(nextClosestVertexID)
          .getOutputs();
        for (Node element : nextClosestNodes) {
      final Site n = (Site) element;
      final RadioLink e = (RadioLink) this.basicTopology.getEdges().get(basicTopology
        .generateEdgeID(nextClosestVertexID, n.getID()));
      if(e != null)
      {
      final double try_d = (distance.get(nextClosestVertexID))
        .doubleValue()
        + e.getCost(set.getEdgeChoice(e.getID()));
      final double current_d = (distance.get(n.getID()))
        .doubleValue();
      if (try_d < current_d) {
          distance.put(n.getID(), new Double(try_d));
          previous.put(n.getID(), nextClosestVertexID); //?
      }
      }
        }
        
    }

    Path path = new Path((Site) this.basicTopology.getAllNodes().get(destID));
    
    if (previous.containsKey(destID)) {
        String siteID = destID;
        while (previous.get(siteID) != null) {
          siteID = previous.get(siteID);
          path.prepend(basicTopology.getSite(siteID));
        }
    } else if (!sourceID.equals(destID)) {
        path = null;
    }
    return path;
  }

}
