package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

/**
 * class used to represent a set of nodes used within the local strategy for node failure
 * @author alan
 *
 */
public class LogicalOverlayNetwork implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2290495984293624164L;
  
  
  // cluster rep
  private HashMapList<String, String> clusters = null; 
  private SensorNetworkQueryPlan qep = null;
  private String id = "";
  private static Integer idCounter = 1;
  
  /**
   * constructor
   */
  public LogicalOverlayNetwork()
  {
    clusters = new HashMapList<String, String>();
    id = "Overlay" + idCounter.toString();
    idCounter++;
  }
  
  public void addClusterNode(String primary, ArrayList<String> equivilentNodes)
  {
    this.clusters.addAll(primary, equivilentNodes);
  }
  
  /**
   * searches though the rest of the clusters, looking for the equivilentNode.
   * If none exist, tyhen add, else choose which cluster owns the node
   * @param primary
   * @param equivilentNode
   */
  public void addClusterNode(String primary, String equivilentNode)
  {
    String key = locateOtherClusters(equivilentNode);
    if(key == null)
      this.clusters.add(primary, equivilentNode);
    else
    {
      chooseNodeLocation(primary, key, equivilentNode);
    }
  }
  
  /**
   * uses the huristic of putting the node in the smaller of the 2 relations
   * @param primary
   * @param key
   * @param equivilentNode
   */
  private void chooseNodeLocation(String primary, String key, String equivilentNode)
  {
    int oldSize = clusters.get(key).size();
    if(clusters.get(primary) == null)
    {
      this.removeNode(equivilentNode);
      clusters.add(primary, equivilentNode);
    }
    else
    {
      int newSize = clusters.get(primary).size();
      if(newSize < oldSize)
      {
        this.removeNode(equivilentNode);
        clusters.add(primary, equivilentNode);
      }
    }
  }

  private String locateOtherClusters(String equivilentNode)
  {
    Iterator<String> keyIterator = this.clusters.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      ArrayList<String> nodes = clusters.get(key);
      if(nodes.contains(equivilentNode))
        return key;
    }
    return null;
  }

  public Set<String> getKeySet()
  {
    return this.clusters.keySet();
  }

  public ArrayList<String> getEquivilentNodes(String key)
  {
    return clusters.get(key);
  }
  
  /**
   * removes the node from all clusters, as once removed, 
   * relation wont be correct for any cluster which contains the node
   * @param removal
   */
  public void removeNode(String removal)
  {
    Set<String> nodeKeys = clusters.keySet();
    Iterator<String> keyIterator = nodeKeys.iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      clusters.remove(key, removal);
    }
  }
  
  @Override
  public String toString()
  {
    Iterator<String> keys = this.getKeySet().iterator();
    String output = "";
    while(keys.hasNext())
    {
      String key = keys.next();
      output = output.concat(key + " : " + clusters.get(key).toString() + ":" );
    }
    return output;
  }

  /**
   * checks if any cluster contains the candidate
   * @param candidate
   * @return
   */
  public boolean contains(String candidate)
  {
    Iterator<String> keys = this.getKeySet().iterator();
    boolean found = false;
    while(keys.hasNext() && !found)
    {
      String key = keys.next();
      ArrayList<String> candiates = this.getEquivilentNodes(key);
      if(candiates.contains(candidate))
        found = true;
    }
    return found;
  }

  /**
   * finds the cluster head to which this candidate is associated with
   * @param candidate
   * @return
   */
  public String getClusterHeadFor(String candidate)
  {
    Iterator<String> keys = this.getKeySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      ArrayList<String> candiates = this.getEquivilentNodes(key);
      if(candiates.contains(candidate))
        return key;
    }
    return null;
  }

  /**
   * does a simple check to see which candidate has the least energy cost overall in relation to the 
   * parent and children cluster heads.
   * @param key
   * @param children
   * @param parent
   * @param network
   * @return
   */
  public String getReplacement(String key, List<Node> children, Node parent, Topology network)
  {
    Iterator<String> candiates = this.getEquivilentNodes(key).iterator();
    String bestCandiate = null;
    double bestCost = Double.MAX_VALUE;
    while(candiates.hasNext())
    {
      double cost = 0;
      String candiate = candiates.next();
      Site candiateSite = network.getSite(candiate);
      Iterator<Node> childrenIterator = children.iterator();
      while(childrenIterator.hasNext())
      {
        Site child = (Site) childrenIterator.next();
        cost += network.getLinkEnergyCost(child, candiateSite);
      }
      cost += network.getLinkEnergyCost(candiateSite, (Site) parent);
      if(cost < bestCost)
      {
        bestCandiate = candiate;
        bestCost = cost;
      }
    }
    return bestCandiate;
  }

  public void setQep(SensorNetworkQueryPlan qep)
  {
    this.qep = qep;
  }

  public SensorNetworkQueryPlan getQep()
  {
    return qep;
  }
  
  /**
   * updates a cluster by removing head and changing the candidate to the head
   * @param head
   * @param newHead
   */
  public void updateCluster(String head, String newHead)
  {
    ArrayList<String> candidates = clusters.get(head);
    candidates.remove(newHead);
    clusters.remove(head);
    clusters.set(newHead, candidates);
  }

  public void removeQEP()
  {
    this.qep = null;
  }

  public String getId()
  {
    return id;
  }
}
