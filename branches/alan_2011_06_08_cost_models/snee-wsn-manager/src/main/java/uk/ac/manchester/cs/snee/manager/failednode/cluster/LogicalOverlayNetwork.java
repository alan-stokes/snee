package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;

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
  private HashMapList<String, String> clusters; 
  
  /**
   * constructor
   */
  public LogicalOverlayNetwork()
  {
    clusters = new HashMapList<String, String>();
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
  
}
