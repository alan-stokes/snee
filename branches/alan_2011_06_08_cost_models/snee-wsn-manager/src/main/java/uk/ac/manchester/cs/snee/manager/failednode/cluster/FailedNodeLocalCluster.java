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
public class FailedNodeLocalCluster implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2290495984293624164L;
  
  
  // cluster rep
  private HashMapList<String, String> equivilentNodes; 
  
  /**
   * constructor
   */
  public FailedNodeLocalCluster()
  {
    equivilentNodes = new HashMapList<String, String>();
  }
  
  public void addClusterNode(String primary, ArrayList<String> equivilentNodes)
  {
    this.equivilentNodes.addAll(primary, equivilentNodes);
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
      this.equivilentNodes.add(primary, equivilentNode);
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
    int oldSize = equivilentNodes.get(key).size();
    if(equivilentNodes.get(primary) == null)
    {
      this.removeNode(equivilentNode);
      equivilentNodes.add(primary, equivilentNode);
    }
    else
    {
      int newSize = equivilentNodes.get(primary).size();
      if(newSize < oldSize)
      {
        this.removeNode(equivilentNode);
        equivilentNodes.add(primary, equivilentNode);
      }
    }
  }

  private String locateOtherClusters(String equivilentNode)
  {
    Iterator<String> keyIterator = this.equivilentNodes.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      ArrayList<String> nodes = equivilentNodes.get(key);
      if(nodes.contains(equivilentNode))
        return key;
    }
    return null;
  }

  public Set<String> getKeySet()
  {
    return this.equivilentNodes.keySet();
  }

  public ArrayList<String> getEquivilentNodes(String key)
  {
    return equivilentNodes.get(key);
  }
  
  /**
   * removes the node from all clusters, as once removed, 
   * relation wont be correct for any cluster which contains the node
   * @param removal
   */
  public void removeNode(String removal)
  {
    Set<String> nodeKeys = equivilentNodes.keySet();
    Iterator<String> keyIterator = nodeKeys.iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      equivilentNodes.remove(key, removal);
    }
  }
  
  
}
