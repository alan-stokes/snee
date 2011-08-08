package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;

/**
 * class used to represent a set of nodes used within the local strategy for node failure
 * @author alan
 *
 */
public class FailedNodeLocalCluster 
{
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
  
  public void addClusterNode(String primary, String equivilentNodes)
  {
    this.equivilentNodes.add(primary, equivilentNodes);
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
