package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

/**
 * class used to represent a set of nodes used within the local strategy for node failure
 * @author alan
 *
 */
public class LogicalOverlayNetworkHierarchy extends LogicalOverlayNetwork implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2290495984293624164L;
  
  
  // cluster rep
  private HashMapList<String, String> clusters = null; 
  private HashMapList<String, String> activeClusters = null; 
  private HashMap<String, Integer> activeClustersPriority = null; 
  private HashMap<String, Integer> activeClustersOriginalPriority = null; 
  private SensorNetworkQueryPlan qep = null;
  private String id = "";
  private int resilientLevel;
  private int k;
  private Topology deployment;
  
  public LogicalOverlayNetworkHierarchy(HashMapList<String, String> newHashMapList, Topology deployment,
                                        SensorNetworkQueryPlan qep)
  throws NumberFormatException, SNEEConfigurationException
  {
    this.qep = qep;
    clusters = new HashMapList<String, String>();
    this.deployment = deployment;
    Iterator<String> keys = newHashMapList.keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      Iterator<String> eqNodes = newHashMapList.get(key).iterator();
      while(eqNodes.hasNext())
      {
        String eqNode = eqNodes.next();
        this.addClusterNode(key, eqNode);
      }
    }
    id = "HierarchyBasedOverlay";
    
    //first discover the resilient level for the reliable channel.
    resilientLevel = Integer.parseInt(
      SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_RESILIENTLEVEL));
    k = Integer.parseInt(
        SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_LEVEL));
    //check if possible to create cluster.
    if(k < resilientLevel)
      throw new SNEEConfigurationException("cannot support a resilient level above the minimal level defined as k");
    if(resilientLevel <= 0)
      resilientLevel = 1; 
    
    selectActiveNodes();
  }
  
  public LogicalOverlayNetworkHierarchy(LogicalOverlayNetworkHierarchy logicaloverlayNetwork, 
                                        Topology network, SensorNetworkQueryPlan qep)
  throws NumberFormatException, SNEEConfigurationException
  {
    this.qep = qep;
    clusters = new HashMapList<String, String>();
    this.deployment = network;
    Iterator<String> keys = logicaloverlayNetwork.getClusters().keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      Iterator<String> eqNodes = logicaloverlayNetwork.getClusters().get(key).iterator();
      while(eqNodes.hasNext())
      {
        String eqNode = eqNodes.next();
        this.addClusterNode(key, eqNode);
      }
    }
    id = "HierarchyBasedOverlay";
    
    //first discover the resilient level for the reliable channel.
    resilientLevel = Integer.parseInt(
      SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_RESILIENTLEVEL));
    k = Integer.parseInt(
        SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_LEVEL));
    //check if possible to create cluster.
    if(k < resilientLevel)
      throw new SNEEConfigurationException("cannot support a resilient level above the minimal level defined as k");
    if(resilientLevel <= 0)
      resilientLevel = 1; 
    
    //copy over selectedNodes
    this.activeClusters = logicaloverlayNetwork.getActiveClusters();
    this.activeClustersPriority = logicaloverlayNetwork.getActiveClustersPriorities();
    this.activeClustersOriginalPriority = logicaloverlayNetwork.getActiveClustersOriginalPriority();
  }

  private HashMap<String, Integer> getActiveClustersOriginalPriority()
  {
    Cloner cloner = new Cloner();
    return cloner.deepClone(this.activeClustersOriginalPriority);
  }

  private HashMap<String, Integer> getActiveClustersPriorities()
  {
    Cloner cloner = new Cloner();
    return cloner.deepClone(this.activeClustersPriority);
  }

  private HashMapList<String, String> getActiveClusters()
  {
   Cloner cloner = new Cloner();
   return cloner.deepClone(this.activeClusters);
  }
  
  

  /**
   * creates the list of nodes that will paritcipate with the unreliable channel model.
   * Is goverened by the resilient level
   */
  @SuppressWarnings("unchecked")
  private void selectActiveNodes()
  {
    this.activeClusters = new HashMapList<String, String>();
    this.activeClustersPriority = new HashMap<String, Integer>();
    this.activeClustersOriginalPriority = new HashMap<String, Integer>();
    Iterator<String> ClusterHeadIterator = this.clusters.keySet().iterator();
    while(ClusterHeadIterator.hasNext())
    {
      int priority = 1;
      String clusterHeadID = ClusterHeadIterator.next();
      if(resilientLevel == 1)
      {
        activeClusters.add(clusterHeadID, clusterHeadID);
        activeClustersPriority.put(clusterHeadID, priority);
        activeClustersOriginalPriority.put(clusterHeadID, priority);
        priority ++;
      }
      else
      {
        activeClusters.add(clusterHeadID, clusterHeadID);
        activeClustersPriority.put(clusterHeadID, priority);
        activeClustersOriginalPriority.put(clusterHeadID, priority);
        priority ++;
        ArrayList<String> equivNodes = this.getInactiveCluster(clusterHeadID);
        equivNodes = (ArrayList<String>) equivNodes.clone();
        int leftOverResilience = resilientLevel -1;
        while(leftOverResilience >= 0)
        {
          String bestID = locateCheapestNode(equivNodes, clusterHeadID);
          equivNodes.remove(bestID);
          activeClusters.add(clusterHeadID, bestID);
          activeClustersPriority.put(bestID, priority);
          activeClustersOriginalPriority.put(bestID, priority);
          priority++;
          leftOverResilience--;
        }
      }
    }
  }

  private ArrayList<String> getInactiveCluster(String clusterHeadID)
  {
    ArrayList<String> returable = new ArrayList<String>();
    Iterator<String> clusteriterator = this.clusters.get(clusterHeadID).iterator();
    while(clusteriterator.hasNext())
    {
      String possibleNodeID = clusteriterator.next();
      if(!this.activeClusters.get(clusterHeadID).contains(possibleNodeID))
        returable.add(possibleNodeID);
    }
    return returable;
  }

  /**
   * returns the cheapest node of a cluster excluding the cluster head.
   * collects data about the routing tree given a cluster head
   * @param equivNodes
   * @param clusterHeadID
   * @return
   */
  private String locateCheapestNode(ArrayList<String> equivNodes, String clusterHeadID)
  {
    Site rtSite = this.qep.getRT().getSite(clusterHeadID);
    List<Node> children = rtSite.getInputsList();
    Node parent = rtSite.getOutput(0);
    String nextBest = this.getReplacement(clusterHeadID, children, parent, equivNodes);
    return nextBest;
  }

  /**
   * compares the candidates comm cost to locate the node with the cheapest overall cost for transmissions
   * @param clusterHeadID
   * @param children
   * @param parent
   * @param equivNodes
   * @return
   */
  private String getReplacement(String clusterHeadID, List<Node> children,
      Node parent, ArrayList<String> equivNodes)
  {
    Iterator<String> candiates = equivNodes.iterator();
    String bestCandiate = null;
    double bestCost = Double.MAX_VALUE;
    while(candiates.hasNext())
    {
      double cost = 0;
      String candiate = candiates.next();
      Site candiateSite = deployment.getSite(candiate);
      Iterator<Node> childrenIterator = children.iterator();
      while(childrenIterator.hasNext())
      {
        Site child = (Site) childrenIterator.next();
        cost += deployment.getLinkEnergyCost(child, candiateSite);
      }
      if(deployment.hasLink(candiateSite, (Site) parent))
      {
        cost += deployment.getLinkEnergyCost(candiateSite, (Site) parent);
        if(cost < bestCost)
        {
          bestCandiate = candiate;
          bestCost = cost;
        }
      }
    }
    return bestCandiate;
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
    this.clusters.add(primary, equivilentNode);
  }

  public Set<String> getKeySet()
  {
    return this.clusters.keySet();
  }

  /**
   * get nodes that are defined as Equivilent to key
   * @param key
   * @return
   */
  public ArrayList<String> getEquivilentNodes(String key)
  {
    ArrayList<String> cluster = clusters.get(key);
    return cluster;
  }
  
  public ArrayList<String> getActiveEquivilentNodes(String key)
  {
    ArrayList<String> cluster = activeClusters.get(key);
    return cluster;
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
    if(this.isClusterHead(candidate))
      return candidate; 
    
    Iterator<String> keys = this.getKeySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      ArrayList<String> candiates = this.getEquivilentNodes(key);
      if(candiates.contains(candidate))
        return key;
    }
    
    return candidate;
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
    ArrayList<String> candiates = new ArrayList<String>();
    ArrayList<String> activeSet = this.getActiveEquivilentNodes(key);
    Iterator<String> clusterSetIterator = this.getEquivilentNodes(key).iterator();
    while(clusterSetIterator.hasNext())
    {
      String candidate = clusterSetIterator.next();
      if(!activeSet.contains(candidate))
        candiates.add(candidate);
    }
    if(candiates.size() == 0)
    {
      candiates = this.getActiveEquivilentNodes(key);
      candiates.remove(key);
    }
    
   
    
    Iterator<String> candiatesIterator = candiates.iterator();
    String bestCandiate = null;
    double bestCost = Double.MAX_VALUE;
    while(candiatesIterator.hasNext())
    {
      double cost = 0;
      String candiate = candiatesIterator.next();
      Site candiateSite = network.getSite(candiate);
      Iterator<Node> childrenIterator = children.iterator();
      while(childrenIterator.hasNext())
      {
        Site child = (Site) childrenIterator.next();
        cost += network.getLinkEnergyCost(child, candiateSite);
      }
      if(network.hasLink(candiateSite, (Site) parent))
      {
        cost += network.getLinkEnergyCost(candiateSite, (Site) parent);
        if(cost < bestCost)
        {
          bestCandiate = candiate;
          bestCost = cost;
        }
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

  /**
   * removes the QEP associated with this logical overlay network
   */
  public void removeQEP()
  {
    this.qep = null;
  }

  /**
   * return a string id for the logical overlay network
   * @return
   */
  public String getId()
  {
    return id;
  }
  
  /**
   * Comparison helper method.
   */
  @Override
  public boolean equals(Object other)
  {
    LogicalOverlayNetworkHierarchy otherNetwork = (LogicalOverlayNetworkHierarchy) other;
    if(otherNetwork.getKeySet().size() == this.getKeySet().size())
    {
      Iterator<String> keyIterator = otherNetwork.getKeySet().iterator();
      while(keyIterator.hasNext())
      {
        String key = keyIterator.next();
        ArrayList<String> otherCandidates = otherNetwork.getEquivilentNodes(key);
        ArrayList<String> candidates = this.getEquivilentNodes(key);
        if(otherCandidates.size() == candidates.size())
        {
          Iterator<String> candidateIterator = otherCandidates.iterator();
          while(candidateIterator.hasNext())
          {
            if(!candidates.contains(candidateIterator.next()))
              return false;
          }
        }
        else
          return false;
      }
    }
    else
      return false;
    return true;
  }
  
  /**
   * iterator of all sites within the logical overlay network, including cluster heads.
   * no particular order of cluster heads, but clusters are output together
   * @return
   */
  public Iterator<String> siteIdIterator()
  {
    ArrayList<String> array = new ArrayList<String>();
    Iterator<String> keyIterator = this.clusters.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      array.add(key);
      Iterator<String> candidates = this.clusters.get(key).iterator();
      while(candidates.hasNext())
      {
        String cand = candidates.next();
        array.add(cand);
      }
    }
    //add root
    array.add(this.qep.getRT().getRoot().getID());
    return array.iterator();
  }

  public boolean isClusterHead(String sourceID)
  {
    return this.clusters.keySet().contains(sourceID);
  }

  /**
   * determines if a node is active in whichever logical node it belongs to
   * @param node
   * @return
   */
  public boolean isActive(Site node)
  {
    ArrayList<String> activeNodes = this.getActiveEquivilentNodes(this.getClusterHeadFor(node.getID()));
    if(activeNodes.contains(node.getID()))
      return true;
    else 
      return false;
  }
  
  /**
   * determines if a node is active in whichever logical node it belongs to
   * @param node
   * @return
   */
  public boolean isActive(String node)
  {
    ArrayList<String> activeNodes = this.getActiveEquivilentNodes(this.getClusterHeadFor(node));
    if(activeNodes.contains(node))
      return true;
    else 
      return false;
  }
  
  /**
   * returns the priority of the node in relation to its siblings within its logical node.
   * (lower value means higher priority)
   * @param NodeID
   * @return
   */
  public int getPriority(String NodeID)
  {
    return this.activeClustersPriority.get(NodeID);
  }

  public ArrayList<String> getNodesWithHigherPriority(String clusterHeadID, int lowestPrioirty)
  {
    ArrayList<String> returnableNodes = new ArrayList<String>();
    Iterator<String> activeNodesFromLogicalNode = this.getActiveEquivilentNodes(clusterHeadID).iterator();
    while(activeNodesFromLogicalNode.hasNext())
    {
      String nodeID = activeNodesFromLogicalNode.next();
      if(this.activeClustersPriority.get(nodeID) >= lowestPrioirty)
        returnableNodes.add(nodeID);
    }
    return returnableNodes;
  }
  
  
  public ArrayList<String> getNodesWithLowerPriority(String clusterHeadID, int lowestPrioirty)
  {
    ArrayList<String> returnableNodes = new ArrayList<String>();
    Iterator<String> activeNodesFromLogicalNode = this.getActiveEquivilentNodes(clusterHeadID).iterator();
    while(activeNodesFromLogicalNode.hasNext())
    {
      String nodeID = activeNodesFromLogicalNode.next();
      if(this.activeClustersPriority.get(nodeID) < lowestPrioirty)
        returnableNodes.add(nodeID);
    }
    return returnableNodes;
  }
  
  public ArrayList<String> getActiveNodesInRankedOrder(String clusterHeadID)
  {
    ArrayList<String> returnableNodes = new ArrayList<String>();
    int priority = 1;
    while(priority <= this.getActiveEquivilentNodes(clusterHeadID).size())
    {
      Iterator<String> activeNodesFromLogicalNode = 
        this.getActiveEquivilentNodes(clusterHeadID).iterator();
      boolean found = false;
      while(activeNodesFromLogicalNode.hasNext() && !found)
      {
        String nodeID = activeNodesFromLogicalNode.next();
        if(this.activeClustersPriority.get(nodeID) == priority)
        {
          returnableNodes.add(nodeID);
          priority++;
          found = true;
        }
      }
      if(!found)
        priority++;
    }
    return returnableNodes;
  }

  /**
   * takes a cluster and updates the priority to allow a cycle in redundant transmissions head.
   * @param ClusterHeadnodeID
   */
  public void updatePriority(String ClusterHeadnodeID)
  {
    Iterator<String> activeNodes = this.activeClusters.get(ClusterHeadnodeID).iterator();    
    while(activeNodes.hasNext())
    {
      String nodeID = activeNodes.next();
      Integer oldValue = this.activeClustersPriority.get(nodeID);
      
      if(oldValue + 1 > this.activeClusters.get(ClusterHeadnodeID).size())
        oldValue = 1;
      else
        oldValue ++;
      
      this.activeClustersPriority.put(nodeID, oldValue);
    }
  }

  public void updateClusters(String failedNodeID, String replacementNodeID)
  {
    if(this.isActive(replacementNodeID))
    {
      if(this.isClusterHead(failedNodeID))
      {
        updateCluster(failedNodeID, replacementNodeID);
        ArrayList<String> cluster = this.activeClusters.get(failedNodeID);
        cluster.remove(failedNodeID);
        this.activeClusters.remove(failedNodeID);
        this.activeClusters.addAll(replacementNodeID, cluster);
        sortOutActivePriorities(failedNodeID, replacementNodeID);
      }
      else
      {
        this.clusters.remove(this.getClusterHeadFor(failedNodeID), failedNodeID);
        this.activeClusters.remove(this.getClusterHeadFor(failedNodeID), failedNodeID);
        sortOutActivePriorities(failedNodeID, replacementNodeID);
      }
    }
    else
    {
      this.activeClusters.add(this.getClusterHeadFor(failedNodeID), replacementNodeID);
      if(this.isClusterHead(failedNodeID))
      {
        updateCluster(failedNodeID, replacementNodeID);
        ArrayList<String> cluster = this.activeClusters.get(failedNodeID);
        cluster.remove(failedNodeID);
        this.activeClusters.remove(failedNodeID);
        this.activeClusters.addAll(replacementNodeID, cluster);
        sortOutPrioties(failedNodeID, replacementNodeID);
      }
      else
      {
        this.activeClusters.remove(this.getClusterHeadFor(failedNodeID), failedNodeID);
        sortOutPrioties(failedNodeID, replacementNodeID);
        this.clusters.remove(this.getClusterHeadFor(failedNodeID), failedNodeID);
      }
    }
  }

  private void sortOutActivePriorities(String failedNodeID, String replacementNodeID)
  {
    int newpriority = 0;
    int failedPriority = this.activeClustersPriority.get(failedNodeID);
    int activePrioirty = this.activeClustersPriority.get(replacementNodeID);
    if(failedPriority < activePrioirty)
      newpriority = failedPriority;
    else
      newpriority = activePrioirty;
    
    int newOriginalpriority = 0;
    failedPriority = this.activeClustersOriginalPriority.get(failedNodeID);
    activePrioirty = this.activeClustersOriginalPriority.get(replacementNodeID);
    if(failedPriority < activePrioirty)
      newOriginalpriority = failedPriority;
    else
      newOriginalpriority = activePrioirty;
    
    this.activeClustersPriority.put(replacementNodeID, newpriority);
    this.activeClustersPriority.remove(failedNodeID);
    this.activeClustersOriginalPriority.put(replacementNodeID, newOriginalpriority);
    this.activeClustersOriginalPriority.remove(failedNodeID);
    //update other nodes
    Iterator<String> otherNodes = 
      this.getNodesWithHigherPriority(this.getClusterHeadFor(replacementNodeID), newpriority).iterator();
    int counter = 1;
    while(otherNodes.hasNext())
    {
      String node = otherNodes.next();
      this.activeClustersPriority.put(node, newpriority + counter);
      counter++;
    }
  }

  private void sortOutPrioties(String failedNodeID, String replacementNodeID)
  {
    this.activeClustersPriority.put(replacementNodeID, this.activeClustersPriority.get(failedNodeID));
    this.activeClustersPriority.remove(failedNodeID);
    this.activeClustersOriginalPriority.put(replacementNodeID, this.activeClustersPriority.get(failedNodeID));
    this.activeClustersOriginalPriority.remove(failedNodeID);
  }
}
