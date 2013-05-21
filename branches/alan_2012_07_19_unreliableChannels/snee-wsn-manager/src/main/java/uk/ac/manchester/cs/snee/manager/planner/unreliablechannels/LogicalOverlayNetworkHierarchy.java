package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.LogicalOverlayStrategy;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
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
  
  /**
   * mkae skele of a logicaloverlay netowrk for channel model testing of the original QEP
   * @param logicaloverlayNetwork
   * @param network
   * @param qep
   * @throws NumberFormatException
   * @throws SNEEConfigurationException
   */
  public LogicalOverlayNetworkHierarchy(LogicalOverlayNetworkHierarchy logicaloverlayNetwork, 
                                        SensorNetworkQueryPlan qep, Topology network)
  throws NumberFormatException, SNEEConfigurationException
  {
    this.qep = qep;
    clusters = new HashMapList<String, String>();
    activeClusters = new HashMapList<String, String>();
    this.activeClustersPriority = new HashMap<String, Integer>();
    this.deployment = network;
    Iterator<String> keys = logicaloverlayNetwork.getClusters().keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      this.addClusterNode(key, key);
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
    keys = logicaloverlayNetwork.getClusters().keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      this.activeClusters.add(key, key);
    }
    keys = logicaloverlayNetwork.getClusters().keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      this.activeClustersPriority.put(key, 1);
    }
  }

  public LogicalOverlayNetworkHierarchy(LogicalOverlayNetwork logicalOverlay,
      SensorNetworkQueryPlan qep, Topology wsnTopology) 
  throws NumberFormatException, SNEEConfigurationException
  {
    this.qep = qep;
    clusters = new HashMapList<String, String>();
    activeClusters = new HashMapList<String, String>();
    this.activeClustersPriority = new HashMap<String, Integer>();
    this.deployment = wsnTopology;
    Iterator<String> keys = logicalOverlay.getClusters().keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      this.addClusterNode(key,logicalOverlay.getClusters().get(key));
      
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
    keys = logicalOverlay.getClusters().keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      this.addActiveClusters(key, logicalOverlay.getClusters().get(key));
    }
    keys = logicalOverlay.getClusters().keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      this.activeClustersPriority.put(key, 1);
      Iterator<String> eqivNodeIterator = logicalOverlay.getClusters().get(key).iterator();
      int counter = 2;
      while(eqivNodeIterator.hasNext())
      {
        this.activeClustersPriority.put(eqivNodeIterator.next(),counter);
        counter++;
      }
    } // TODO Auto-generated constructor stub
  }

  private void addActiveClusters(String key, ArrayList<String> equivNodes)
  {
    this.activeClusters.addAll(key, equivNodes);
    if(equivNodes.isEmpty())
    {
      this.activeClusters.add(key, "");
      this.activeClusters.remove(key, "");
    }
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
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  @SuppressWarnings("unchecked")
  private void selectActiveNodes() throws NumberFormatException, SNEEConfigurationException
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
        int leftOverResilience = Integer.parseInt(
            SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_K_ACTIVE_LEVEL));
        while(leftOverResilience > 0)
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

  /**
   * returns nodes within a cluster that are currently inactive
   * @param clusterHeadID
   * @return
   */
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

  /**
   * adds all nodes to a cluster.
   */
  public void addClusterNode(String primary, ArrayList<String> equivilentNodes)
  {
    this.clusters.addAll(primary, equivilentNodes);
    if(equivilentNodes.isEmpty())
    {
      this.clusters.add(primary, "");
      this.clusters.remove(primary, "");
    }
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

  /**
   * returns the set of traditonal cluster heads of this overlay (non active)
   * WARNING. If a cluster has only the clusterhead, this method will return false.
   */
  public Set<String> getKeySet()
  {
    return this.clusters.keySet();
  }

  /**
   * get nodes that are defined as Equivilent to key
   * 
   * @param key
   * @return
   */
  public ArrayList<String> getEquivilentNodes(String key)
  {
    ArrayList<String> cluster = clusters.get(key);
    return cluster;
  }
  
  
  /**
   * returns the set of active nodes within this cluster.
   * @param clusterHead
   * @return
   */
  public ArrayList<String> getActiveEquivilentNodes(String key)
  {
    ArrayList<String> cluster = activeClusters.get(key);
    Set<String> setCluster = new TreeSet<String>();
    setCluster.addAll(cluster);
    cluster.clear();
    Iterator<String> iterator = setCluster.iterator();
    while(iterator.hasNext())
    {
      cluster.add(iterator.next());
    }
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
      activeClusters.remove(key, removal);
    }
  }
  
  /**
   * returns a string repesnetation of this overlay
   */
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
    this.cleanCollection(activeSet);
    ArrayList<String> clusterSet = this.getEquivilentNodes(key);
    this.cleanCollection(clusterSet);
    candiates.addAll(clusterSet);
    Iterator<String> activeSetIterator = activeSet.iterator();
    while(activeSetIterator.hasNext())
    {
      String activeNode = activeSetIterator.next();
      candiates.remove(activeNode);
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

  /**
   * sets a QEP for this overlay
   */
  public void setQep(SensorNetworkQueryPlan qep)
  {
    this.qep = qep;
  }

  /**
   * returns the QEP associated with this overlay
   */
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
    candidates.remove(head);
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
    Iterator<String> keyIterator = this.activeClusters.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      array.add(key);
      Iterator<String> candidates = this.activeClusters.get(key).iterator();
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

  /**
   * overloaded method to determine if a node is in fact a cluster head node
   */
  public boolean isClusterHead(String sourceID)
  {
    return this.activeClusters.keySet().contains(sourceID);
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
    if(!this.activeClustersPriority.keySet().contains(NodeID))
      return 0;
    else
      return this.activeClustersPriority.get(NodeID);
  }

  /**
   * returns the acitve node of a cluster with the highest priority
   * @param clusterHeadID
   * @param lowestPrioirty
   * @return
   */
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
  
  /**
   * returns the node of a cluster which has the lowest priority
   * @param clusterHeadID
   * @param lowestPrioirty
   * @return
   */
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
  
  /**
   * returns the active nodes of the cluster in the order of priority
   * @param clusterHeadID
   * @return
   */
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
        if(nodeID == null || activeClustersPriority.get(nodeID) == null)
          System.out.println();
        if(activeClustersPriority.get(nodeID) == priority)
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

  
  /**
   * Updates all the cluster data structures given a node fialure and its replacement.
   * @param failedNodeID
   * @param replacementNodeID
   */
  public void updateClusters(String failedNodeID, String replacementNodeID)
  {
    removeDuplicates();
    
    if(this.isActive(replacementNodeID))
    {
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
        this.clusters.remove(this.getClusterHeadFor(failedNodeID), failedNodeID);
        this.activeClusters.remove(this.getClusterHeadFor(failedNodeID), failedNodeID);
        sortOutPrioties(failedNodeID, replacementNodeID);
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
    removeDuplicates();
  }

  private void removeDuplicates()
  {
    HashMapList<String, String> newCluster = new HashMapList<String, String>();
    Iterator<String> keySet = this.clusters.keySet().iterator();
    while(keySet.hasNext())
    {
      String key = keySet.next();
      Iterator<String> values = this.clusters.get(key).iterator();
      while(values.hasNext())
      {
        String value = values.next();
        
        if(newCluster.get(key)!= null & !newCluster.get(key).contains(value))
          newCluster.add(key, value);
      }
    }
    this.clusters = newCluster;
  }

  /**
   * resets the original cluster priotiries given a ndoe fialure.
   * @param failedNodeID
   * @param replacementNodeID
   */
  private void sortOutPrioties(String failedNodeID, String replacementNodeID)
  {
    ArrayList<String> clusteredNodes = this.getActiveEquivilentNodes(replacementNodeID);
    //move the replacement to front of list
    if(clusteredNodes.indexOf(replacementNodeID) == -1)
      System.out.println();
    clusteredNodes.remove(clusteredNodes.indexOf(replacementNodeID));
    clusteredNodes.add(0, replacementNodeID);
    
    this.activeClustersPriority.remove(failedNodeID);
    this.activeClustersOriginalPriority.remove(failedNodeID);
    
    //reset priorites so no gap between them, and new replacementNodeID has priority of 1.
    Iterator<String> clusterNodeIterator = clusteredNodes.iterator();
    int newPriority = 1;
    while(clusterNodeIterator.hasNext())
    {
      String nodeID = clusterNodeIterator.next();
      this.activeClustersPriority.put(nodeID, newPriority);
      this.activeClustersOriginalPriority.put(nodeID, newPriority);
      newPriority++;
    }
  }

  /**
   * resets the priorities to the original ones.
   */
  public void resetPriorities()
  {
    this.activeClustersPriority = new HashMap<String, Integer>();
    this.activeClustersPriority.putAll(activeClustersOriginalPriority);
  }

  public void removeClonedData() 
  {
	removeClones(this.activeClusters);
	removeClones(this.clusters);
  }
  
  private ArrayList<String> cleanCollection(ArrayList<String> dirty)
  {
    ArrayList<String> cleaned = new ArrayList<String>();
    Iterator<String> values = dirty.iterator();
    while(values.hasNext()) 
    {
      String value = values.next();
      
      if(!cleaned.contains(value))
        cleaned.add(value);
    }
    return cleaned;
  }
  
  private void removeClones(HashMapList<String, String> clusterToClean)
  {
	  HashMapList<String, String> newCluster = new HashMapList<String, String>();
	  Iterator<String> keySet = clusterToClean.keySet().iterator();
	  while(keySet.hasNext())
	  {
	    String key = keySet.next();
	    Iterator<String> values = clusterToClean.get(key).iterator();
	    while(values.hasNext())
	    {
	      String value = values.next();
	      
	      if(newCluster.get(key)!= null & !newCluster.get(key).contains(value))
	        newCluster.add(key, value);
	    }
	  }
	  clusterToClean = newCluster;
  }

  public boolean canAdapt(String failedSite, RobustSensorNetworkQueryPlan rQEP)
  {
    if(failedSite == null)
      return false;
    else 
      if(isThereACluster(failedSite, rQEP.getLogicalOverlayNetwork()))
          return true;
      else
        return false;
  }

  private boolean isThereACluster(String failedSite,
                                  LogicalOverlayNetworkHierarchy logicalOverlayNetwork)
  {
    ArrayList<String> col = logicalOverlayNetwork.getEquivilentNodes(logicalOverlayNetwork.getClusterHeadFor(failedSite));
    boolean isEmpty = col.isEmpty();
    if(logicalOverlayNetwork.getEquivilentNodes(logicalOverlayNetwork.getClusterHeadFor(failedSite)).size() != 0)
      return true;
    else
      return false;
  }

  public void setDeployment(Topology network)
  {
    this.deployment = network;
  }
  
}
