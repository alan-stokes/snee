package uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.SensorType;
/**
 * class used to compare 2 nodes to test if they are equivalent
 * @author alan
 *
 */

public class LocalClusterSuperEquivalenceRelation implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 6395221901543118715L;

  /**
   * compares two nodes, by checking memory capacity, if the second node is not in the QEP, 
   * the first must be in the QEP, and all connections must be the same.
   * @param first
   * @param second
   * @param qep
   * @return
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  public static boolean isEquivalent(Node first, Node second, SensorNetworkQueryPlan qep, 
                                     Topology network, boolean k_resilence_sense, boolean useDefaultAlts) 
  throws 
  SchemaMetadataException, 
  TypeMappingException, 
  OptimizationException
  {
    //turn nodes into sites
    Site primarySite = (Site) first;
    Site secondarySite = (Site) second;
    //collect sites in qeop
    ArrayList<Integer> siteIdsInQEP = qep.getRT().getSiteIDs();
    //get memories
    Long primarySiteMemoryUsage = memoryRequiredForQEP(primarySite, qep);
    Long secondarySiteMemoryAvilible = secondarySite.getRAM();
    if((first.getID().equals("66") || second.getID().equals("66")) &&
        (first.getID().equals("16") || second.getID().equals("16")))
    {
      System.out.println("");
    }
    
    if(first.getID().equals(qep.getRT().getRoot().getID()))
    	return false;
    //check memory test
    if(secondarySiteMemoryAvilible < primarySiteMemoryUsage)
    {
      return false;
    }
    if(siteIdsInQEP.contains(Integer.parseInt(secondarySite.getID())))
    {
      return false;
    }
    if(!siteIdsInQEP.contains(Integer.parseInt(primarySite.getID())))
    {
     return false;
    }
    if(primarySite.isSource() && k_resilence_sense)
    {
      HashSet<SensorType> primarySensorTypes = primarySite.getSensingCapabilities();
      Iterator<SensorType> secondarySiteSensorTypesIterator = secondarySite.getSensingCapabilities().iterator();
      while(secondarySiteSensorTypesIterator.hasNext())
      {
        if(!primarySensorTypes.contains(secondarySiteSensorTypesIterator.next()))
          return false;
      }
      if(!useDefaultAlts)
      {
        if(!primarySite.getAlterativeSites().contains(secondarySite.getID()))
          return false;
      }
    }
    if(primarySite.isSource() && !k_resilence_sense)
    {
      return false;
    }
    if(!sameConnections(primarySite, secondarySite, network, qep))
    {
      return false;
    }
    return true;
  }
  
  /**
   * used to compare the edges from each node
   * @param primarySite
   * @param secondarySite
   * @param network
   * @param qep 
   * @return
   */
  private static boolean sameConnections(Site primarySite, Site secondarySite,
      Topology network, SensorNetworkQueryPlan qep)
  {
    //gets each nodes edges
    ArrayList<Node> primaryNodes = new ArrayList<Node>();
    primaryNodes.addAll(qep.getRT().getSite(primarySite.getID()).getInputsList());
    primaryNodes.add(qep.getRT().getSite(primarySite.getID()).getOutput(0));
    HashSet<EdgeImplementation> secondaryEdges = network.getNodeEdges(secondarySite.getID());
    Iterator<Node> primaryEdgesIterator = primaryNodes.iterator();
    //goes though primary nodes edges looking for simular one in the secondary nodes set
    boolean overallFound = true;
    while(primaryEdgesIterator.hasNext() && overallFound)
    {
      Node primaryNode = primaryEdgesIterator.next();
      Iterator<EdgeImplementation> secondaryEdgeIterator = secondaryEdges.iterator();
      boolean found = false;
      while(secondaryEdgeIterator.hasNext() && !found)
      {
        EdgeImplementation secondaryEdge = secondaryEdgeIterator.next();
        //checks other set looking for same source id
        if(secondaryEdge.getSourceID().equals(primaryNode.getID())
          || secondaryEdge.getDestID().equals(primaryNode.getID()))
          found = true;
      }
      if(found)
        overallFound = true;
      else
      {
        overallFound = false;
      }
    }
    return overallFound;
  }

  /**
   * helper method to get the memory required for the sites QEP.
   * @param site
   * @param qep
   * @return
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private static Long memoryRequiredForQEP(Site site, SensorNetworkQueryPlan qep) 
  throws 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException
  {
    int totalFragmentDataMemoryCost = 0;
    final Iterator<Fragment> fragments = 
      site.getFragments().iterator();
    while (fragments.hasNext()) 
    {
      final Fragment fragment = fragments.next();
      totalFragmentDataMemoryCost += fragment.getDataMemoryCost(site, qep.getDAF());
    }
    
    int totalExchangeComponentsDataMemoryCost = 0;

    final Iterator<ExchangePart> comps = site.getExchangeComponents().iterator();
    while (comps.hasNext()) 
    {
      final ExchangePart comp = comps.next();
      totalExchangeComponentsDataMemoryCost += comp.getDataMemoryCost(site, qep.getDAF());
    }
    
    return totalFragmentDataMemoryCost + (totalExchangeComponentsDataMemoryCost * qep.getBufferingFactor());
  }
}
