package uk.ac.manchester.cs.snee.manager.failednode.cluster;

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
/**
 * class used to compare 2 nodes to test if they are equivalent
 * @author alan
 *
 */

public class LocalClusterEquivalenceRelation implements Serializable
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
                                     Topology network) 
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
    
    //set to true, but then if any test passes, set to false
    boolean success = true;
    //check memory test
    if(secondarySiteMemoryAvilible < primarySiteMemoryUsage)
    {
      success = false;
    }
    if(siteIdsInQEP.contains(Integer.parseInt(secondarySite.getID())))
    {
      success = false;
    }
    if(!siteIdsInQEP.contains(Integer.parseInt(primarySite.getID())))
    {
      success = false;
    }
    if(primarySite.isSource())
    {
      success = false;
    }
    if(!sameConnections(primarySite, secondarySite, network))
    {
      success = false; 
    }
    return success;
  }
  
  /**
   * used to compare the edges from each node
   * @param primarySite
   * @param secondarySite
   * @param network
   * @return
   */
  private static boolean sameConnections(Site primarySite, Site secondarySite,
      Topology network)
  {
    //gets each nodes edges
    HashSet<EdgeImplementation> primaryEdges = network.getNodeEdges(primarySite.getID());
    HashSet<EdgeImplementation> secondaryEdges = network.getNodeEdges(secondarySite.getID());
    Iterator<EdgeImplementation> primaryEdgesIterator = primaryEdges.iterator();
    //goes though primary nodes edges looking for simular one in the secondary nodes set
    boolean overallFound = true;
    while(primaryEdgesIterator.hasNext() && overallFound)
    {
      EdgeImplementation primaryEdge = primaryEdgesIterator.next();
      if(primaryEdge.getDestID().equals(primarySite.getID()))
      {
        Iterator<EdgeImplementation> secondaryEdgeIterator = secondaryEdges.iterator();
        boolean found = false;
        while(secondaryEdgeIterator.hasNext() && !found)
        {
          EdgeImplementation secondaryEdge = secondaryEdgeIterator.next();
          //checks other set looking for same source id
          if(secondaryEdge.getSourceID().equals(primaryEdge.getSourceID()))
            found = true;
        }
        if(found)
          overallFound = true;
        else
        {
          overallFound = false;
        }
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
