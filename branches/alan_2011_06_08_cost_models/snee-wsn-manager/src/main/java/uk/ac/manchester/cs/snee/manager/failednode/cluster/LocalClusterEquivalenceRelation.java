package uk.ac.manchester.cs.snee.manager.failednode.cluster;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
/**
 * class used to compare 2 nodes to test if they are equivalent
 * @author alan
 *
 */

public class LocalClusterEquivalenceRelation
{
  /**
   * compares two nodes, by checking memory capacity, if the second node is not in the QEP, 
   * the first must be in the QEP, and all connections must be the same.
   * @param first
   * @param second
   * @param qep
   * @return
   */
  public static boolean isEquivalent(Node first, Node second, SensorNetworkQueryPlan qep)
  {
    Site primarySite = (Site) first;
    Site secondarySite = (Site) second;
    Long primarySiteMemoryUsage = memoryRequiredForQEP(primarySite, qep);
    Long secondarySiteMemoryAvilible = secondarySite.getRAM();
    //check memory test
    if(secondarySiteMemoryAvilible > primarySiteMemoryUsage)
    {
      
    }
    return false;
  }
  
  /**
   * helper method to get the memory required for the sites QEP.
   * @param site
   * @param qep
   * @return
   */
  private static Long memoryRequiredForQEP(Site site, SensorNetworkQueryPlan qep)
  {
    //TODO FIND WAY TO FIGURE HOW MUCH MEMROY NEEDED FOR SITE QEP
    return (long) 0;
  }
  
}
