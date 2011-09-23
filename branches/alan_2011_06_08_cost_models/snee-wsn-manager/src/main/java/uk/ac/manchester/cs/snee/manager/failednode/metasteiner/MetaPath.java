package uk.ac.manchester.cs.snee.manager.failednode.metasteiner;

import java.io.Serializable;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.manager.failednode.alternativerouter.HeuristicSet;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class MetaPath implements Serializable

{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -98999209171463234L;
  
  Path path;
  
  public MetaPath(Path path)
  {
    this.path = path;
  }
  
  /**
   * Calculates cost of traversing this path in relation to the metric per site given in the set. 
   * assumes there's at least 2 nodes in the path
   * @param top
   * @param set
   * @return
   */
  public double getCost(MetaTopology top, HeuristicSet set)
  {
    double totalCost = 0.0;
    Iterator<Site> siteIterator = path.iterator();
    Site firstSite = siteIterator.next();
    while(siteIterator.hasNext())
    {
      Site nextSite = siteIterator.next();
      double currentCost = top.getEdgeCost(firstSite, nextSite, set);
      totalCost += currentCost;
      firstSite = nextSite;
    }
    return totalCost;
  }
  
}
