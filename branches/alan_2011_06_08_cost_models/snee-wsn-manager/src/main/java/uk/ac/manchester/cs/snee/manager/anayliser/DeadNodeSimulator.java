package uk.ac.manchester.cs.snee.manager.anayliser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class DeadNodeSimulator
{
  private SensorNetworkQueryPlan qep;
  private CardinalityEstimatedCostModel cardECM;
  
  public DeadNodeSimulator()
  {}
  
  public void initilise(QueryExecutionPlan qep, CardinalityEstimatedCostModel cardECM) 
  {
    this.qep = (SensorNetworkQueryPlan) qep;
    this.cardECM = cardECM;
  }
  
  
  public String simulateDeadNodes(ArrayList<Integer> deadNodes) throws OptimizationException
  {
    Iterator<Integer> nodeIterator = deadNodes.iterator();
    String deadSitesList = "";
    while(nodeIterator.hasNext())
    {
        Integer deadNode = nodeIterator.next();
      cardECM.setSiteDead(deadNode);
      deadSitesList = deadSitesList.concat(deadNode.toString() + " ");
    }
    return deadSitesList;
  }
  
  public String simulateDeadNodes(int numberOfDeadNodes) throws OptimizationException
  {
    ArrayList<Integer> sites = new ArrayList<Integer>();
    ArrayList<Integer> deadSites = new ArrayList<Integer>();
    Random generator = new Random();
    int rootSiteValue = Integer.parseInt(qep.getRT().getRoot().getID());
    int biggestSiteID = qep.getRT().getMaxSiteID();
    for(int siteNo = rootSiteValue; siteNo < biggestSiteID; siteNo++)
    {
      sites.add(siteNo);
    }
    
    for(int deadNodeValue = 0; deadNodeValue < numberOfDeadNodes; deadNodeValue++)
    {
      int indexToSiteToDie = generator.nextInt(sites.size());
      int siteToDie = sites.get(indexToSiteToDie);
      Site toDie = qep.getRT().getSite(siteToDie);
      toDie.setisDead(true);
      sites.remove(indexToSiteToDie);
      deadSites.add(siteToDie);
    }
    
    String deadSitesList = "";
    for(int index = 0; index < deadSites.size(); index++)
    {
      deadSitesList = deadSitesList.concat(deadSites.get(index).toString() + " ");
    }
    return deadSitesList;
  }
}
