package uk.ac.manchester.cs.snee.autonomicmanager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class AutonomicManagerAnaylsis 
{
  private CardinalityEstimatedCostModel cardECM;
  private SensorNetworkQueryPlan qep;
  private AutonomicManager manager;

  public AutonomicManagerAnaylsis(AutonomicManager autonomicManager)
  {
    manager = autonomicManager;
  }

  public void initiliseCardECM(QueryExecutionPlan qep) 
  {//sets ECMs with correct query execution plan
	this.qep = (SensorNetworkQueryPlan) qep;
	cardECM = new CardinalityEstimatedCostModel(qep);
  }

  public void anaylsisSNEECard(float sneeTuplesPerEpoch, float sneeTuplesPerAgendaCycle)
  {
    try
    {
      float cecmEpochCard = getCECMEpochResult();
      float cecmAgendaCard = getCECMAgendaResult();
    } 
    catch (OptimizationException e)
    {
      e.printStackTrace();
    }
  }
  
  
  public void runECMs() throws OptimizationException 
  {//runs ecms
	  runCardECM();
  }
  
  public void runCardECM() throws OptimizationException
  {//runs ecm and outputs result to terminal
  	cardECM.runModel();
  	float epochResult = cardECM.returnEpochResult();
  	float agendaCycleResult = cardECM.returnAgendaExecutionResult();
  		
  	System.out.println("the cardinality of this query for epoch cycle is " + epochResult);
  	System.out.println("the cardinality of this query for agenda cycle is " + agendaCycleResult);
  }
  
  /**
   * sets all nodes in deadNodes to dead for simulation
   * @param deadNodes
 * @throws OptimizationException 
   */
  public void simulateDeadNodes(ArrayList<Integer> deadNodes) throws OptimizationException
  {
  	Iterator<Integer> nodeIterator = deadNodes.iterator();
  	String deadSitesList = "";
  	while(nodeIterator.hasNext())
  	{
        Integer deadNode = nodeIterator.next();
  	  cardECM.setSiteDead(deadNode);
  	  deadSitesList = deadSitesList.concat(deadNode.toString() + " ");
  	}
  	
  	cardECM.runModel();
  	float epochResult = cardECM.returnEpochResult();
  	float agendaResult = cardECM.returnAgendaExecutionResult();
  	  
  	System.out.println("Test with node " + deadSitesList + "death, cardianlity of the query per epoch is estimated to be " + epochResult);
  	System.out.println("Test with node " + deadSitesList + "death, cardianlity of the query per agenda cycle is estimated to be " + agendaResult);
  }
  
  /**
   * assumes root site has the lowest node value
   * @param numberOfDeadNodes
 * @throws OptimizationException 
   */
  public void simulateDeadNodes(int numberOfDeadNodes) throws OptimizationException
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
	  
	  cardECM.runModel();
	  float epochResult = cardECM.returnEpochResult();
	  float agendaResult = cardECM.returnAgendaExecutionResult();
	  
	  System.out.println("Test with node " + deadSitesList + "death, cardianlity of the query per epoch is estimated to be " + epochResult);
	  System.out.println("Test with node " + deadSitesList + "death, cardianlity of the query per agenda cycle is estimated to be " + agendaResult);
  }
  
  public float getCECMEpochResult() throws OptimizationException
  {
    return cardECM.returnEpochResult();
  }
  
  public float getCECMAgendaResult() throws OptimizationException
  {
    return cardECM.returnAgendaExecutionResult();
  }
}
