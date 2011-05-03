package uk.ac.manchester.cs.snee.autonomicmanager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import uk.ac.manchester.cs.snee.common.Utils;
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
  private AutonomicManagerAnaylsiserDeadNodeSimulator deadNodeSimulator; 
  private boolean anaylisieCECM = true;
  private String deadSitesList = "";
  private int lastSneeTuplesSize = 0;

  public AutonomicManagerAnaylsis(AutonomicManager autonomicManager)
  {
    manager = autonomicManager;
    deadNodeSimulator = new AutonomicManagerAnaylsiserDeadNodeSimulator();
  }

  public void initiliseCardECM(QueryExecutionPlan qep) 
  {//sets ECMs with correct query execution plan
	  this.qep = (SensorNetworkQueryPlan) qep;
	  cardECM = new CardinalityEstimatedCostModel(qep);
	  deadNodeSimulator.initilise(qep, cardECM);
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
    deadSitesList = deadNodeSimulator.simulateDeadNodes(deadNodes);
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
    deadSitesList = deadNodeSimulator.simulateDeadNodes(numberOfDeadNodes);  
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

  public void anaylsisSNEECard(Map<Integer, Integer> sneeTuples)
  { 
    if(anaylisieCECM)
    {
      if(sneeTuples.size() > 0)
      {
        try
        {
          System.out.println("comparing");
          float cecmEpochCard = getCECMEpochResult();
          float cecmAgendaCard = getCECMAgendaResult();
          float sneeAgendaCard = 0;
          float sneeEpochCard = 0;
          boolean sameValue = true;
          boolean sameEpochValue = false;
          boolean sameAgendaValue = false;
          int epoch = 0;
          sneeEpochCard = sneeTuples.get(epoch);
          
          
          //check all epochs have same values also used to figure if an agenda worth of epochs have arrived
          while(epoch <= (cardECM.getBeta()) && epoch < (sneeTuples.size()))
          {
            float sneeNextEpochCard = sneeTuples.get(epoch);
            if(sneeNextEpochCard != sneeEpochCard)
              sameValue = false;
            epoch ++;
          }
          
          if(epoch >= cardECM.getBeta())//reached an agenda cycle.
          {
            anaylisieCECM = false;
            if(sameValue)
            {
              sneeAgendaCard = sneeEpochCard * cardECM.getBeta();
            }
            
            //compare snee tuples to cost model estimates
            if(cecmEpochCard == sneeEpochCard)
              sameEpochValue = true;
            if(sneeAgendaCard == cecmAgendaCard)
              sameAgendaValue = true;
            
            //append to results file
            String path = Utils.validateFileLocation("results/results.tex");
            BufferedWriter out = new BufferedWriter(new FileWriter(path, true));
            if ( deadSitesList.equals(""))
              deadSitesList = "control";
            if(sameEpochValue && sameAgendaValue)
              out.write("&" + deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "SUCCESS \\\\ \\hline \n");
            else
              out.write("&" + deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "FAIL \\\\ \\hline \n");
            out.flush();
            out.close();
          }
        }
        catch (Exception e)
        {
          e.printStackTrace();
        } 
      }
    }   
  }

  public void anaylsisSNEECard()
  {
    try
    {
      float cecmEpochCard = getCECMEpochResult();
      float cecmAgendaCard = getCECMAgendaResult();
      float sneeAgendaCard = 0;
      float sneeEpochCard = 0;
      boolean sameEpochValue = false;
      boolean sameAgendaValue = false;
      
      if(cecmEpochCard == sneeEpochCard)
        sameEpochValue = true;
      if(sneeAgendaCard == cecmAgendaCard)
        sameAgendaValue = true;
      
      String path = Utils.validateFileLocation("results/results.tex");
      BufferedWriter out = new BufferedWriter(new FileWriter(path, true));
      if ( deadSitesList.equals(""))
        deadSitesList = "control";
      if(sameEpochValue && sameAgendaValue)
        out.write("&" + deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "SUCCESS \\\\ \\hline \n");
      else
        out.write("&" + deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "FAIL \\\\ \\hline \n");
      out.flush();
      out.close();
    }
    catch(Exception e) { }  
  }

  public void queryStarted()
  {
    anaylisieCECM = true;   
  }
}
