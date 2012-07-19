package uk.ac.manchester.cs.snee.manager.anayliser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Map;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;

public class AnayliserCostModelAssessor
{
  private CardinalityEstimatedCostModel cardECM;
  private DeadNodeSimulator deadNodeSimulator; 
  
  
  public AnayliserCostModelAssessor(QueryExecutionPlan qep)
  {
    cardECM = new CardinalityEstimatedCostModel(qep);
    deadNodeSimulator = new DeadNodeSimulator();
    deadNodeSimulator.initilise(qep, cardECM); 
  }

  public CardinalityEstimatedCostModel getCardinalityCostModel()
  {
    return cardECM;
  }

  public void runCardinalityCostModel() 
  throws OptimizationException
  {
  //runs ecm and outputs result to terminal
    cardECM.runModel();
    float epochResult = cardECM.returnEpochResult();
    float agendaCycleResult = cardECM.returnAgendaExecutionResult();
      
    System.out.println("the cardinality of this query for epoch cycle is " + epochResult);
    System.out.println("the cardinality of this query for agenda cycle is " + agendaCycleResult);
  }

  public void simulateDeadNodes(ArrayList<String> deadNodes, String deadSitesList) 
  throws OptimizationException
  {
    deadSitesList = deadNodeSimulator.simulateDeadNodes(deadNodes);
    cardECM.runModel();
    float epochResult = cardECM.returnEpochResult();
    float agendaResult = cardECM.returnAgendaExecutionResult();
      
    System.out.println("Test with node " + deadSitesList + "death, cardianlity of the query per epoch is estimated to be " + epochResult);
    System.out.println("Test with node " + deadSitesList + "death, cardianlity of the query per agenda cycle is estimated to be " + agendaResult);
  }



  public void simulateDeadNodes(int numberOfDeadNodes, String deadSitesList) 
  throws OptimizationException
  {
    deadSitesList = deadNodeSimulator.simulateDeadNodes(numberOfDeadNodes);  
    cardECM.runModel();
    float epochResult = cardECM.returnEpochResult();
    float agendaResult = cardECM.returnAgendaExecutionResult();
    
    System.out.println("Test with node " + deadSitesList + "death, cardianlity of the query per epoch is estimated to be " + epochResult);
    System.out.println("Test with node " + deadSitesList + "death, cardianlity of the query per agenda cycle is estimated to be " + agendaResult);
    
  }

  public float returnEpochResult() 
  throws OptimizationException
  {
    return cardECM.returnEpochResult();
  }

  public float returnAgendaExecutionResult() 
  throws OptimizationException
  {
    return cardECM.returnAgendaExecutionResult();
  }

  public void anaylsisSNEECard(Map<Integer, Integer> sneeTuples, 
                               boolean anaylisieCECM, String deadSitesList)
  {
    if(anaylisieCECM)
    {
      if(sneeTuples.size() > 0)
      {
        try
        {
          System.out.println("comparing");
          float cecmEpochCard = returnEpochResult();
          float cecmAgendaCard = returnAgendaExecutionResult();
          float sneeAgendaCard = 0;
          float sneeEpochCard = 0;
          boolean sameValue = true;
          boolean sameEpochValue = false;
          boolean sameAgendaValue = false;
          int epoch = 0;
          sneeEpochCard = sneeTuples.get(epoch);
          
          
          //check all epochs have same values also used to figure if an agenda worth of epochs have 
          //arrived
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
              out.write(deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "Y \\\\ \\hline \n");
            else
              out.write(deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "N \\\\ \\hline \n");
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

  public void anaylsisSNEECard(String deadSitesList)
  {
    try
    {
      float cecmEpochCard = returnEpochResult();
      float cecmAgendaCard = returnAgendaExecutionResult();
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
        out.write(deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "Y \\\\ \\hline \n");
      else
        out.write(deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "N \\\\ \\hline \n");
      out.flush();
      out.close();
    }
    catch(Exception e) { }    
  }
  
  
  
}
