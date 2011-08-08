package uk.ac.manchester.cs.snee.manager.anayliser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.Adapatation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeFrameWorkLocal;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeFrameWorkPartial;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class Anaylsiser 
{
  private CardinalityEstimatedCostModel cardECM;
  private SensorNetworkQueryPlan qep;
  private AutonomicManager manager;
  private DeadNodeSimulator deadNodeSimulator; 
  private boolean anaylisieCECM = true;
  private String deadSitesList = "";  
  ArrayList<FrameWork> frameworks;

  public Anaylsiser(AutonomicManager autonomicManager)
  {
    manager = autonomicManager;
    frameworks = new ArrayList<FrameWork>();
    FailedNodeFrameWorkPartial failedNodeFrameworkSpaceAndTimePinned = new FailedNodeFrameWorkPartial(manager, true, true);
    FailedNodeFrameWorkPartial failedNodeFrameworkSpacePinned = new FailedNodeFrameWorkPartial(manager, true, false);
    FailedNodeFrameWorkLocal failedNodeFrameworkLocal = new FailedNodeFrameWorkLocal(manager);
    //add methodologies in order wished to be assessed
    frameworks.add(failedNodeFrameworkLocal);
    frameworks.add(failedNodeFrameworkSpaceAndTimePinned);
    frameworks.add(failedNodeFrameworkSpacePinned);
    deadNodeSimulator = new DeadNodeSimulator();
  }

  public void initilise(QueryExecutionPlan qep, Integer noOfTrees) throws SchemaMetadataException 
  {//sets ECMs with correct query execution plan
	  this.qep = (SensorNetworkQueryPlan) qep;
	  cardECM = new CardinalityEstimatedCostModel(qep);
	  Iterator<FrameWork> frameworkIterator = frameworks.iterator();
	  while(frameworkIterator.hasNext())
	  {
	    FrameWork currentFrameWork = frameworkIterator.next();
	    currentFrameWork.initilise(qep, noOfTrees);
	  }
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
        out.write(deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "Y \\\\ \\hline \n");
      else
        out.write(deadSitesList + "&" + cecmEpochCard + "&" + cecmAgendaCard + "&" + sneeEpochCard + "&" + sneeAgendaCard + "&" + "N \\\\ \\hline \n");
      out.flush();
      out.close();
    }
    catch(Exception e) { }  
  }

  public void queryStarted()
  {
    anaylisieCECM = true;   
  }
  
  public SensorNetworkQueryPlan adapatationStrategyIntermediateSpaceAndTimePinned(ArrayList<String> failedNodes) 
  throws OptimizationException, 
         SchemaMetadataException, 
         TypeMappingException, 
         AgendaException, SNEEException, SNEEConfigurationException, MalformedURLException, WhenSchedulerException, MetadataException, UnsupportedAttributeTypeException, SourceMetadataException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException
  {
  	//create adaparatation array
  	ArrayList<Adapatation> adapatations = new ArrayList<Adapatation>();
  	Iterator<FrameWork> frameworkIterator = frameworks.iterator();
  	//go though methodologyies till located a adapatation.
  	while(adapatations.size() == 0 && frameworkIterator.hasNext())
  	{
  	  FrameWork framework = frameworkIterator.next();
  	  if(framework.canAdaptToAll(failedNodes))
  	    adapatations = framework.adapt(failedNodes);
  	  else
  	  {
  	    //can't adapt to them all, so check to see if any are adpatable, if so, remove them from next frameworks scope
  	    Iterator<String> failedNodeIterator = failedNodes.iterator();
  	    while(failedNodeIterator.hasNext())
  	    {
  	      //TODO CHECK EACH FAILED NODE IN LOCAL, BEFORE SENDING TO PARTIAL
  	    }
  	  }
  	}
    
    //output adapatations in a String format
    Iterator<Adapatation> adapatationIterator = adapatations.iterator();
    while(adapatationIterator.hasNext())
    {
      System.out.println(adapatationIterator.next().toString());
    }
    return null;
  }
}
