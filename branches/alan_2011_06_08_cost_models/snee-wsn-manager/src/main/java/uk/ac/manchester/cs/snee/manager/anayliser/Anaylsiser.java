package uk.ac.manchester.cs.snee.manager.anayliser;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.Adaptation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.StrategyAbstract;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyGlobal;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyLocal;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyPartial;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class Anaylsiser 
{
 
  private AnayliserCostModelAssessor CMA;
  private SensorNetworkQueryPlan qep;
  private AutonomicManager manager;
  private boolean anaylisieCECM = true;
  private String deadSitesList = "";  
  ArrayList<StrategyAbstract> frameworks;

  public Anaylsiser(AutonomicManager autonomicManager, 
                    SourceMetadataAbstract _metadata, MetadataManager _metadataManager)
  {
    manager = autonomicManager;
    frameworks = new ArrayList<StrategyAbstract>();
    FailedNodeStrategyPartial failedNodeFrameworkSpaceAndTimePinned = 
      new FailedNodeStrategyPartial(manager, _metadata, true, true);
    FailedNodeStrategyPartial failedNodeFrameworkSpacePinned = 
      new FailedNodeStrategyPartial(manager, _metadata, true, false);
    FailedNodeStrategyLocal failedNodeFrameworkLocal = 
      new FailedNodeStrategyLocal(manager, _metadata);
    FailedNodeStrategyGlobal failedNodeFrameworkGlobal = 
      new FailedNodeStrategyGlobal(manager, _metadata, _metadataManager);
    //add methodologies in order wished to be assessed
    frameworks.add(failedNodeFrameworkLocal);
    frameworks.add(failedNodeFrameworkSpaceAndTimePinned);
    //frameworks.add(failedNodeFrameworkSpacePinned);
    frameworks.add(failedNodeFrameworkGlobal);
  }

  public void initilise(QueryExecutionPlan qep, Integer noOfTrees) 
  throws 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException 
  {//sets ECMs with correct query execution plan
	  this.qep = (SensorNetworkQueryPlan) qep;
	  this.CMA = new AnayliserCostModelAssessor(qep);
	  Iterator<StrategyAbstract> frameworkIterator = frameworks.iterator();
	  while(frameworkIterator.hasNext())
	  {
	    StrategyAbstract currentFrameWork = frameworkIterator.next();
	    currentFrameWork.initilise(qep, noOfTrees);
	  } 
  }
   
  public void runECMs() 
  throws OptimizationException 
  {//runs ecms
	  runCardECM();
  }
  
  public void runCardECM() 
  throws OptimizationException
  {
    CMA.runCardinalityCostModel();
  }
  
  public void simulateDeadNodes(ArrayList<Integer> deadNodes) 
  throws OptimizationException
  {
    CMA.simulateDeadNodes(deadNodes, deadSitesList);
  }
  
  /**
   * chooses nodes to simulate to fail
   * @param numberOfDeadNodes
 * @throws OptimizationException 
   */
  public void simulateDeadNodes(int numberOfDeadNodes) 
  throws OptimizationException
  { 
    CMA.simulateDeadNodes(numberOfDeadNodes, deadSitesList);
  }
  
  public float getCECMEpochResult() 
  throws OptimizationException
  {
    return CMA.returnEpochResult();
  }
  
  public float getCECMAgendaResult() throws OptimizationException
  {
    return CMA.returnAgendaExecutionResult();
  }

  public void anaylsisSNEECard(Map<Integer, Integer> sneeTuples)
  { 
    CMA.anaylsisSNEECard(sneeTuples, anaylisieCECM, deadSitesList);
  }

  public void anaylsisSNEECard()
  {
    CMA.anaylsisSNEECard(deadSitesList);
  }

  public void queryStarted()
  {
    anaylisieCECM = true;   
  }
  
  public List<Adaptation> runFailedNodeFramework(ArrayList<String> failedNodes) 
  throws OptimizationException, SchemaMetadataException, 
         TypeMappingException, AgendaException, 
         SNEEException, SNEEConfigurationException, 
         MalformedURLException, MetadataException, 
         UnsupportedAttributeTypeException, SourceMetadataException, 
         TopologyReaderException, SNEEDataSourceException, 
         CostParametersException, SNCBException, SNEECompilerException, 
         NumberFormatException
  {
  	//create adaparatation array
  	List<Adaptation> adapatations = new ArrayList<Adaptation>();
  	Iterator<StrategyAbstract> frameworkIterator = frameworks.iterator();
  	//go though methodologyies till located a adapatation.
  	while(frameworkIterator.hasNext())
  	{
  	  StrategyAbstract framework = frameworkIterator.next();
  	  if(framework.canAdaptToAll(failedNodes))
  	    adapatations.addAll(framework.adapt(failedNodes));
  	  else
  	  {
  	    if(framework instanceof FailedNodeStrategyLocal)
  	    {
  	      checkEachFailureIndividually(failedNodes, adapatations);
  	    }
  	  }
  	}
    
    //output adapatations in a String format
    Iterator<Adaptation> adapatationIterator = adapatations.iterator();
    return adapatations;
  }

  private void checkEachFailureIndividually(ArrayList<String> failedNodes,
      List<Adaptation> adapatations)
  {
    /*can't adapt to them all, so check to see if any are adpatable, 
    if so, remove them from next frameworks scope as local framework safer*/
    Iterator<String> failedNodeIterator = failedNodes.iterator();
    while(failedNodeIterator.hasNext())
    {
      String failedNodeID = failedNodeIterator.next();
      //TODO CHECK EACH FAILED NODE IN LOCAL, BEFORE SENDING TO PARTIAL
    }
  }
}
