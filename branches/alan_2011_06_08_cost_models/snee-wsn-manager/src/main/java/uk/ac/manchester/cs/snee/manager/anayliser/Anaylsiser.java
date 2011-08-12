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
import uk.ac.manchester.cs.snee.manager.Adapatation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.FrameWorkAbstract;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeFrameWorkGlobal;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeFrameWorkLocal;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeFrameWorkPartial;
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
  ArrayList<FrameWorkAbstract> frameworks;

  public Anaylsiser(AutonomicManager autonomicManager, 
                    SourceMetadataAbstract _metadata, MetadataManager _metadataManager)
  {
    manager = autonomicManager;
    frameworks = new ArrayList<FrameWorkAbstract>();
    FailedNodeFrameWorkPartial failedNodeFrameworkSpaceAndTimePinned = 
      new FailedNodeFrameWorkPartial(manager, _metadata, true, true);
    FailedNodeFrameWorkPartial failedNodeFrameworkSpacePinned = 
      new FailedNodeFrameWorkPartial(manager, _metadata, true, false);
    FailedNodeFrameWorkLocal failedNodeFrameworkLocal = 
      new FailedNodeFrameWorkLocal(manager, _metadata);
    FailedNodeFrameWorkGlobal failedNodeFrameworkGlobal = 
      new FailedNodeFrameWorkGlobal(manager, _metadata, _metadataManager);
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
	  Iterator<FrameWorkAbstract> frameworkIterator = frameworks.iterator();
	  while(frameworkIterator.hasNext())
	  {
	    FrameWorkAbstract currentFrameWork = frameworkIterator.next();
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
  
  public List<Adapatation> runFailedNodeFramework(ArrayList<String> failedNodes) 
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
  	List<Adapatation> adapatations = new ArrayList<Adapatation>();
  	Iterator<FrameWorkAbstract> frameworkIterator = frameworks.iterator();
  	//go though methodologyies till located a adapatation.
  	while(frameworkIterator.hasNext())
  	{
  	  FrameWorkAbstract framework = frameworkIterator.next();
  	  if(framework.canAdaptToAll(failedNodes))
  	    adapatations.addAll(framework.adapt(failedNodes));
  	  else
  	  {
  	    if(framework instanceof FailedNodeFrameWorkLocal)
  	    {
  	      checkEachFailureIndividually(failedNodes, adapatations);
  	    }
  	  }
  	}
    
    //output adapatations in a String format
    Iterator<Adapatation> adapatationIterator = adapatations.iterator();
    return adapatations;
  }

  private void checkEachFailureIndividually(ArrayList<String> failedNodes,
      List<Adapatation> adapatations)
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
