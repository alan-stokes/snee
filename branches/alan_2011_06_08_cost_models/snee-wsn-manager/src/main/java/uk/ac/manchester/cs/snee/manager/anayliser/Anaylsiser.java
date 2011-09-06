package uk.ac.manchester.cs.snee.manager.anayliser;

import java.io.File;
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
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.StrategyAbstract;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategy;
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
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
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

  /**
   * constructor
   * @param autonomicManager
   * @param _metadata
   * @param _metadataManager
   * @throws SNEEConfigurationException 
   */
  public Anaylsiser(AutonomicManager autonomicManager, 
                    SourceMetadataAbstract _metadata, MetadataManager _metadataManager) 
  throws SNEEConfigurationException
  {
    manager = autonomicManager;
    frameworks = new ArrayList<StrategyAbstract>();
    String prop = SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_STRATEGIES);
    if(prop.equals(FailedNodeStrategy.FailedNodeLocal.toString()))
    {
      FailedNodeStrategyGlobal failedNodeFrameworkGlobal = 
        new FailedNodeStrategyGlobal(manager, _metadata, _metadataManager);
      FailedNodeStrategyLocal failedNodeFrameworkLocal = 
        new FailedNodeStrategyLocal(manager, _metadata);
      frameworks.add(failedNodeFrameworkGlobal);
      frameworks.add(failedNodeFrameworkLocal);
    }
    if(prop.equals(FailedNodeStrategy.FailedNodePartial.toString()))
    {
      FailedNodeStrategyGlobal failedNodeFrameworkGlobal = 
        new FailedNodeStrategyGlobal(manager, _metadata, _metadataManager);
      FailedNodeStrategyPartial failedNodeFrameworkSpaceAndTimePinned = 
        new FailedNodeStrategyPartial(manager, _metadata, true, true);
      FailedNodeStrategyPartial failedNodeFrameworkSpacePinned = 
        new FailedNodeStrategyPartial(manager, _metadata, true, false);
      frameworks.add(failedNodeFrameworkGlobal);
      frameworks.add(failedNodeFrameworkSpaceAndTimePinned);
    }
    if(prop.equals(FailedNodeStrategy.FailedNodeGlobal.toString()))
    {
      FailedNodeStrategyGlobal failedNodeFrameworkGlobal = 
        new FailedNodeStrategyGlobal(manager, _metadata, _metadataManager);
      frameworks.add(failedNodeFrameworkGlobal);
    }
    if(prop.equals(FailedNodeStrategy.All.toString()))
    { 
      FailedNodeStrategyPartial failedNodeFrameworkSpaceAndTimePinned = 
        new FailedNodeStrategyPartial(manager, _metadata, true, true);
      FailedNodeStrategyPartial failedNodeFrameworkSpacePinned = 
        new FailedNodeStrategyPartial(manager, _metadata, true, false);
      FailedNodeStrategyLocal failedNodeFrameworkLocal = 
        new FailedNodeStrategyLocal(manager, _metadata);
      FailedNodeStrategyGlobal failedNodeFrameworkGlobal = 
        new FailedNodeStrategyGlobal(manager, _metadata, _metadataManager);
      frameworks.add(failedNodeFrameworkLocal);
      frameworks.add(failedNodeFrameworkSpaceAndTimePinned);
      //frameworks.add(failedNodeFrameworkSpacePinned);
      frameworks.add(failedNodeFrameworkGlobal);     
    }
  }

  /**
   * initilisier
   * @param qep
   * @param noOfTrees
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws IOException
   */
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
   
  /**
   * run estimate models models
   * @throws OptimizationException
   */
  public void runECMs() 
  throws OptimizationException 
  {//runs ecms
	  runCardECM();
  }
  
  /**
   * run cardianlity cost model
   * @throws OptimizationException
   */
  public void runCardECM() 
  throws OptimizationException
  {
    CMA.runCardinalityCostModel();
  }
  
  /**
   * run simulation of failed nodes
   * @param deadNodes
   * @throws OptimizationException
   */
  public void simulateDeadNodes(ArrayList<String> deadNodes) 
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
  
  /**
   * take estimated epoch cardinality
   * @return
   * @throws OptimizationException
   */
  public float getCECMEpochResult() 
  throws OptimizationException
  {
    return CMA.returnEpochResult();
  }
  /**
   * take agenda estimated cardinality.
   * @return
   * @throws OptimizationException
   */
  public float getCECMAgendaResult() throws OptimizationException
  {
    return CMA.returnAgendaExecutionResult();
  }

  /**
   * takes results and compares them against models. Also updates runnign energy models
   * @param sneeTuples
   */
  public void anaylsisSNEECard(Map<Integer, Integer> sneeTuples)
  { 
    CMA.anaylsisSNEECard(sneeTuples, anaylisieCECM, deadSitesList);
  }

  /**
   * takes results and compares them against models. Also updates runnign energy models
   * @param sneeTuples
   */
  public void anaylsisSNEECard()
  {
    CMA.anaylsisSNEECard(deadSitesList);
  }

  /**
   * tells anayslier to expect tuples
   */
  public void queryStarted()
  {
    anaylisieCECM = true;   
  }
  
  /**
   * method to run failed node strageties
   * @param failedNodes
   * @return
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws AgendaException
   * @throws SNEEException
   * @throws SNEEConfigurationException
   * @throws MalformedURLException
   * @throws MetadataException
   * @throws UnsupportedAttributeTypeException
   * @throws SourceMetadataException
   * @throws TopologyReaderException
   * @throws SNEEDataSourceException
   * @throws CostParametersException
   * @throws SNCBException
   * @throws SNEECompilerException
   * @throws NumberFormatException
   */
  public List<Adaptation> runFailedNodeStragities(ArrayList<String> failedNodes) 
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
  	boolean feasiable = true;
  	//go though methodologyies till located a adapatation.
  	while(frameworkIterator.hasNext() && feasiable)
  	{
  	  StrategyAbstract framework = frameworkIterator.next();
  	  if(framework.canAdaptToAll(failedNodes))
  	  {
  	    List<Adaptation> frameworkOutput = framework.adapt(failedNodes);
  	    if(frameworkOutput.size() == 0 && framework instanceof FailedNodeStrategyGlobal)
  	      feasiable = false;
  	    adapatations.addAll(frameworkOutput);
  	  }
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

  /**
   * method which runs each framework seperately
   * @param failedNodes
   * @param adapatations
   */
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

  public Double calculateQepRunningCostForSite(Site currentSite) 
  throws OptimizationException, SchemaMetadataException, 
         TypeMappingException
  {
    return this.qep.getAgendaIOT().getSiteEnergyConsumption(currentSite); // J
  }

  public void updateFrameWorkStorageLocation(File outputFolder)
  {
    Iterator<StrategyAbstract> frameworkIterator = this.frameworks.iterator();    
    while(frameworkIterator.hasNext())
    {
      StrategyAbstract framework = frameworkIterator.next();
      framework.updateFrameWorkStorage(outputFolder);
    }
  }

}
