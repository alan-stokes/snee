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
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.router.RouterException;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerException;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationCollection;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.FailedNodeStrategyEnum;
import uk.ac.manchester.cs.snee.manager.common.StrategyAbstract;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.completerecompilationstrategy.CompleteReCompilationStrategy;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.localrepairstrategy.LocalRepairStrategy;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.LogicalOverlayStrategy;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.ChoiceAssessorPreferenceEnum;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.energy.SiteEnergyModel;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class Anaylsiser extends AutonomicManagerComponent
{
 
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 3921928737218887964L;
  private AnayliserCostModelAssessor CMA;
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
  public Anaylsiser(AutonomicManagerImpl autonomicManager, 
                    SourceMetadataAbstract _metadata, MetadataManager _metadataManager) 
  throws SNEEConfigurationException
  {
    manager = autonomicManager;
    frameworks = new ArrayList<StrategyAbstract>();
    SetupFailedNodeFrameWorks(_metadataManager);
  }

  private void SetupFailedNodeFrameWorks(MetadataManager _metadataManager) 
  throws SNEEConfigurationException
  {
    boolean createStratgies = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_INITILISE_STRATEGIES);
    if(createStratgies)
    {
      String prop = SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_STRATEGIES);
      if(prop.equals(FailedNodeStrategyEnum.FailedNodeLocal.toString()))
      {
        CompleteReCompilationStrategy failedNodeFrameworkGlobal = 
          new CompleteReCompilationStrategy(manager, _metadata, _metadataManager);
        LogicalOverlayStrategy failedNodeFrameworkLocal = 
          new LogicalOverlayStrategy(manager, _metadata, _metadataManager);
        frameworks.add(failedNodeFrameworkGlobal);
        frameworks.add(failedNodeFrameworkLocal);
        
      }
      if(prop.equals(FailedNodeStrategyEnum.FailedNodePartial.toString()))
      {
        CompleteReCompilationStrategy failedNodeFrameworkGlobal = 
          new CompleteReCompilationStrategy(manager, _metadata, _metadataManager);
        LocalRepairStrategy failedNodeFrameworkSpaceAndTimePinned = 
          new LocalRepairStrategy(manager, _metadata, true, true);
        //FailedNodeStrategyPartial failedNodeFrameworkSpacePinned = 
         // new FailedNodeStrategyPartial(manager, _metadata, true, false);
        frameworks.add(failedNodeFrameworkGlobal);
        frameworks.add(failedNodeFrameworkSpaceAndTimePinned);
      }
      if(prop.equals(FailedNodeStrategyEnum.FailedNodeGlobal.toString()))
      {
        CompleteReCompilationStrategy failedNodeFrameworkGlobal = 
          new CompleteReCompilationStrategy(manager, _metadata, _metadataManager);
        frameworks.add(failedNodeFrameworkGlobal);
      }
      if(prop.equals(FailedNodeStrategyEnum.All.toString()))
      { 
        LocalRepairStrategy failedNodeFrameworkSpaceAndTimePinned = 
          new LocalRepairStrategy(manager, _metadata, true, true);
        //FailedNodeStrategyPartial failedNodeFrameworkSpacePinned = 
        //  new FailedNodeStrategyPartial(manager, _metadata, true, false);
        LogicalOverlayStrategy failedNodeFrameworkLocal = 
          new LogicalOverlayStrategy(manager, _metadata, _metadataManager);
        CompleteReCompilationStrategy failedNodeFrameworkGlobal = 
          new CompleteReCompilationStrategy(manager, _metadata, _metadataManager);
        frameworks.add(failedNodeFrameworkLocal);
        frameworks.add(failedNodeFrameworkSpaceAndTimePinned);
        //frameworks.add(failedNodeFrameworkSpacePinned);
        frameworks.add(failedNodeFrameworkGlobal);     
      }
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
   * @throws SNEEConfigurationException 
   * @throws CodeGenerationException 
   * @throws ClassNotFoundException 
   */
  public void initilise(QueryExecutionPlan qep, Integer noOfTrees) 
  throws 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException, 
  SNEEConfigurationException, CodeGenerationException 
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
   * method to give operating strategies
   */
  public int getOperatingStrategies()
  {
    return frameworks.size();
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
   * over rides manager component so that frameworks can be updated
   */
  public void setQEP(SensorNetworkQueryPlan qep)
  {
    Iterator<StrategyAbstract> frameworkIterator = frameworks.iterator();
    while(frameworkIterator.hasNext())
    {
      StrategyAbstract currentFrameWork = frameworkIterator.next();
      currentFrameWork.setQEP(qep);
    } 
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
   * @throws AutonomicManagerException 
   * @throws RouterException 
   */
  public AdaptationCollection runFailedNodeStragities(ArrayList<String> failedNodes) 
  throws OptimizationException, SchemaMetadataException, 
         TypeMappingException, AgendaException, 
         SNEEException, SNEEConfigurationException, 
         MalformedURLException, MetadataException, 
         UnsupportedAttributeTypeException, SourceMetadataException, 
         TopologyReaderException, SNEEDataSourceException, 
         CostParametersException, SNCBException, SNEECompilerException, 
         NumberFormatException, AutonomicManagerException
  {
  	//create adaptation collection
    AdaptationCollection adapatations = new AdaptationCollection();
  	Iterator<StrategyAbstract> frameworkIterator = frameworks.iterator();
  	String choice = SNEEProperties.getSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE);
  	boolean feasiable = true;
  	//go though methodologyies till located a adapatation.
  	while(frameworkIterator.hasNext() && feasiable)
  	{
  	  StrategyAbstract framework = frameworkIterator.next();
  	  if((framework instanceof CompleteReCompilationStrategy 
  	      && (choice.equals(ChoiceAssessorPreferenceEnum.Global.toString()) || choice.equals(ChoiceAssessorPreferenceEnum.Best.toString()))) 
  	   || (framework instanceof LocalRepairStrategy 
          && (choice.equals(ChoiceAssessorPreferenceEnum.Partial.toString()) || choice.equals(ChoiceAssessorPreferenceEnum.Best.toString()))) 
  	   || (framework instanceof LogicalOverlayStrategy 
          && (choice.equals(ChoiceAssessorPreferenceEnum.Local.toString()) || choice.equals(ChoiceAssessorPreferenceEnum.Best.toString()))) 
       )
  	  {
       List<Adaptation> frameworkOutput = framework.adapt(failedNodes);
  	  if(frameworkOutput.size() == 0 && framework instanceof CompleteReCompilationStrategy)
  	    feasiable = false;
  	  adapatations.addAll(frameworkOutput);
  	  }
  	}
    return adapatations;
  }

  public Double calculateQepRunningCostForSite(Site currentSite) 
  throws OptimizationException, SchemaMetadataException, 
         TypeMappingException, SNEEConfigurationException
  {
    SiteEnergyModel siteModel = new SiteEnergyModel(this.qep.getAgendaIOT());
    return siteModel.getSiteEnergyConsumption(currentSite); // J
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

  /**
   * updates the local strategy with the final 
   * @param finalChoice
   */
  public void updateFrameworks(Adaptation finalChoice)
  {
    Iterator<StrategyAbstract> frameworkIterator = frameworks.iterator();
    while(frameworkIterator.hasNext())
    {
      StrategyAbstract framework = frameworkIterator.next();
      framework.update(finalChoice);
    }
  }

  public LogicalOverlayNetwork getOverlay()
  {
    Iterator<StrategyAbstract> frameworkIterator = frameworks.iterator();
    while(frameworkIterator.hasNext())
    {
      StrategyAbstract framework = frameworkIterator.next();
      if(framework instanceof LogicalOverlayStrategy)
      {
        LogicalOverlayStrategy local = (LogicalOverlayStrategy) framework;
        return local.getLogicalOverlay();
      }
    }
    LogicalOverlayNetwork overlay = new LogicalOverlayNetwork();
    overlay.setQep(qep);
    return overlay;
  }

  public void setupOverlay() 
  throws SchemaMetadataException, TypeMappingException, OptimizationException, 
  IOException, SNEEConfigurationException, CodeGenerationException
  {
    Iterator<StrategyAbstract> frameworkIterator = frameworks.iterator();
    while(frameworkIterator.hasNext())
    {
      StrategyAbstract currentFrameWork = frameworkIterator.next();
      if(currentFrameWork instanceof LogicalOverlayStrategy)
      {
        currentFrameWork.initilise(this.qep, 0);
      }
    }
  }
}
