package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationUtils;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyLocal;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LogicalOverlayNetworkUtils;
import uk.ac.manchester.cs.snee.manager.planner.model.EnergyModel;
import uk.ac.manchester.cs.snee.manager.planner.model.EnergyModelOverlay;
import uk.ac.manchester.cs.snee.manager.planner.model.Model;
import uk.ac.manchester.cs.snee.manager.planner.model.TimeModel;
import uk.ac.manchester.cs.snee.manager.planner.model.TimeModelOverlay;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCB;
import uk.ac.manchester.cs.snee.sncb.TinyOS_SNCB_Controller;

public class ChoiceAssessor implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 4727493480043344592L;
  
  private String sep = System.getProperty("file.separator");
  private File AssessmentFolder;
  private MetadataManager _metadataManager;
  private File outputFolder;
  private File imageGenerationFolder;
  private SNCB imageGenerator = null;
  private HashMap<String, RunTimeSite> runningSites;
  private EnergyModel energyModel = null;
  private TimeModel timeModel = null;
  private TimeModelOverlay timeModelOverlay = null;
  private EnergyModelOverlay energyModelOverlay = null;
  
  public ChoiceAssessor(SourceMetadataAbstract _metadata, MetadataManager _metadataManager,
                        File outputFolder)
  {
    this._metadataManager = _metadataManager;
    this.outputFolder = outputFolder;
    this.imageGenerator = new TinyOS_SNCB_Controller();
    timeModel = new TimeModel(imageGenerator);
    energyModel = new EnergyModel(imageGenerator);
    timeModelOverlay = new TimeModelOverlay(imageGenerator);
    energyModelOverlay = new EnergyModelOverlay(imageGenerator);
  }

  /**
   * checks all constraints to executing the changes and locates the best choice 
   * @param choices
 * @param runningSites 
   * @return
   * @throws IOException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws CodeGenerationException 
   */
  public void assessChoices(List<Adaptation> choices, HashMap<String, RunTimeSite> runningSites) 
  throws 
  IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  CodeGenerationException
  {
    AssessmentFolder = new File(outputFolder.toString() + sep + "assessment");
    AssessmentFolder.mkdir();
    imageGenerationFolder = new File(AssessmentFolder.toString() + sep + "Adaptations");
    imageGenerationFolder.mkdir();
    
    this.runningSites = runningSites;
    System.out.println("Starting assessment of choices");
    Iterator<Adaptation> choiceIterator = choices.iterator();
    while(choiceIterator.hasNext())
    {
      resetRunningSitesAdaptCost();
      Adaptation adapt = choiceIterator.next();
      timeModel.initilise(imageGenerationFolder, _metadataManager);
      energyModel.initilise(imageGenerationFolder, _metadataManager, runningSites);
      adapt.setTimeCost(timeModel.calculateTimeCost(adapt));
      adapt.setEnergyCost(energyModel.calculateEnergyCost(adapt));
      adapt.setLifetimeEstimate(this.calculateEstimatedLifetime(adapt));
      adapt.setRuntimeCost(calculateEnergyQEPExecutionCost());
      File adaptFolder = new File(imageGenerationFolder.toString() + sep + adapt.getOverallID());
      if(!adaptFolder.exists())
        adaptFolder.mkdir();
      new ChoiceAssessorUtils(this.runningSites, adapt.getNewQep().getRT())
          .exportRTWithEnergies(imageGenerationFolder.toString() + sep + adapt.getOverallID() + sep + "routingTreewithEnergies", "");
      resetRunningSitesAdaptCost(); 
    }
    new AdaptationUtils(choices, _metadataManager.getCostParameters()).FileOutput(AssessmentFolder);
  }
  
  
  /**
   * calcualtes overall energy cost of executing the qep for an agenda
   * @return
   */
  private Double calculateEnergyQEPExecutionCost()
  {
    Iterator<String> siteIDIterator = runningSites.keySet().iterator();
    double overallCost = 0;
    while(siteIDIterator.hasNext())
    {
      String siteID = siteIDIterator.next();
      overallCost += runningSites.get(siteID).getQepExecutionCost();
    }
    return overallCost;
  }

  /**
   * resets the energy tracker for each running site
   */
  private void resetRunningSitesAdaptCost()
  {
    resetRunningSitesAdaptCost(true);
  }
  

  private void resetRunningSitesAdaptCost(boolean resetCompiled)
  {
    Iterator<String> siteIDIterator = runningSites.keySet().iterator();
    while(siteIDIterator.hasNext())
    {
      String siteID = siteIDIterator.next();
      runningSites.get(siteID).resetAdaptEnergyCosts();
    }
    if(resetCompiled)
      Model.setCompiledAlready(false);
  }

  /**
   * method to determine the estimated lifetime of the new QEP
   * @param adapt
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private Double calculateEstimatedLifetime(Adaptation adapt) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException
  {
    double shortestLifetime = Double.MAX_VALUE; //s
    
    Iterator<Site> siteIter = 
                   adapt.getNewQep().getIOT().getRT().siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();
      RunTimeSite rSite = runningSites.get(site.getID());
      double currentEnergySupply = rSite.getCurrentEnergy() - rSite.getCurrentAdaptationEnergyCost();
      double siteEnergyCons = adapt.getNewQep().getAgendaIOT().getSiteEnergyConsumption(site); // J
      runningSites.get(site.getID()).setQepExecutionCost(siteEnergyCons);
      adapt.putSiteEnergyCost(site.getID(), siteEnergyCons);
      double agendaLength = Agenda.bmsToMs(adapt.getNewQep().getAgendaIOT().getLength_bms(false))/new Double(1000); // ms to s
      double siteLifetime = (currentEnergySupply / siteEnergyCons) * agendaLength;
      //uncomment out sections to not take the root site into account
      if (site!=adapt.getNewQep().getIOT().getRT().getRoot()) 
      { 
        if(shortestLifetime > siteLifetime)
        {
          if(!site.isDeadInSimulation())
          {
            shortestLifetime = siteLifetime;
            adapt.setNodeIdWhichEndsQuery(site.getID());
          }
        }
      }
    }
    return shortestLifetime;
  }

  public void updateStorageLocation(File outputFolder)
  {
   this.outputFolder = outputFolder;
    
  }

  /**
   * assesses an adaptation storing all results within the adaptation
   * @param orginal
   * @param runningSites
   * @param reset
   * @throws IOException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws CodeGenerationException
   */
  public void assessChoice(Adaptation orginal, HashMap<String, RunTimeSite> runningSites, boolean reset)
  throws 
  IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  CodeGenerationException
  {
    AssessmentFolder = new File(outputFolder.toString() + sep + "assessment");
    AssessmentFolder.mkdir();
    imageGenerationFolder = new File(AssessmentFolder.toString() + sep + "Adaptations");
    imageGenerationFolder.mkdir();
    
    this.runningSites = runningSites;
    resetRunningSitesAdaptCost();
    Adaptation adapt = orginal;
    timeModel.initilise(imageGenerationFolder, _metadataManager);
    energyModel.initilise(imageGenerationFolder, _metadataManager, runningSites);
    adapt.setTimeCost(timeModel.calculateTimeCost(adapt));
    adapt.setEnergyCost(energyModel.calculateEnergyCost(adapt));
    adapt.setLifetimeEstimate(this.calculateEstimatedLifetime(adapt));
    adapt.setRuntimeCost(calculateEnergyQEPExecutionCost());
    if(reset)
      resetRunningSitesAdaptCost(); 
  }

  /**
   * calculates the costs of setting up this overlay network. All results are stored within the 
   * associated adaptation object
   * @param overlayOTAProgramCost
   * @param runningSites
   * @param current
   * @param failedNodeStrategyLocal 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws CodeGenerationException 
   * @throws IOException 
   * @throws SNEEConfigurationException 
   */
  public void assessOverlayChoice(Adaptation overlayOTAProgramCost,
                                  HashMap<String, RunTimeSite> runningSites, 
                                  LogicalOverlayNetwork current,
                                  FailedNodeStrategyLocal failedNodeStrategyLocal) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, IOException, CodeGenerationException, SNEEConfigurationException
  {
    AssessmentFolder = new File(outputFolder.toString() + sep + "assessment");
    AssessmentFolder.mkdir();
    imageGenerationFolder = new File(AssessmentFolder.toString() + sep + "Adaptations");
    imageGenerationFolder.mkdir();
    
    this.runningSites = runningSites;
    resetRunningSitesAdaptCost(false);
    Adaptation adapt = overlayOTAProgramCost;    
    timeModelOverlay.initilise(imageGenerationFolder, _metadataManager);
    energyModelOverlay.initilise(imageGenerationFolder, _metadataManager, runningSites);
    adapt.setTimeCost(timeModelOverlay.calculateTimeCost(adapt, current));
    adapt.setEnergyCost(energyModelOverlay.calculateEnergyCost(adapt, current));
    adapt.setLifetimeEstimate(this.calculateEstimatedLifetimeOverlay(adapt, current, failedNodeStrategyLocal));
    adapt.setRuntimeCost(calculateEnergyQEPExecutionCost());
  }

  /**
   * Calculates the minimal lifetime for the query over the overlay. (first overlay failure)
   * @param adapt
   * @param current
   * @param failedNodeStrategyLocal 
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws IOException 
   * @throws FileNotFoundException 
   */
  private Double calculateEstimatedLifetimeOverlay(Adaptation adapt, LogicalOverlayNetwork current,
                                                   FailedNodeStrategyLocal failedNodeStrategyLocal) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, SNEEConfigurationException, FileNotFoundException, IOException
  {
    double shortestLifetime = Double.MAX_VALUE; //s
    double overallShortestLifetime = 0;
    new LogicalOverlayNetworkUtils().storeOverlayAsFile(current, outputFolder);
    boolean adapted = true;
    while(adapted)
    {
      Iterator<Site> siteIter = 
        current.getQep().getIOT().getRT().siteIterator(TraversalOrder.POST_ORDER);
      String failedSite = null;
      while (siteIter.hasNext()) 
      {
        Site site = siteIter.next();
        System.out.println("testing for site " + site.getID());
        RunTimeSite rSite = runningSites.get(site.getID());
        double currentEnergySupply = rSite.getCurrentEnergy() - rSite.getCurrentAdaptationEnergyCost();
        double siteEnergyCons =  current.getQep().getAgendaIOT().getSiteEnergyConsumption(site); // J
        runningSites.get(site.getID()).setQepExecutionCost(siteEnergyCons);
        adapt.putSiteEnergyCost(site.getID(), siteEnergyCons);
        double agendaLength = Agenda.bmsToMs(current.getQep().getAgendaIOT().getLength_bms(false))/new Double(1000); // ms to s
        double siteLifetime = (currentEnergySupply / siteEnergyCons) * agendaLength;
        
        boolean useAcquires = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_SENSE);
        //uncomment out sections to not take the root site into account
        if (site!= current.getQep().getIOT().getRT().getRoot() &&
            ((useAcquires) ||  (!useAcquires && !site.isSource()))) 
        { 
          if(shortestLifetime > siteLifetime)
          {
            if(!site.isDeadInSimulation())
            {
              shortestLifetime = siteLifetime;
              failedSite = site.getID();
            }
          }
        }
      }
      if(failedNodeStrategyLocal.canAdapt(failedSite, current))
      {
        ArrayList<String> failedNodeIDs = new ArrayList<String>();
        failedNodeIDs.add(failedSite);
        List<Adaptation> result = failedNodeStrategyLocal.adapt(failedNodeIDs, current);
        failedNodeStrategyLocal.update(result.get(0), current);
        current.setQep(result.get(0).getNewQep());
        overallShortestLifetime += shortestLifetime;
      }
      else
      {
        adapted = false;
        overallShortestLifetime += shortestLifetime;
      }
    }
    return overallShortestLifetime;
  }
}
