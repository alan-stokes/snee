package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel;

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
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationUtils;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.LogicalOverlayStrategy;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetworkUtils;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.Model;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.energy.AdaptationEnergyModel;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.energy.SiteEnergyModel;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.energy.SiteOverlayRobustEnergyModel;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.time.TimeModel;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.RobustSensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.UnreliableChannelAgendaUtils;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class RobustChoiceAssessor extends ChoiceAssessor implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 4727493480043344592L;
  
  public RobustChoiceAssessor(SourceMetadataAbstract _metadata, MetadataManager _metadataManager,
                        File outputFolder, Topology network)
  {
    super(_metadata,_metadataManager, outputFolder,  network);
  }
  
  public RobustChoiceAssessor(SourceMetadataAbstract _metadata, MetadataManager _metadataManager, 
      File outputFolder, boolean useCostModelForBinaries, Topology network)
  {
    super(_metadata, _metadataManager, outputFolder, useCostModelForBinaries,network );
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
   * @throws SNEEConfigurationException 
   */
  public void assessChoices(List<Adaptation> choices, HashMap<String, RunTimeSite> runningSites) 
  throws 
  IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  CodeGenerationException, SNEEConfigurationException
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
      timeModel.initilise(imageGenerationFolder, _metadataManager, useModelForBinaries);
      energyModel.initilise(imageGenerationFolder, _metadataManager, runningSites, useModelForBinaries);
      adapt.setTimeCost(timeModel.calculateTimeCost(adapt));
      adapt.setEnergyCost(energyModel.calculateEnergyCost(adapt));
      adapt.setLifetimeEstimate(this.calculateEstimatedLifetime(adapt));
      adapt.setRuntimeCost(calculateEnergyQEPExecutionCost());
      File adaptFolder = new File(imageGenerationFolder.toString() + sep + adapt.getOverallID());
      if(!adaptFolder.exists())
        adaptFolder.mkdir();
      new ChoiceAssessorUtils(this.runningSites, adapt.getNewQep().getRT())
          .exportWithEnergies(imageGenerationFolder.toString() + sep + adapt.getOverallID() + sep + "routingTreewithEnergies", "");
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
   * @throws SNEEConfigurationException 
   */
  private Double calculateEstimatedLifetime(Adaptation adapt) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, SNEEConfigurationException
  {
    double shortestLifetime = Double.MAX_VALUE; //s
    SiteEnergyModel siteModel = new SiteEnergyModel(adapt.getNewQep().getAgendaIOT());
    Iterator<Site> siteIter = 
                   adapt.getNewQep().getIOT().getRT().siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();
      RunTimeSite rSite = runningSites.get(site.getID());
      double currentEnergySupply = rSite.getCurrentEnergy() - rSite.getCurrentAdaptationEnergyCost();
      double siteEnergyCons = siteModel.getSiteEnergyConsumption(site); // J
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
    try{
      System.out.println("creating folders");
    AssessmentFolder = new File(outputFolder.toString() + sep + "assessment");
    AssessmentFolder.mkdir();
    imageGenerationFolder = new File(AssessmentFolder.toString() + sep + "Adaptations");
    imageGenerationFolder.mkdir();
    timeModel = new TimeModel(imageGenerator);
    energyModel = new AdaptationEnergyModel(imageGenerator);
    System.out.println("updating sites");
    this.runningSites = runningSites;
    System.out.println("reset sites");
    resetRunningSitesAdaptCost();
    System.out.println("adapt = original");
    Adaptation adapt = orginal;
    System.out.println("tmodel initilise");
    if(timeModel == null)
      System.out.println();
    timeModel.initilise(imageGenerationFolder, _metadataManager, true);
    System.out.println("emnodel initilise");
    energyModel.initilise(imageGenerationFolder, _metadataManager, runningSites, true);
    System.out.println("set time");
    adapt.setTimeCost(timeModel.calculateTimeCost(adapt));
    System.out.println("set energy");
    adapt.setEnergyCost(energyModel.calculateEnergyCost(adapt));
    System.out.println("set lifetime");
    adapt.setLifetimeEstimate(this.calculateEstimatedLifetime(adapt));
    System.out.println("set runtime");
    adapt.setRuntimeCost(calculateEnergyQEPExecutionCost());
    System.out.println("if reset");
    if(reset)
    {
      System.out.println("reset");
      resetRunningSitesAdaptCost(); 
    }
    }catch(Exception e)
    {
      e.printStackTrace();
    }
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
                                  LogicalOverlayStrategy failedNodeStrategyLocal,
                                  int networkSize) 
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
    adapt.setLifetimeEstimate(this.calculateEstimatedLifetimeOverlay(adapt, current, failedNodeStrategyLocal, networkSize));
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
      LogicalOverlayStrategy failedNodeStrategyLocal, int networkSize)
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, SNEEConfigurationException, FileNotFoundException, IOException
  {
    double shortestLifetime = Double.MAX_VALUE; //s
    ArrayList<String> globalFailedNodes = new ArrayList<String>();
    
    double overallShortestLifetime = 0;
    boolean adapted = true;
    RobustSensorNetworkQueryPlan rQEP = (RobustSensorNetworkQueryPlan) adapt.getNewQep();
    double agendaLength = Agenda.bmsToMs( rQEP.getUnreliableAgenda().getLength_bms(false))/new Double(1000); // ms to s
    
    new LogicalOverlayNetworkUtils().storeOverlayAsFile(current, outputFolder);
    while(adapted)
    {
      SiteOverlayRobustEnergyModel siteModel = 
        new SiteOverlayRobustEnergyModel(rQEP.getUnreliableAgenda(), current, networkSize, globalFailedNodes);
      
      Iterator<Node> siteIter = this.network.siteIterator();
      
      String failedSite = null;
      while (siteIter.hasNext()) 
      {
        Node site = siteIter.next();
        RunTimeSite rSite = runningSites.get(site.getID());
        double currentEnergySupply = rSite.getCurrentEnergy() - rSite.getCurrentAdaptationEnergyCost();
        double siteEnergyCons = siteModel.getSiteEnergyConsumption((Site) site); // J
        runningSites.get(site.getID()).setQepExecutionCost(siteEnergyCons);
        adapt.putSiteEnergyCost(site.getID(), siteEnergyCons);
        double siteLifetime = (currentEnergySupply / siteEnergyCons);
        boolean useAcquires = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_SENSE);
        //uncomment out sections to not take the root site into account
        if (!site.getID().equals(adapt.getNewQep().getIOT().getRT().getRoot().getID()) &&
           ((useAcquires) ||  (!useAcquires && !((Site) site).isSource())) &&
           !globalFailedNodes.contains(site.getID())) 
        { 
          if(shortestLifetime > siteLifetime)
          {
            if(!((Site) site).isDeadInSimulation())
            {
              shortestLifetime = siteLifetime;
              failedSite = site.getID();
            }
          }
        }
      }
      new ChoiceAssessorUtils(current, runningSites, adapt.getNewQep().getRT()).exportWithEnergies(
                      AssessmentFolder.toString() + sep + "Node" + failedSite , null);
      globalFailedNodes.add(failedSite);
      new UnreliableChannelAgendaUtils(rQEP.getUnreliableAgenda(), rQEP.getLogicalOverlayNetwork().getQep().getIOT(), false)
      .generateImage(outputFolder.toString(), "with" + (globalFailedNodes.size() -1) + " failures");
      if(failedNodeStrategyLocal.canAdapt(failedSite, current))
      {
        ArrayList<String> failedNodeIDs = new ArrayList<String>();
        failedNodeIDs.add(failedSite);
        System.out.println("node " + failedSite);
        List<Adaptation> result = failedNodeStrategyLocal.adapt(failedNodeIDs, current);
        failedNodeStrategyLocal.update(result.get(0), current);
        adapt.setNewQep(result.get(0).getNewQep());
        current.setQep(result.get(0).getNewQep());
        updateSitesEnergyLevels(shortestLifetime, adapt);
        overallShortestLifetime += (shortestLifetime * agendaLength);
        shortestLifetime =  Double.MAX_VALUE;
      }
      else
      {
        System.out.println("node " + failedSite);
        adapted = false;
        updateSitesEnergyLevels(shortestLifetime, adapt);
        overallShortestLifetime += (shortestLifetime * agendaLength);
        LogicalOverlayNetwork oldCurrent = new LogicalOverlayNetworkUtils().retrieveOverlayFromFile(outputFolder, current.getId());
        oldCurrent.setFinalRunningSites(runningSites);
        new LogicalOverlayNetworkUtils().storeOverlayAsFile(oldCurrent, outputFolder);
      }
    }
    return overallShortestLifetime;
   
  }
  
  /**
   * removes the amount of energy off the runtime sites for the lifetime of the shortest node
   * @param shortestLifetime
   * @param adapt 
   */
  private void updateSitesEnergyLevels(double shortestLifetime, Adaptation adapt)
  {
    Iterator<Node> siteIter = this.network.siteIterator();
    while (siteIter.hasNext()) 
    {
      Node site = siteIter.next();
      RunTimeSite rSite = runningSites.get(site.getID());
      rSite.removeDefinedCost(rSite.getQepExecutionCost() * shortestLifetime);
    }
  }
}
