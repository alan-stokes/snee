package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts.AvroraCostExpressions;
import uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts.AvroraCostParameters;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationUtils;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.TemporalAdjustment;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCB;
import uk.ac.manchester.cs.snee.sncb.TinyOS_SNCB_Controller;

public class ChoiceAssessor
{
  private String sep = System.getProperty("file.separator");
  private File AssessmentFolder;
  private MetadataManager _metadataManager;
  private File outputFolder;
  private File imageGenerationFolder;
  private boolean underSpareTime;
  private SNCB imageGenerator = null;
  private boolean compiledAlready = false;
  private HashMap<String, RunTimeSite> runningSites;
  
  public ChoiceAssessor(SourceMetadataAbstract _metadata, MetadataManager _metadataManager,
                        File outputFolder)
  {
    this._metadataManager = _metadataManager;
    this.outputFolder = outputFolder;
    this.imageGenerator = new TinyOS_SNCB_Controller();
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
      adapt.setTimeCost(this.calculateTimeCost(adapt));
      adapt.setEnergyCost(this.calculateEnergyCost(adapt));
      adapt.setLifetimeEstimate(this.calculateEstimatedLifetime(adapt));
      adapt.setRuntimeCost(calculateEnergyQEPExecutionCost());
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
    Iterator<String> siteIDIterator = runningSites.keySet().iterator();
    while(siteIDIterator.hasNext())
    {
      String siteID = siteIDIterator.next();
      runningSites.get(siteID).resetEnergyCosts();
    }
    compiledAlready = false;
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
      if (site!=adapt.getNewQep().getIOT().getRT().getRoot()) 
      {
        RunTimeSite rSite = runningSites.get(site.getID());
        double currentEnergySupply = rSite.getCurrentEnergy() - rSite.getCurrentAdaptationEnergyCost();
        double siteEnergyCons = adapt.getNewQep().getAgendaIOT().getSiteEnergyConsumption(site); // J
        runningSites.get(site.getID()).setQepExecutionCost(siteEnergyCons);
        double agendaLength = Agenda.bmsToMs(adapt.getNewQep().getAgendaIOT().getLength_bms(false))/1000.0; // ms to s
        //double energyConsumptionRate = siteEnergyCons/agendaLength; // J/s
        //double siteLifetime = siteEnergySupply / energyConsumptionRate; //s
        double siteLifetime = (currentEnergySupply / siteEnergyCons) * agendaLength;
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

  /**
   * calcuates how mnay hops are needed to get data from the sink to the reprogrammed node
   * @param adapt
   * @param site
   * @return
   */
  private int calculateNumberOfHops(Adaptation adapt, String site, boolean deactivatedNodesChecking)
  {
    RT routingTree = null;
    if(!deactivatedNodesChecking)
      routingTree = adapt.getNewQep().getRT();
    else
      routingTree = adapt.getOldQep().getRT();
    
    Site sink = routingTree.getRoot();
    //checking not jumping unpon itself
    if(sink.getID().equals(site))
      return 0;
    //find path between the two nodes
    Path path = routingTree.getPath(site, sink.getID());
    //if no path return 0
    if(path.getNodes().length == 1)
      return 0;
    else
      return path.getNodes().length -1;
  }

  /**
   * calls the sncb to genreate the nesc code images, and then the site QEP is assessed.
   * @return
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  private Long calculateNumberOfPacketsForSiteQEP(Adaptation adapt, String reprogrammedSite) 
  throws 
  IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    File adaptFolder = new File(imageGenerationFolder.toString() + sep + adapt.getOverallID());
    if(!compiledAlready)
    {
      //create folder for this adaptation 
      adaptFolder.mkdir();
      imageGenerator.generateNesCCode(adapt.getNewQep(), adaptFolder.toString() + sep, this._metadataManager);
      imageGenerator.compileNesCCode(adaptFolder.toString()+ sep);
      compiledAlready = true;
    }
    File moteQEP = new File(adaptFolder.toString() + sep + "avrora_mica2_t2" + sep + "mote" + reprogrammedSite + ".elf");
    Long fileSize = moteQEP.length();
    CostParameters parameters = _metadataManager.getCostParameters();
    int packetSize = parameters.getDeliverPayloadSize();
    Long packets = fileSize / packetSize;
    return packets;
  }

  /**
   * Method which determines the energy cost of making the adaptation.
   * @param adapt
   * @return returns the cost in energy units.
   */
  private double calculateEnergyCost(Adaptation adapt)
  throws IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    CostParameters parameters = _metadataManager.getCostParameters();
    //do each reprogrammed site (most expensive cost)
    Iterator<String> reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    while(reporgrammedSitesIterator.hasNext())
    {
      String reprogrammedSite = reporgrammedSitesIterator.next();
      Long packets = calculateNumberOfPacketsForSiteQEP(adapt, reprogrammedSite);
      calculateEnergyCostForDataHops(adapt, reprogrammedSite, packets);
      
      //energy usage of the reprogrammed site by flash writing
      //voltage * getCycles(mode) * ampere[mode] * cycleTime;
      double costPerByteWritten = AvroraCostParameters.VOLTAGE * AvroraCostParameters.FlashWRITECYCLES * 
              AvroraCostParameters.CYCLETIME * AvroraCostParameters.FlashWRITEAMPERE;
      runningSites.get(reprogrammedSite).addToCurrentAdaptationEnergyCost((packets * parameters.getDeliverPayloadSize() * costPerByteWritten));
    }
    
    //do for each of redirect, deact, act site
    Iterator<String> redirectedSiteIterator = adapt.redirectedionSitesIterator();
    Iterator<String> deactivatedSiteIterator = adapt.deactivationSitesIterator();
    Iterator<String> activatedSiteIterator = adapt.activateSitesIterator();
    
    calculateEnergyOnePacketCost(redirectedSiteIterator, adapt, false);
    calculateEnergyOnePacketCost(deactivatedSiteIterator, adapt, true);
    calculateEnergyOnePacketCost(activatedSiteIterator, adapt, false);
    
    //do for temporal adjustment
    Iterator<TemporalAdjustment> temporalSiteIterator = adapt.temporalSitesIterator();
    while(temporalSiteIterator.hasNext())
    {
      TemporalAdjustment adjustment = temporalSiteIterator.next();
      Iterator<String> affectedsitesIterator = adjustment.affectedsitesIterator();
      calculateEnergyOnePacketCost(affectedsitesIterator, adapt, false);
    }
    if(!underSpareTime)
      calculateEnergyCostOfRunningStartStopCommand(adapt.getNewQep().getRT());
    return calculateEnergyOverallAdaptationCost();
  }
  
  /**
   * used to determine the energy cost of calling start and stop on each node
   * @param rt
   */
  private void calculateEnergyCostOfRunningStartStopCommand(RT rt)
  {
    // TODO calculate each energy cost
    
  }

  /**
   * goes though the entire running sites, looking for cost of adaptation.
   * @return
   */
  private double calculateEnergyOverallAdaptationCost()
  {
    Iterator<String> siteIDIterator = runningSites.keySet().iterator();
    double overallCost = 0;
    while(siteIDIterator.hasNext())
    {
      String siteID = siteIDIterator.next();
      overallCost += runningSites.get(siteID).getCurrentAdaptationEnergyCost();
      
    }
    return overallCost;
  }

  /**
   * method to calculate cost of each hop adn place cost onto running sites adpatation cost counter
   * @param adapt
   * @param reprogrammedSite
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private void calculateEnergyCostForDataHops(Adaptation adapt, String reprogrammedSite, Long packets) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    RT routingTree = adapt.getNewQep().getRT();
    CostParameters parameters = _metadataManager.getCostParameters();
    Site sink = routingTree.getRoot();
    Path path = routingTree.getPath(reprogrammedSite, sink.getID());
    Iterator<Site> sitesInPath = path.iterator();
    if(path.getNodes().length != 1)
    {
      Site dest = sitesInPath.next();
      while(sitesInPath.hasNext())
      {
        Site source = sitesInPath.next();
        CommunicationTask sourceTask = new CommunicationTask(new Long(0), dest, source, CommunicationTask.TRANSMIT, packets, parameters);
        CommunicationTask destTask = new CommunicationTask(new Long(0), dest, source, CommunicationTask.RECEIVE, packets, parameters);
        double sourceCost = adapt.getNewQep().getAgendaIOT().evaluateCommunicationTask(sourceTask, packets);
        double destCost = adapt.getNewQep().getAgendaIOT().evaluateCommunicationTask(destTask, packets);
        runningSites.get(source.getID()).addToCurrentAdaptationEnergyCost(sourceCost);
        runningSites.get(dest.getID()).addToCurrentAdaptationEnergyCost(destCost);
        dest = source;
      }
    }
  }

/**
   * Method which determines the time cost of an adaptation
   * @param adapt
   * @return
   */
  private Long calculateTimeCost(Adaptation adapt)
  throws 
  IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    CostParameters parameters = _metadataManager.getCostParameters();
    Double timeTakesSoFar = new Double(0);
    
    //do for type of adaptation
    Iterator<String> reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    Iterator<String> redirectedSiteIterator = adapt.redirectedionSitesIterator();
    Iterator<String> deactivatedSiteIterator = adapt.deactivationSitesIterator();
    Iterator<String> activatedSiteIterator = adapt.activateSitesIterator();
    
    timeTakesSoFar += calculateTimePacketsCost(redirectedSiteIterator, parameters, adapt, new Long(1), false);
    timeTakesSoFar += calculateTimePacketsCost(deactivatedSiteIterator, parameters, adapt, new Long(1), true);
    timeTakesSoFar += calculateTimePacketsCost(activatedSiteIterator,  parameters, adapt, new Long(1), false);
    timeTakesSoFar += calculateTimePacketsCost(reporgrammedSitesIterator,  parameters, adapt, null, false);
    
    //do for temporal adjustment
    Iterator<TemporalAdjustment> temporalSiteIterator = adapt.temporalSitesIterator();
    while(temporalSiteIterator.hasNext())
    {
      TemporalAdjustment adjustment = temporalSiteIterator.next();
      Iterator<String> affectedsitesIterator = adjustment.affectedsitesIterator();
      timeTakesSoFar += calculateTimePacketsCost(affectedsitesIterator, parameters, adapt, new Long(1), false);
    }
    
    //do time for reprogramming 
    reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    while(reporgrammedSitesIterator.hasNext())
    {
      timeTakesSoFar += calculateTimePerReprogram(reporgrammedSitesIterator.next(), adapt);
    }
    
    
    long goldenFrame = calculateGoldenTimeFrame(adapt.getOldQep().getAgendaIOT());
    if(timeTakesSoFar > goldenFrame)
      underSpareTime = false;
    else
      underSpareTime = true;
    if(!underSpareTime)
      timeTakesSoFar += calculateTimeStartStopAddition(adapt.getNewQep().getRT());
    return timeTakesSoFar.longValue();//goes though each reprogrammable node and cal
  }
  
  /**
   * calculates how lnog the reprogramming step takes
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  private double calculateTimePerReprogram(String site, Adaptation adapt) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    double packets = calculateNumberOfPacketsForSiteQEP(adapt, site);
    return packets * AvroraCostParameters.FlashWRITECYCLES * 100;
  }

  /**
   * used to determine how long the start and stop command will take to run.
   * @param rt
   * @return
   */
  private Long calculateTimeStartStopAddition(RT rt)
  {
    // TODO calculate time and energy cost for start / stop commands
    /**
     * pulled from the sncb python scripts, the stop takes a 10 second delay 
     * and the start a 30 second delay
     */
    return new Long(0);
   // return new Long(40000);
  }

  /**
   * method used to calculate the golden time frame
   * @param agendaIOT
   * @return
   */
  private long calculateGoldenTimeFrame(AgendaIOT agendaIOT)
  {
    Long deliveryTime = agendaIOT.getDeliveryTime_ms();
    Long agendaAcquisitionTime = agendaIOT.getAcquisitionInterval_ms();
    Long agendaBufferingFactor = agendaIOT.getBufferingFactor();
    Long agendaExecutionTime = agendaAcquisitionTime*agendaBufferingFactor;
    return agendaExecutionTime - deliveryTime;
  }

  /**
   * calculates the time cost of sending one packet down to a node 
   * (used for redircet, deact, and act adaptations) if no packets, then calculates the packet size
   * @param redirectedSiteIterator
   * @param parameters
   * @param adapt
   * @param packets
   * @return
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  private Long calculateTimePacketsCost(Iterator<String> redirectedSiteIterator,
                                 CostParameters parameters, Adaptation adapt,
                                 Long packets, boolean deactivatedNodesChecking) 
  throws 
  IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    Long time = new Long(0);
    while(redirectedSiteIterator.hasNext())
    {
      String site = redirectedSiteIterator.next();
      int hops = calculateNumberOfHops(adapt, site, deactivatedNodesChecking);
      long timePerHop = (long) Math.ceil(parameters.getCallMethod() + parameters.getSignalEvent() + 
          parameters.getTurnOnRadio()+ parameters.getRadioSyncWindow() * 2
          + parameters.getTurnOffRadio());
      if(packets == null)
      {
        packets = calculateNumberOfPacketsForSiteQEP(adapt, site);
      }
      Long packetTime = (long) Math.ceil(parameters.getSendPacket() * packets);
      time += packetTime * hops * timePerHop;
    }
    return time;
  }
  
  /**
   * calculates the energy cost of sending one packet down to a node
   * @param redirectedSiteIterator
   * @param parameters
   * @param adapt
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private void calculateEnergyOnePacketCost(Iterator<String> redirectedSiteIterator, Adaptation adapt, 
                                       boolean deactivedNodes) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException
  {
    
    RT routingTree;
    if(!deactivedNodes)
      routingTree = adapt.getNewQep().getRT();
    else
      routingTree= adapt.getOldQep().getRT();
    CostParameters parameters = _metadataManager.getCostParameters();
    Site sink = routingTree.getRoot();
    while(redirectedSiteIterator.hasNext())
    {
      String maindest = redirectedSiteIterator.next();
      Path path = routingTree.getPath(maindest, sink.getID());
      Iterator<Site> sitesInPath = path.iterator();
      if(path.getNodes().length != 0)
      {
        Site dest = sitesInPath.next();
        while(sitesInPath.hasNext())
        {
          Site source = sitesInPath.next();
          
          CommunicationTask sourceTask = new CommunicationTask(new Long(0),  dest, source, CommunicationTask.TRANSMIT, new Long(1), parameters);
          CommunicationTask destTask = new CommunicationTask(new Long(0), dest, source, CommunicationTask.RECEIVE, new Long(1), parameters);
          double sourceCost;
          double destCost;
          
          if(!deactivedNodes)
          {
            sourceCost = adapt.getNewQep().getAgendaIOT().evaluateCommunicationTask(sourceTask, new Long(1));
            destCost = adapt.getNewQep().getAgendaIOT().evaluateCommunicationTask(destTask, new Long(1));
          }
          else
          {
            sourceCost = adapt.getOldQep().getAgendaIOT().evaluateCommunicationTask(sourceTask, new Long(1));
            destCost = adapt.getOldQep().getAgendaIOT().evaluateCommunicationTask(sourceTask, new Long(1));
          }
          runningSites.get(dest.getID()).addToCurrentAdaptationEnergyCost(destCost);
          runningSites.get(source.getID()).addToCurrentAdaptationEnergyCost(sourceCost);
          dest = source;
        }
      }
    }
  }

  public void updateStorageLocation(File outputFolder)
  {
   this.outputFolder = outputFolder;
    
  }

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
    adapt.setTimeCost(this.calculateTimeCost(adapt));
    adapt.setEnergyCost(this.calculateEnergyCost(adapt));
    adapt.setLifetimeEstimate(this.calculateEstimatedLifetime(adapt));
    adapt.setRuntimeCost(calculateEnergyQEPExecutionCost());
    if(reset)
      resetRunningSitesAdaptCost(); 
  }
}
