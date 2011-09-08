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
      adapt.setTimeCost(this.timeCost(adapt));
      adapt.setEnergyCost(this.energyCost(adapt));
      adapt.setLifetimeEstimate(this.estimatedLifetime(adapt));
      adapt.setRuntimeCost(calculateQEPExecutionEnergyCost());
      resetRunningSitesAdaptCost();  
    }
    new AdaptationUtils(choices, _metadataManager.getCostParameters()).FileOutput(AssessmentFolder);
  }
  
  
  /**
   * calcualtes overall energy cost of executing the qep for an agenda
   * @return
   */
  private Double calculateQEPExecutionEnergyCost()
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
  private Double estimatedLifetime(Adaptation adapt) 
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
        double currentEnergySupply = runningSites.get(site.getID()).getCurrentEnergy() - 
                                     runningSites.get(site.getID()).getCurrentAdaptationEnergyCost();
        double siteEnergySupply = currentEnergySupply /1000.0; // mJ to J 
        double siteEnergyCons = adapt.getNewQep().getAgendaIOT().getSiteEnergyConsumption(site); // J
        runningSites.get(site.getID()).setQepExecutionCost(siteEnergyCons);
        double agendaLength = Agenda.bmsToMs(adapt.getNewQep().getAgendaIOT().getLength_bms(false))/1000.0; // ms to s
        double energyConsumptionRate = siteEnergyCons/agendaLength; // J/s
        double siteLifetime = siteEnergySupply / energyConsumptionRate; //s
        shortestLifetime = Math.min((double)shortestLifetime, siteLifetime);
      }
    }
    return shortestLifetime;
  }

  /**
   * calcuates how mnay hops are needed to get data from the sink to the reprogrammed node
   * @param adapt
   * @param reprogrammedSite
   * @return
   */
  private int calculateNoHops(Adaptation adapt, Site reprogrammedSite)
  {
    RT routingTree = adapt.getNewQep().getRT();
    Site sink = routingTree.getRoot();
    Path path = routingTree.getPath(reprogrammedSite.getID(), sink.getID());
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
  private Long calculateMemorySizeOfSiteQEP(Adaptation adapt, Site reprogrammedSite) 
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
    File moteQEP = new File(adaptFolder.toString() + sep + "avrora_mica2_t2" + sep + "mote" + reprogrammedSite.getID() + ".elf");
    Long fileSize = moteQEP.length();
    return fileSize;
  }

  /**
   * Method which determines the energy cost of making the adaptation.
   * @param adapt
   * @return returns the cost in energy units.
   */
  private double energyCost(Adaptation adapt)
  throws IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    CostParameters parameters = _metadataManager.getCostParameters();
    //do each reprogrammed site (most expensive cost)
    Iterator<Site> reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    while(reporgrammedSitesIterator.hasNext())
    {
      Site reprogrammedSite = reporgrammedSitesIterator.next();
      Long SiteImageMemorySize = calculateMemorySizeOfSiteQEP(adapt, reprogrammedSite);
      int packetSize = parameters.getDeliverPayloadSize();
      Long packets = SiteImageMemorySize / packetSize;
      calculateEnergyCostForDataHops(adapt, reprogrammedSite, packets);
      
      runningSites.get(reprogrammedSite.getID())
      .addToCurrentAdaptationEnergyCost(packetSize * AvroraCostParameters.FlashWRITEAMPERE);
    }
    
    //do for each of redirect, deact, act site
    Iterator<Site> redirectedSiteIterator = adapt.redirectedionSitesIterator();
    Iterator<Site> deactivatedSiteIterator = adapt.deactivationSitesIterator();
    Iterator<Site> activatedSiteIterator = adapt.activateSitesIterator();
    
    calcOnePacketEnergyCost(redirectedSiteIterator, adapt);
    calcOnePacketEnergyCost(deactivatedSiteIterator, adapt);
    calcOnePacketEnergyCost(activatedSiteIterator, adapt);
    
    //do for temporal adjustment
    Iterator<TemporalAdjustment> temporalSiteIterator = adapt.temporalSitesIterator();
    while(temporalSiteIterator.hasNext())
    {
      TemporalAdjustment adjustment = temporalSiteIterator.next();
      Iterator<Site> affectedsitesIterator = adjustment.affectedsitesIterator();
      calcOnePacketEnergyCost(affectedsitesIterator, adapt);
    }
    if(!underSpareTime)
      calculateEnergyCostOfRunningStartStopCommand(adapt.getNewQep().getRT());
    return calculateOverallEnergyCost();
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
  private double calculateOverallEnergyCost()
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
  private void calculateEnergyCostForDataHops(Adaptation adapt, Site reprogrammedSite, Long packets) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    RT routingTree = adapt.getNewQep().getRT();
    CostParameters parameters = _metadataManager.getCostParameters();
    AvroraCostExpressions costs = 
      new AvroraCostExpressions(adapt.getNewQep().getDAF(), parameters, adapt.getNewQep().getAgendaIOT());
    Site sink = routingTree.getRoot();
    Path path = routingTree.getPath(reprogrammedSite.getID(), sink.getID());
    Iterator<Site> sitesInPath = path.iterator();
    if(path.getNodes().length == 1)
    {}
    Site dest = sitesInPath.next();
    while(sitesInPath.hasNext())
    {
      Site source = sitesInPath.next();
      double costPerPacket = costs.getPacketEnergyExpression(source, dest, false, null);
      costPerPacket = costPerPacket * packets;
      runningSites.get(dest.getID()).addToCurrentAdaptationEnergyCost(costPerPacket);
      runningSites.get(source.getID()).addToCurrentAdaptationEnergyCost(costPerPacket);
      dest = source;
    }
  }

/**
   * Method which determines the time cost of an adaptation
   * @param adapt
   * @return
   */
  private Long timeCost(Adaptation adapt)
  throws 
  IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    CostParameters parameters = _metadataManager.getCostParameters();
    Long timeTakesSoFar = new Long(0);
    
    //do for type of adaptation
    Iterator<Site> reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    Iterator<Site> redirectedSiteIterator = adapt.redirectedionSitesIterator();
    Iterator<Site> deactivatedSiteIterator = adapt.deactivationSitesIterator();
    Iterator<Site> activatedSiteIterator = adapt.activateSitesIterator();
    
    timeTakesSoFar += calcPacketsTimeCost(redirectedSiteIterator, parameters, adapt, new Long(1));
    timeTakesSoFar += calcPacketsTimeCost(deactivatedSiteIterator, parameters, adapt, new Long(1));
    timeTakesSoFar += calcPacketsTimeCost(activatedSiteIterator,  parameters, adapt, new Long(1));
    timeTakesSoFar += calcPacketsTimeCost(reporgrammedSitesIterator,  parameters, adapt, null);
    
    //do for temporal adjustment
    Iterator<TemporalAdjustment> temporalSiteIterator = adapt.temporalSitesIterator();
    while(temporalSiteIterator.hasNext())
    {
      TemporalAdjustment adjustment = temporalSiteIterator.next();
      Iterator<Site> affectedsitesIterator = adjustment.affectedsitesIterator();
      timeTakesSoFar += calcPacketsTimeCost(affectedsitesIterator, parameters, adapt, new Long(1));
    }
    long goldenFrame = calculateGolderTime(adapt.getOldQep().getAgendaIOT());
    if(timeTakesSoFar > goldenFrame)
      underSpareTime = false;
    else
      underSpareTime = true;
    if(!underSpareTime)
      timeTakesSoFar += calculateStartStopTimeAddition(adapt.getNewQep().getRT());
    return timeTakesSoFar;//goes though each reprogrammable node and cal
  }
  
  /**
   * used to determine how long the start and stop command will take to run.
   * @param rt
   * @return
   */
  private Long calculateStartStopTimeAddition(RT rt)
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
  private long calculateGolderTime(AgendaIOT agendaIOT)
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
   * @param siteIterator
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
  private Long calcPacketsTimeCost(Iterator<Site> siteIterator,
                                 CostParameters parameters, Adaptation adapt,
                                 Long packets) 
  throws 
  IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    Long time = new Long(0);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      int hops = calculateNoHops(adapt, site);
      long timePerHop = (long) Math.ceil(parameters.getCallMethod() + parameters.getSignalEvent() + 
          parameters.getTurnOnRadio()+ parameters.getRadioSyncWindow() * 2
          + parameters.getTurnOffRadio());
      if(packets == null)
      {
        Long SiteImageMemorySize = calculateMemorySizeOfSiteQEP(adapt, site);
        int packetSize = parameters.getDeliverPayloadSize();
        packets = SiteImageMemorySize / packetSize;
      }
      time += packets * hops * timePerHop;
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
  private void calcOnePacketEnergyCost(Iterator<Site> redirectedSiteIterator, Adaptation adapt) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException
  {
    RT routingTree = adapt.getNewQep().getRT();
    CostParameters parameters = _metadataManager.getCostParameters();
    AvroraCostExpressions costs = 
      new AvroraCostExpressions(adapt.getNewQep().getDAF(), parameters, adapt.getNewQep().getAgendaIOT());
    Site sink = routingTree.getRoot();
    while(redirectedSiteIterator.hasNext())
    {
      Site maindest = redirectedSiteIterator.next();
      Path path = routingTree.getPath(maindest.getID(), sink.getID());
      Iterator<Site> sitesInPath = path.iterator();
      if(path.getNodes().length != 0)
      {
        Site dest = sitesInPath.next();
        while(sitesInPath.hasNext())
        {
          Site source = sitesInPath.next();
          double costPerPacket = costs.getPacketEnergyExpression(source, dest, false, null);
          runningSites.get(dest.getID()).addToCurrentAdaptationEnergyCost(costPerPacket);
          runningSites.get(source.getID()).addToCurrentAdaptationEnergyCost(costPerPacket);
          dest = source;
        }
      }
      else
      {
        System.out.println("w");
      }
    }
  }

  public void updateStorageLocation(File outputFolder)
  {
   this.outputFolder = outputFolder;
    
  }
}
