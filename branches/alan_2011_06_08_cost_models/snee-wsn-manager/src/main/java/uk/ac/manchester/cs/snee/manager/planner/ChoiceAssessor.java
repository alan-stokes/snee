package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.TemporalAdjustment;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationUtils;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCB;
import uk.ac.manchester.cs.snee.sncb.TinyOS_SNCB_Controller;

public class ChoiceAssessor
{
  private String sep = System.getProperty("file.separator");
  private File AssessmentFolder;
  private MetadataManager _metadataManager;
  private SensorNetworkSourceMetadata _metadata;
  private Topology network;
  private File outputFolder;
  private File imageGenerationFolder;
  private boolean underSpareTime;
  private SNCB imageGenerator = null;
  private boolean compiledAlready = false;
  
  
  public ChoiceAssessor(SourceMetadataAbstract _metadata, MetadataManager _metadataManager,
                        File outputFolder)
  {
    this._metadataManager = _metadataManager;
    this._metadata = (SensorNetworkSourceMetadata) _metadata;
    network = this._metadata.getTopology();
    this.outputFolder = outputFolder;
    this.imageGenerator = new TinyOS_SNCB_Controller();
  }

  /**
   * checks all constraints to executing the changes and locates the best choice 
   * @param choices
   * @return
   * @throws IOException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws CodeGenerationException 
   */
  public Adaptation assessChoices(List<Adaptation> choices) 
  throws 
  IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  CodeGenerationException
  {
    AssessmentFolder = new File(outputFolder.toString() + sep + "assessment");
    AssessmentFolder.mkdir();
    imageGenerationFolder = new File(AssessmentFolder.toString() + sep + "Adaptations");
    imageGenerationFolder.mkdir();
    
    System.out.println("Starting assessment of choices");
    Iterator<Adaptation> choiceIterator = choices.iterator();
    while(choiceIterator.hasNext())
    {
      Adaptation adapt = choiceIterator.next();
      adapt.setTimeCost(this.timeCost(adapt));
      adapt.setEnergyCost(this.energyCost(adapt));
      adapt.setRuntimeCost(this.qepExecutionCost(adapt));
      adapt.setLifetimeEstimate(this.estimatedLifetime(adapt));
      
    }
    new AdaptationUtils(choices, _metadataManager.getCostParameters()).FileOutput(AssessmentFolder);
    
    return this.locateBestAdaptation(choices);
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
        double currentEnergySupply = site.getEnergyStock() - adapt.getSiteEnergyCost(site.getID());
        double siteEnergySupply = currentEnergySupply /1000.0; // mJ to J 
        double siteEnergyCons = adapt.getNewQep().getAgendaIOT().getSiteEnergyConsumption(site); // J
        double agendaLength = Agenda.bmsToMs(adapt.getNewQep().getAgendaIOT().getLength_bms(false))/1000.0; // ms to s
        double energyConsumptionRate = siteEnergyCons/agendaLength; // J/s
        double siteLifetime = siteEnergySupply / energyConsumptionRate; //s
      
        shortestLifetime = Math.min((double)shortestLifetime, siteLifetime);
      }
    }
    return shortestLifetime;
  }

  /**
   * method to determine cost of running new QEP for an agenda execution cycle
   * @param adapt
   * @return
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  private Long qepExecutionCost(Adaptation adapt) 
  {
    return null;
  }

  /**
   * calcuates how mnay hops are needed to get data from the sink to the reprogrammed node
   * @param adapt
   * @param reprogrammedSite
   * @return
   */
  private int calculateHops(Adaptation adapt, Site reprogrammedSite)
  {
    RT routingTree = adapt.getNewQep().getRT();
    Site sink = routingTree.getRoot();
    Path path = routingTree.getPath(reprogrammedSite.getID(), sink.getID());
    return path.getNodes().length;
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
    return moteQEP.length();
  }

  /**
   * Method which determines the energy cost of making the adaptation.
   * @param adapt
   * @return returns the cost in energy units.
   */
  private Long energyCost(Adaptation adapt)
  throws IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    CostParameters parameters = _metadataManager.getCostParameters();
    Long timeTakesSoFar = new Long(0);
    //do each reprogrammed site (most expensive cost)
    Iterator<Site> reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    while(reporgrammedSitesIterator.hasNext())
    {
      Site reprogrammedSite = reporgrammedSitesIterator.next();
      Long SiteImageMemorySize = calculateMemorySizeOfSiteQEP(adapt, reprogrammedSite);
      int packetSize = parameters.getDeliverPayloadSize();
      Long packets = SiteImageMemorySize / packetSize;
      int hops = calculateHops(adapt, reprogrammedSite);
      
      long timePerHop = (long) Math.ceil(parameters.getCallMethod() + parameters.getSignalEvent() + 
                                  parameters.getTurnOnRadio()+ parameters.getRadioSyncWindow() * 2
                                  + parameters.getTurnOffRadio());
      long reprogrammingTime = (long) (packetSize * parameters.getWriteToFlash());
      timeTakesSoFar += packets * hops * timePerHop * reprogrammingTime;
    }
    //do for each of redirect, deact, act site
    Iterator<Site> redirectedSiteIterator = adapt.redirectedionSitesIterator();
    Iterator<Site> deactivatedSiteIterator = adapt.deactivationSitesIterator();
    Iterator<Site> activatedSiteIterator = adapt.activateSitesIterator();
    
    timeTakesSoFar += calcOnePacketTimeCost(redirectedSiteIterator, parameters, adapt);
    timeTakesSoFar += calcOnePacketTimeCost(deactivatedSiteIterator, parameters, adapt);
    timeTakesSoFar += calcOnePacketTimeCost(activatedSiteIterator,  parameters, adapt);
    
    //do for temporal adjustment
    Iterator<TemporalAdjustment> temporalSiteIterator = adapt.temporalSitesIterator();
    while(temporalSiteIterator.hasNext())
    {
      TemporalAdjustment adjustment = temporalSiteIterator.next();
      Iterator<Site> affectedsitesIterator = adjustment.affectedsitesIterator();
      timeTakesSoFar += calcOnePacketTimeCost(affectedsitesIterator, parameters, adapt);
    }
    return timeTakesSoFar;//goes though each reprogrammable node and cal
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
    //do each reprogrammed site (most expensive cost)
    Iterator<Site> reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    while(reporgrammedSitesIterator.hasNext())
    {
      Site reprogrammedSite = reporgrammedSitesIterator.next();
      Long SiteImageMemorySize = calculateMemorySizeOfSiteQEP(adapt, reprogrammedSite);
      int packetSize = parameters.getDeliverPayloadSize();
      Long packets = SiteImageMemorySize / packetSize;
      int hops = calculateHops(adapt, reprogrammedSite);
      long timePerHop = (long) Math.ceil(parameters.getCallMethod() + parameters.getSignalEvent() + 
                                  parameters.getTurnOnRadio()+ parameters.getRadioSyncWindow() * 2
                                  + parameters.getTurnOffRadio());
      long reprogrammingTime = (long) (packetSize * parameters.getWriteToFlash());
      timeTakesSoFar += packets * hops * timePerHop * reprogrammingTime;
    }
    //do for each of redirect, deact, act site
    Iterator<Site> redirectedSiteIterator = adapt.redirectedionSitesIterator();
    Iterator<Site> deactivatedSiteIterator = adapt.deactivationSitesIterator();
    Iterator<Site> activatedSiteIterator = adapt.activateSitesIterator();
    
    timeTakesSoFar += calcOnePacketTimeCost(redirectedSiteIterator, parameters, adapt);
    timeTakesSoFar += calcOnePacketTimeCost(deactivatedSiteIterator, parameters, adapt);
    timeTakesSoFar += calcOnePacketTimeCost(activatedSiteIterator,  parameters, adapt);
    
    //do for temporal adjustment
    Iterator<TemporalAdjustment> temporalSiteIterator = adapt.temporalSitesIterator();
    while(temporalSiteIterator.hasNext())
    {
      TemporalAdjustment adjustment = temporalSiteIterator.next();
      Iterator<Site> affectedsitesIterator = adjustment.affectedsitesIterator();
      timeTakesSoFar += calcOnePacketTimeCost(affectedsitesIterator, parameters, adapt);
    }
    return timeTakesSoFar;//goes though each reprogrammable node and cal
  }
  
  /**
   * calculates the time cost of sending one packet down to a node 
   * (used for redircet, deact, and act adaptations)
   * @param redirectedSiteIterator
   * @param parameters
   * @param adapt
   * @return
   */
  private Long calcOnePacketTimeCost(Iterator<Site> redirectedSiteIterator,
                                 CostParameters parameters, Adaptation adapt)
  {
    Long time = new Long(0);
    while(redirectedSiteIterator.hasNext())
    {
      Site redirectedSite = redirectedSiteIterator.next();
      Long packets = new Long(1);
      int hops = calculateHops(adapt, redirectedSite);
      long timePerHop = (long) Math.ceil(parameters.getCallMethod() + parameters.getSignalEvent() + 
          parameters.getTurnOnRadio()+ parameters.getRadioSyncWindow() * 2
          + parameters.getTurnOffRadio());
      time += packets * hops * timePerHop;
    }
    return time;
  }
  
  /**
   * calculates the energy cost of sending one packet down to a node
   * @param redirectedSiteIterator
   * @param parameters
   * @param adapt
   * @return
   */
  private Long calcOnePacketEnergyCost(Iterator<Site> redirectedSiteIterator,
      CostParameters parameters, Adaptation adapt)
  {
    Long time = new Long(0);
    while(redirectedSiteIterator.hasNext())
    {
      Site redirectedSite = redirectedSiteIterator.next();
      Long packets = new Long(1);
      int packetSize = parameters.getDeliverPayloadSize();
      int hops = calculateHops(adapt, redirectedSite);
      long timePerHop = (long) Math.ceil(parameters.getCallMethod() + parameters.getSignalEvent() + 
      parameters.getTurnOnRadio()+ parameters.getRadioSyncWindow() * 2
      + parameters.getTurnOffRadio());
      time += packets * hops * timePerHop;
    }
    return time;
  }

  /**
   * goes though the choices list locating the one with the biggest lifetime
   * @param choices
   * @return
   */
  private Adaptation locateBestAdaptation(List<Adaptation> choices)
  {
    Adaptation finalChoice = null;
    Double cost = Double.MAX_VALUE;
    Iterator<Adaptation> choiceIterator = choices.iterator();
    //calculate each cost, and compares it with the best so far, if the same, store it 
    while(choiceIterator.hasNext())
    {
      Adaptation choice = choiceIterator.next();
      Double choiceCost = choice.getLifetimeEstimate();
      if(choiceCost < cost)
      {
        finalChoice = choice;
        cost = choiceCost;
      }
    }
    return finalChoice;
  }
}
