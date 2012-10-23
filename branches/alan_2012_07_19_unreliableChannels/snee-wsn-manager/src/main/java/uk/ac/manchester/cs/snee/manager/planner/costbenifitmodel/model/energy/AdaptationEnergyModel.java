package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.energy;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts.AvroraCostParameters;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.TemporalAdjustment;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.Model;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCB;

public class AdaptationEnergyModel extends Model
{
  protected HashMap<String, RunTimeSite> runningSites;
  
  
  public AdaptationEnergyModel(SNCB imageGenerator)
  {
    super(imageGenerator);
  }
  
  public void initilise(File imageGenerationFolder, MetadataManager _metadataManager, 
                        HashMap<String, RunTimeSite> runningSites)
  {
    super.initilise(imageGenerationFolder, _metadataManager, false);
    this.runningSites = runningSites;
  }
  
  public void initilise(File imageGenerationFolder, MetadataManager _metadataManager, 
                        HashMap<String, RunTimeSite> runningSites, Boolean useModelForBinaries)
  {
    super.initilise(imageGenerationFolder, _metadataManager, useModelForBinaries);
    this.runningSites = runningSites;
  }

  
  /**
   * Method which determines the energy cost of making the adaptation.
   * @param adapt
   * @return returns the cost in energy units.
   */
  public double calculateEnergyCost(Adaptation adapt)
  throws IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    CostParameters parameters = _metadataManager.getCostParameters();
    
    //calculateReprogrammingCost(adapt, parameters);
    calculateReprogrammingCostNeat(adapt, parameters);
    
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
    if(!Model.underSpareTime)
      calculateEnergyCostOfRunningStartStopCommand(adapt.getNewQep().getRT());
    return calculateEnergyOverallAdaptationCost();
  }
  
  /**
   * calculates the cost induced on all nodes for the reprogramming events
   * @param adapt
   * @param parameters
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private void calculateReprogrammingCostNeat(Adaptation adapt,
      CostParameters parameters) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    Iterator<Node> nodeIterator = adapt.getNewQep().getRT().getSiteTree().getNodes().iterator();
    while(nodeIterator.hasNext())
    {
      Site site = (Site) nodeIterator.next();
      runningSites.get(site.getID())
      .addToCurrentAdaptationEnergyCost((reprogrammingCost(site, adapt, parameters) + 
                                        communicationCost(site, adapt, parameters) / new Double(1000)));
    }
  }

  /**
   * calculates the cost of all communication events ran on this node for reprogramming in mj
   * @param site
   * @param adapt
   * @param parameters
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double  communicationCost(Site site, Adaptation adapt, CostParameters parameters) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    return recieveCost(site, adapt, parameters) + transmissionCost(site, adapt, parameters);
  }

  /**
   * calculates the cost of transmitting all the downstream reprogramming binaries
   * @param site
   * @param adapt
   * @param parameters
   * @return
   */
  private double transmissionCost(Site site, Adaptation adapt, CostParameters parameters)
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    double cost = 0;
    Iterator<Site> childrenIterator = adapt.getNewQep().getRT().siteIterator(site, TraversalOrder.POST_ORDER);
    cost += transmissionCost(childrenIterator, site, adapt, parameters);
    while(childrenIterator.hasNext())
    {
      Site childSite = childrenIterator.next();
      cost+= transmissionCostOf(childSite, site, adapt, parameters);
    }
    return cost;
  }
  
  
  /**
   * helper method to calculate the energy cost of transmitting binaries
   * @param childrenIterator
   * @param currentSite
   * @param adapt
   * @param parameters
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double transmissionCost(Iterator<Site> childrenIterator, Site currentSite,
      Adaptation adapt, CostParameters parameters) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    double cost = 0;
    while(childrenIterator.hasNext())
    {
      Site childSite = childrenIterator.next();
      if(!childSite.getID().equals(currentSite.getID()))
        cost+= transmissionCostOf(childSite, currentSite, adapt, parameters);
    }
    return cost;
  }

  /**
   * calculates the energy cost of transmitting 
   * @param childSite
   * @param adapt
   * @param parameters
   * @return
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  private double transmissionCostOf(Site site, Site currentSite, Adaptation adapt,
      CostParameters parameters) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    if(adapt.getReprogrammingSites().contains(site.getID()))
    {
      return tCost(site, currentSite, adapt, parameters) + CPU(site, adapt, parameters);
    }
    else
      return 0;
  }

  /**
   * calculates the cost of transmitting a binary over the radio from 
   * @param site
   * @param adapt
   * @param parameters
   * @return
   */
  private double tCost(Site site, Site currentSite, Adaptation adapt, CostParameters parameters)
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    Long packets = this.calculateNumberOfPacketsForSiteQEP(adapt, site.getID());
    double duration = AgendaIOT.bmsToMs((packets * (long) Math.ceil(parameters.getSendPacket() * packets)) +
        CommunicationTask.getTimeCostOverhead(parameters, false, true)) / new Double(1000);
    Iterator<Site> pathIterator = adapt.getNewQep().getRT().getPath(site.getID(), currentSite.getID()).iterator();
    boolean notFound = true;
    Site nextSite = null;
    while(notFound)
    {
      nextSite = pathIterator.next();
      if(nextSite.getOutput(0).getID().equals(currentSite.getID()))
        notFound = false;
    }
    int txPower = (int)adapt.getNewQep().getRT().getRadioLink(currentSite, nextSite).getEnergyCost();
    double transmissionAmp = AvroraCostParameters.getTXAmpere(txPower);
    return duration * transmissionAmp * AvroraCostParameters.VOLTAGE;
  }

  /**
   * Calculates the cost of receiving all the downstream reprogramming binaries. and its own binary
   * @param site
   * @param adapt
   * @param parameters
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double recieveCost(Site site, Adaptation adapt, CostParameters parameters) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    double cost = 0;
    Iterator<Site> childrenIterator = adapt.getNewQep().getRT().siteIterator(site, TraversalOrder.POST_ORDER);
    cost += recieveCost(childrenIterator , adapt, parameters);
    return cost;
  }
  
  /**
   * helper method for calculating the cost of receiving all the downstream reprogramming binaries
   * @param childrenIterator
   * @param adapt
   * @param parameters
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  public double recieveCost(Iterator<Site> childrenIterator , Adaptation adapt, CostParameters parameters) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    double cost = 0;
    while(childrenIterator.hasNext())
    {
      Site childSite = childrenIterator.next();
      cost+= recieveCostOf(childSite, adapt, parameters );
    }
    return cost;
  }

  /**
   * Calculates the cost of receiving a sites binary.
   * @param site
   * @param adapt
   * @param parameters
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double recieveCostOf(Site site, Adaptation adapt, CostParameters parameters)
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    if(adapt.getReprogrammingSites().contains(site.getID()))
    {
      return rCost(site, adapt, parameters) + CPU(site, adapt, parameters);
    }
    else
      return 0;
  }

  /**
   * Calculates the cost of the active CPU for a set of packet transmissions.
   * @param site
   * @param adapt
   * @param parameters
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double CPU(Site site, Adaptation adapt, CostParameters parameters) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    double cpuActiveAmp = AvroraCostParameters.CPUACTIVEAMPERE;
    Long packets = this.calculateNumberOfPacketsForSiteQEP(adapt, site.getID());
    double duration = AgendaIOT.bmsToMs((packets * (long) Math.ceil(parameters.getSendPacket() * packets)) +
                       CommunicationTask.getTimeCostOverhead(parameters, false, true)) / new Double(1000);
    return duration * cpuActiveAmp * AvroraCostParameters.VOLTAGE;
  }

  /**
   * 
   * @param site
   * @param adapt
   * @param parameters
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double rCost(Site site, Adaptation adapt, CostParameters parameters ) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    double radioRXAmp = AvroraCostParameters.getRadioReceiveAmpere();
    Long packets = this.calculateNumberOfPacketsForSiteQEP(adapt, site.getID());
    double duration = AgendaIOT.bmsToMs((packets * (long) Math.ceil(parameters.getSendPacket() * packets)) +
                       CommunicationTask.getTimeCostOverhead(parameters, false, true)) / new Double(1000);
    return duration * radioRXAmp * AvroraCostParameters.VOLTAGE;
  }

  /**
   * Calculates the overall energy cost of reprogramming a node.
   * @param site
   * @param adapt
   * @param parameters
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double reprogrammingCost(Site site, Adaptation adapt, CostParameters parameters) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    if(adapt.getReprogrammingSites().contains(site.getID()))
    {
      Long packets = this.calculateNumberOfPacketsForSiteQEP(adapt, site.getID());
      return flash(site, adapt, parameters, packets) + flashCPU(site, adapt, parameters, packets);
    }
    else
      return 0;
  }

  /**
   * Calculates the energy cost of the active cpu during flash writing
   * @param site
   * @param adapt
   * @param parameters
   * @param packets
   * @return
   */
  private double flashCPU(Site site, Adaptation adapt, CostParameters parameters, Long packets)
  {
    double cpuFlashCost = AvroraCostParameters.VOLTAGE * AvroraCostParameters.FlashWRITECYCLES * 
    AvroraCostParameters.CYCLETIME * AvroraCostParameters.CPUACTIVEAMPERE;
    return packets * parameters.getDeliverPayloadSize() * cpuFlashCost;
  }

  /**
   * generates the energy cost of writing the sites binary to flash
   * @param site
   * @param adapt
   * @param parameters
   * @param packets
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double flash(Site site, Adaptation adapt, CostParameters parameters, Long packets) 
  throws IOException, SchemaMetadataException, TypeMappingException,
  OptimizationException, CodeGenerationException
  {
    double costPerByteWritten = AvroraCostParameters.VOLTAGE * AvroraCostParameters.FlashWRITECYCLES * 
    AvroraCostParameters.CYCLETIME * AvroraCostParameters.FlashWRITEAMPERE;
    double bytes = packets * parameters.getDeliverPayloadSize();
    return bytes * costPerByteWritten; 
  }

  /*
  private void calculateReprogrammingCost(Adaptation adapt, CostParameters parameters)
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    //do each reprogrammed site (most expensive cost)
    Iterator<String> reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    while(reporgrammedSitesIterator.hasNext())
    {
      String reprogrammedSite = reporgrammedSitesIterator.next();
      Long packets = calculateNumberOfPacketsForSiteQEP(adapt, reprogrammedSite);
      double overallCost = calculateEnergyCostForDataHops(adapt, reprogrammedSite, packets);
      
      //energy usage of the reprogrammed site by flash writing
      //voltage * getCycles(mode) * ampere[mode] * cycleTime;
      double costPerByteWritten = AvroraCostParameters.VOLTAGE * AvroraCostParameters.FlashWRITECYCLES * 
                                  AvroraCostParameters.CYCLETIME * AvroraCostParameters.FlashWRITEAMPERE;
      double cpuFlashCost = AvroraCostParameters.VOLTAGE * AvroraCostParameters.FlashWRITECYCLES * 
                            AvroraCostParameters.CYCLETIME * AvroraCostParameters.CPUACTIVEAMPERE;
      runningSites.get(reprogrammedSite).addToCurrentAdaptationEnergyCost((packets * parameters.getDeliverPayloadSize() * costPerByteWritten));
      runningSites.get(reprogrammedSite).addToCurrentAdaptationEnergyCost((packets * parameters.getDeliverPayloadSize() * cpuFlashCost));
      overallCost += (packets * parameters.getDeliverPayloadSize() * costPerByteWritten);
    }
  }*/
  
  /**
   * used to determine the energy cost of calling start and stop on each node
   * @param rt
   */
  protected void calculateEnergyCostOfRunningStartStopCommand(RT rt)
  {
    // TODO calculate each energy cost
    
  }

  /**
   * goes though the entire running sites, looking for cost of adaptation.
   * @return
   */
  protected double calculateEnergyOverallAdaptationCost()
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
   * method to calculate cost of each hop and place cost onto running sites adpatation cost counter
   * @param adapt
   * @param reprogrammedSite
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  /*
  protected double calculateEnergyCostForDataHops(Adaptation adapt, String reprogrammedSite, Long packets) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    double overallCost = 0.0;
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
        overallCost += sourceCost + destCost;
        RunTimeSite sourceSite = runningSites.get(source.getID());
        RunTimeSite destSite = runningSites.get(dest.getID());
        sourceSite.addToCurrentAdaptationEnergyCost(sourceCost);
        destSite.addToCurrentAdaptationEnergyCost(destCost);
        dest = source;
      }
    }
     return overallCost;
  }
*/

  /**
   * calculates the energy cost of sending one packet down to a node
   * @param redirectedSiteIterator
   * @param parameters
   * @param adapt
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  protected void calculateEnergyOnePacketCost(Iterator<String> redirectedSiteIterator, Adaptation adapt, 
                                       boolean deactivedNodes) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException
  {
    SiteEnergyModel siteModel = new SiteEnergyModel(adapt.getNewQep().getAgendaIOT());
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
          
          CommunicationTask sourceTask = new CommunicationTask(new Long(0),  dest, source, CommunicationTask.TRANSMIT, new Long(1), parameters, false, dest);
          CommunicationTask destTask = new CommunicationTask(new Long(0), dest, source, CommunicationTask.RECEIVE, new Long(1), parameters, false, dest);
          double sourceCost;
          double destCost;
          
          if(!deactivedNodes)
          {
            sourceCost = siteModel.evaluateCommunicationTask(sourceTask, new Long(1));
            destCost = siteModel.evaluateCommunicationTask(destTask, new Long(1));
          }
          else
          {
            sourceCost = siteModel.evaluateCommunicationTask(sourceTask, new Long(1));
            destCost = siteModel.evaluateCommunicationTask(sourceTask, new Long(1));
          }
          runningSites.get(dest.getID()).addToCurrentAdaptationEnergyCost(destCost);
          runningSites.get(source.getID()).addToCurrentAdaptationEnergyCost(sourceCost);
          dest = source;
        }
      }
    }
  }
  
  

}
