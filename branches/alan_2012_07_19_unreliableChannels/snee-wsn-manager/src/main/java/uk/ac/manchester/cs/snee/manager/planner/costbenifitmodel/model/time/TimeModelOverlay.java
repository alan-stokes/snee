package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.time;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts.AvroraCostParameters;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.RobustSensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCB;

public class TimeModelOverlay extends TimeModel
{
  private boolean underSpareTime;
  
  public TimeModelOverlay(SNCB imageGenerator)
  {
    super(imageGenerator);
  }
  
  public void initilise(File imageGenerationFolder, MetadataManager _metadataManager)
  {
    super.initilise(imageGenerationFolder, _metadataManager);
  }
  
  /**
   * calculates how lnog the reprogramming step takes
   * @param current 
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  private double calculateTimePerReprogram(String site, Adaptation adapt, LogicalOverlayNetwork current) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    CostParameters parameters = _metadataManager.getCostParameters();
    double packets = calculateNumberOfPacketsForSiteQEP(adapt, site);
    int numberInCluster = current.getEquivilentNodes(site).size();
    if(numberInCluster > 0)
      packets += packets * numberInCluster;
    return packets * parameters.getDeliverPayloadSize() * AvroraCostParameters.FlashWRITECYCLES * AvroraCostParameters.CYCLETIME;
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
  private Long calculateTimePacketsCost(Iterator<String> siteIterator,
                                 CostParameters parameters, Adaptation adapt,
                                 Long packets, boolean deactivatedNodesChecking,
                                 LogicalOverlayNetwork current) 
  throws 
  IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    Long time = new Long(0);
    while(siteIterator.hasNext())
    {
      String site = siteIterator.next();
      int hops = calculateNumberOfHops(adapt, site, deactivatedNodesChecking);
      long timePerHop = (long) Math.ceil(parameters.getCallMethod() + parameters.getSignalEvent() + 
          parameters.getTurnOnRadio()+ parameters.getRadioSyncWindow() * 2
          + parameters.getTurnOffRadio());
      if(packets == null)
      {
        packets = calculateNumberOfPacketsForSiteQEP(adapt, site);
      }
      Long packetTime = (long) Math.ceil(parameters.getSendPacket() * packets);
      time += (packetTime + timePerHop) * (hops + current.getEquivilentNodes(site).size());
    }
    return time;
  }
  
  /**
   * Method which determines the time cost of an adaptation
   * @param adapt
   * @return
   */
  public Long calculateTimeCost(Adaptation adapt, LogicalOverlayNetwork current) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    CostParameters parameters = _metadataManager.getCostParameters();
    Double timeTakesSoFar = new Double(0);
    
    //do for type of adaptation
    Iterator<String> reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    timeTakesSoFar += 
      calculateTimePacketsCost(reporgrammedSitesIterator,  parameters, adapt, null, false, current);
    
    //do time for reprogramming 
    reporgrammedSitesIterator = adapt.reprogrammingSitesIterator();
    while(reporgrammedSitesIterator.hasNext())
    {
      timeTakesSoFar += calculateTimePerReprogram(reporgrammedSitesIterator.next(), adapt, current);
    }
    long goldenFrame = 0;
    if(adapt.getOldQep() instanceof RobustSensorNetworkQueryPlan)
    {
      RobustSensorNetworkQueryPlan rQEP = (RobustSensorNetworkQueryPlan) adapt.getOldQep();
      goldenFrame = calculateGoldenTimeFrame(rQEP.getUnreliableAgenda());
    }
    else
      goldenFrame = calculateGoldenTimeFrame(adapt.getOldQep().getAgendaIOT());
    if(timeTakesSoFar > goldenFrame)
      underSpareTime = false;
    else
      underSpareTime = true;
    if(!underSpareTime)
      timeTakesSoFar += calculateTimeStartStopAddition(adapt.getNewQep().getRT());
    return timeTakesSoFar.longValue();//goes though each reprogrammable node and cal
  }
  
}
