package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.LogicalOverlayNetworkHierarchy;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.UnreliableChannelAgenda;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class ChannelModel implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = 7449374992445910226L;
  private ArrayList<ChannelModelSite> channelModel = new ArrayList<ChannelModelSite>();
  private LogicalOverlayNetworkHierarchy logicaloverlayNetwork;
  private UnreliableChannelAgenda agenda = null;
  private NoiseModel noiseModel; 
  private CostParameters costs;
  private int iteration = 0;
  private AgendaIOT agendaIOT = null;
  private File executorFolder;
  
  
  /**
   * constructor for channel model
   * @param logicaloverlayNetwork
   * @param agenda
   * @param networkSize
   * @param failedNodes
   * @throws SNEEConfigurationException 
   * @throws IOException 
   */
    public ChannelModel (LogicalOverlayNetworkHierarchy logicaloverlayNetwork,  
                       UnreliableChannelAgenda agenda, int networkSize, Topology network,
                       CostParameters costs, File executorFolder)
  throws SNEEConfigurationException, IOException
  {
    this.agenda = agenda;
    this.executorFolder = executorFolder;
    this.costs = costs;
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    setupEmptyArray(networkSize + 1);
    createChannelSites();
    noiseModel = new NoiseModel(network, costs);
  }
  public ChannelModel (LogicalOverlayNetworkHierarchy logicaloverlayNetwork,  
        AgendaIOT agenda, int networkSize, Topology network,
        CostParameters costs, File executorFolder)
  throws SNEEConfigurationException, IOException
  {
    this.agendaIOT = agenda;
    this.executorFolder = executorFolder;
    this.costs = costs;
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    setupEmptyArray(networkSize + 1);
    createChannelSites();
    noiseModel = new NoiseModel(network, costs);
  }
    

  /**
   * creates an empry array of nulls for correct placement of channel sites.
   * @param networkSize
   */
  private void setupEmptyArray(int networkSize)
  {
    for(int index = 0; index < networkSize; index++)
    {
      channelModel.add(null);
    }
  }

  /**
   * runs though one iteration of agenda using the unreliable channel model 
   * to determine which tasks within the agenda are to be ran at runtime by using a 
   * noise model
   * @param robustFolder 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  public void runModel(ArrayList<String> failedNodes, IOT IOT) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, NumberFormatException, SNEEConfigurationException
  {
    System.out.println("running iteration " + iteration);
    Iterator<Task> taskIterator = null;
    if(agenda == null)
     taskIterator =  agendaIOT.taskIteratorOrderedByTime();
    else
      taskIterator = this.agenda.taskIteratorOrderedByTime();
    while(taskIterator.hasNext())
    {
      Task task = taskIterator.next();
      //System.out.println(task.toString());
      if(task instanceof CommunicationTask )
      {
        boolean runable = true;
        CommunicationTask cTask = (CommunicationTask) task;
        if(failedNodes.contains(cTask.getSourceID()) || 
           failedNodes.contains(cTask.getDestID()))
        {
          cTask.setRan(false);
          runable = false;
        }
        
        if(cTask.getMode() == CommunicationTask.RECEIVE && runable)
        {
          if(tranmissionTaskRan(cTask))
           {
            if(isSibling(cTask.getSourceID(), cTask.getTrueDestSite().getID())) 
            {
              //int packetsToTransmit = cTask.getMaxPacektsTransmitted();
              ArrayList<Integer> packetIDsTransmitted = packetsSentByTranmissionTask(cTask, IOT);
              for(int index = 0; index < packetIDsTransmitted.size(); index++)
              {
                Integer packetID = packetIDsTransmitted.get(index);
                tryRecievingSibling(cTask.getTrueDestSite().getID(), Integer.parseInt(cTask.getSourceID()), 
                                    packetID, cTask.getStartTime());
              }
              cTask.setRan(true);
            }
            else
            {
            //  int packetsToTransmit = cTask.getMaxPacektsTransmitted();
               ArrayList<Integer> packetIDsTransmitted = packetsSentByTranmissionTask(cTask, IOT);
               for(int index = 0; index < packetIDsTransmitted.size(); index++)
               {
                 Integer packetID = packetIDsTransmitted.get(index);
                 tryRecieving(cTask.getDestID(), Integer.parseInt(cTask.getSourceID()), packetID, 
                              cTask.getStartTime());
               }
               cTask.setRan(true);
            }
           }
          else
          {
            cTask.setRan(false);
          }
        }
          
        else if(cTask.getMode() == CommunicationTask.TRANSMIT && runable)
        {
          ChannelModelSite source = channelModel.get(Integer.parseInt(cTask.getSourceID()));
          
          if(source.receivedACK())
          {
            cTask.setRan(false);
          }
          else
          {
            cTask.setRan(true);
            source.transmittedPackets();
            source.updateCycle();
          }
        }
        
        else if(cTask.getMode() == CommunicationTask.ACKRECEIVE && runable)
        {
          ChannelModelSite sourceSite = channelModel.get(Integer.parseInt(cTask.getSourceID())); 
          ChannelModelSite destSite = channelModel.get(Integer.parseInt(cTask.getDestNode().getID()));
          
          if(destSite.getTask(cTask.getStartTime(), CommunicationTask.ACKTRANSMIT).isRan())
            if(!didPacketGetRecived(destSite.toString(), Integer.parseInt(sourceSite.toString()),
                cTask.getStartTime(), 1))
              cTask.setRan(false);
            else
            {
              cTask.setRan(true);
              sourceSite.receivedAckFrom(cTask.getCommID());
            }
          else
            cTask.setRan(false);
        }
        
        else if(cTask.getMode() == CommunicationTask.ACKTRANSMIT && runable)
        {
          ChannelModelSite site = channelModel.get(Integer.parseInt(cTask.getDestID())); 
          if(site.needToTransmitAckTo(cTask.getSourceID()))
          {
            cTask.setRan(true);
            site.TransmitAckTo(cTask.getSourceID());
          }
          else
          {
            cTask.setRan(false);
          }
        }
      }
    }
    ChannelModelUtils modelUtils = new ChannelModelUtils(channelModel, logicaloverlayNetwork);
    modelUtils.plotPacketRates(iteration, executorFolder);
    iteration++;
  }
  
  /**
   * checks if a the transmission task thats linked to this recieve task (CTask) has ran
   * @param cTask
   * @return
   */
  private boolean tranmissionTaskRan(CommunicationTask cTask)
  {
    ChannelModelSite site = channelModel.get(Integer.parseInt(cTask.getSourceID())); 
    return site.getTask(cTask.getStartTime(), CommunicationTask.TRANSMIT).isRan();
  }
  
  /**
   * determines how many packets were actually sent by the child (based on its recieve)
   * @param cTask
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private ArrayList<Integer> packetsSentByTranmissionTask(CommunicationTask cTask, IOT IOT)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    ChannelModelSite site = channelModel.get(Integer.parseInt(cTask.getSourceID()));
    if(this.logicaloverlayNetwork.isClusterHead(cTask.getSourceID()))
      return  site.transmittablePackets(IOT);
    else
      return site.transmittablePackets();
  }
  
  public Integer packetsToBeSentByDeliveryTask(Site rootSite, IOT IOT)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    ChannelModelSite site = channelModel.get(Integer.parseInt(rootSite.getID())); 
    return site.transmittablePackets(IOT).size();
  }
  
  

  /**
   * determines if a node is in the same logical node
   * @param sourceID
   * @param destID
   * @return
   */
  private boolean isSibling(String sourceID, String destID)
  {
    ArrayList<String> equivNodes = 
      this.logicaloverlayNetwork.getEquivilentNodes(
          this.logicaloverlayNetwork.getClusterHeadFor(sourceID));
    if(equivNodes.contains(destID) || 
       this.logicaloverlayNetwork.getClusterHeadFor(sourceID).equals(destID))
      return true;
    else
      return false;
  }

  /**
   * determines if a node heard any of their 
   * siblings which were of higher priority to the given node transmit
   * @param site
   * @param redundant
   * @return
   */
  public boolean heardHigherPriorityNodeTransmit(ChannelModelSite site, boolean redundant)
  {
    if(site.heardSiblings() == 0)
      return false;
    else
      return true;
  }

  /**
   * checks with noise model to see if a node has been recieved
   * @param destID
   * @param sourceID
   * @param packetID
   * @param startTime
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private void tryRecieving(String destID, Integer sourceID, int packetID, long startTime)
  throws NumberFormatException, SNEEConfigurationException
  {
    ChannelModelSite outputSite = this.channelModel.get(Integer.parseInt(destID));
    if(agenda == null)
      startTime = startTime +(iteration * this.agendaIOT.getLength_bms(false));
    else
      startTime = startTime +(iteration * agenda.getLength_bms(false));
    startTime = startTime + new Double(Math.ceil(costs.getSendPacket() * (packetID - 1))).longValue();
    if(didPacketGetRecived(destID, sourceID, startTime, packetID))
    {
      outputSite.recivedInputPacket(sourceID.toString(), packetID);
    }
  }
  
  /**
   * checks with noise model to see if a node has been recieved
   * @param destID
   * @param sourceID
   * @param packetID
   * @param startTime
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private void tryRecievingSibling(String destID, Integer sourceID, int packetID, long startTime)
  throws NumberFormatException, SNEEConfigurationException
  {
    ChannelModelSite outputSite = this.channelModel.get(Integer.parseInt(destID));
    if(agenda == null)
      startTime = startTime +(iteration * this.agendaIOT.getLength_bms(false));
    else
      startTime = startTime +(iteration * agenda.getLength_bms(false));
    startTime = startTime + new Double(Math.ceil(costs.getSendPacket() * (packetID - 1))).longValue();
    if(didPacketGetRecived(destID, sourceID, startTime, packetID))
    {
      outputSite.recivedSiblingPacket(sourceID.toString(), packetID);
    }
  }

  /**
   * checks with the noise model to see if the packet was recieved at the destimation.
   * @param destID
   * @param timeOfTransmission 
   * @param siteID
   * @return
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private boolean didPacketGetRecived(String destID, Integer sourceID, long timeOfTransmission,
                                      Integer packetID)
  throws NumberFormatException, SNEEConfigurationException
  {
    return noiseModel.packetRecieved(sourceID.toString(), destID, timeOfTransmission,
                                     channelModel.get(Integer.parseInt(destID)), packetID);
  }

  /**
   * helper method for the constructor, creates the models sites with correct parents etc.
   */
  private void createChannelSites()
  {
    Iterator<String> siteIDIterator = logicaloverlayNetwork.siteIdIterator();
    while(siteIDIterator.hasNext())
    {
      String siteID = siteIDIterator.next();
      Integer siteIDInt = Integer.parseInt(siteID);
      if(siteIDInt == 0)
        System.out.println();
      String clusterHeadID = logicaloverlayNetwork.getClusterHeadFor(siteID);
      Iterator<Node> inputNodes = 
        logicaloverlayNetwork.getQep().getRT().getSite(clusterHeadID).getInputsList().iterator();
      HashMap<String, Integer> expectedPackets = new  HashMap<String, Integer>();
      while(inputNodes.hasNext())
      {
        Node input = inputNodes.next();
        Node agendaInput = null;
        if(agenda == null)
          agendaInput = agendaIOT.getSiteByID((Site) input);
        else
          agendaInput = agenda.getSiteByID((Site) input);
        CommunicationTask task = null;
        if(agenda == null)
          task = agendaIOT.getTransmissionTask(agendaInput);
        else 
          task = agenda.getTransmissionTask(agendaInput);
        int packetsToRecieve = task.getMaxPacektsTransmitted();
        expectedPackets.put(input.getID(), packetsToRecieve);
        Iterator<String> equivNodes =
          logicaloverlayNetwork.getActiveEquivilentNodes(input.getID()).iterator();
        while(equivNodes.hasNext())
        {
          String equivNode = equivNodes.next();
          Node agendaEquivInput = null;
          if(agenda == null)
            agendaEquivInput = agendaIOT.getSiteByID(equivNode);
          else
            agendaEquivInput = agenda.getSiteByID(equivNode);
          if(agenda == null)
            task = agendaIOT.getTransmissionTask(agendaEquivInput);
          else
            task = agenda.getTransmissionTask(agendaEquivInput);
          packetsToRecieve = task.getMaxPacektsTransmitted();
          expectedPackets.put(equivNode, packetsToRecieve);
        } 
      }
      if(logicaloverlayNetwork.getQep().getRT().getRoot().getID().equals(clusterHeadID))
      {//root node with delivery operator, doesnt have a cluster, but needs a channel model site
        ChannelModelSite site = null;
        if(agenda == null)
          site = new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 0, 
                                      agendaIOT.getTasks().get(agendaIOT.getSiteByID(siteID)),
                                      agendaIOT.getBufferingFactor(), costs);
        else
          site = new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 0, 
                                      agenda.getTasks().get(agenda.getSiteByID(siteID)),
                                      agenda.getBufferingFactor(), costs);
        channelModel.set(siteIDInt, site);
      }
      else //not the root node
      {
        ChannelModelSite site;
        if(logicaloverlayNetwork.isClusterHead(siteID))//your a cluster head
        {
          if(agenda == null)
            site = 
              new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 
                                   logicaloverlayNetwork.getPriority(siteID), 
                                   agendaIOT.getTasks().get(agendaIOT.getSiteByID(siteID)),
                                   agendaIOT.getBufferingFactor(), costs);
          else
          site = 
            new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 
                                 logicaloverlayNetwork.getPriority(siteID), 
                                 agenda.getTasks().get(agenda.getSiteByID(siteID)),
                                 agenda.getBufferingFactor(), costs);
        }
        else //your a sibling in the logical overlay to the cluster head. 
        {
          expectedPackets = new  HashMap<String, Integer>();
          if(agenda == null)
          {
            //siblings only receive packets from cluster head and other siblings
            Site clusterHeadSite = this.agendaIOT.getSiteByID(clusterHeadID);
            int packetsTransmittedByClusterHead = 
              this.agendaIOT.getTransmissionTask(clusterHeadSite).getMaxPacektsTransmitted();
            expectedPackets.put(clusterHeadID, packetsTransmittedByClusterHead);
            Iterator<String> siblings = 
              this.logicaloverlayNetwork.getActiveEquivilentNodes(clusterHeadID).iterator();
            while(siblings.hasNext())
            {
              expectedPackets.put(siblings.next(), packetsTransmittedByClusterHead);
            }
            
            site = new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 
                                       logicaloverlayNetwork.getPriority(siteID), 
                                       agendaIOT.getTasks().get(agendaIOT.getSiteByID(siteID)),
                                       agendaIOT.getBufferingFactor(), costs);
          }
          else
          {
            //siblings only receive packets from cluster head and other siblings
            Site clusterHeadSite = this.agenda.getSiteByID(clusterHeadID);
            int packetsTransmittedByClusterHead = 
              this.agenda.getTransmissionTask(clusterHeadSite).getMaxPacektsTransmitted();
            expectedPackets.put(clusterHeadID, packetsTransmittedByClusterHead);
            Iterator<String> siblings = 
              this.logicaloverlayNetwork.getActiveEquivilentNodes(clusterHeadID).iterator();
            while(siblings.hasNext())
            {
              expectedPackets.put(siblings.next(), packetsTransmittedByClusterHead);
            }
            site = new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 
                                        logicaloverlayNetwork.getPriority(siteID), 
                                        agenda.getTasks().get(agenda.getSiteByID(siteID)),
                                        agenda.getBufferingFactor(), costs);
          }
        }
          
        channelModel.set(siteIDInt, site);
      }
    }
  }

  public int packetToTupleConversion(Integer packets, Site rootSite)
  throws SchemaMetadataException, TypeMappingException
  {
    Site iotSite = logicaloverlayNetwork.getQep().getIOT().getSiteFromID(rootSite.getID());
    InstanceOperator rootOp = logicaloverlayNetwork.getQep().getIOT().getRootOperatorOfSite(iotSite);
    return ChannelModelSite.packetToTupleConversion(packets, rootOp, rootOp);
  }

  public void clearModel()
  {
    Iterator<ChannelModelSite> modelSiteIterator = channelModel.iterator();
    while(modelSiteIterator.hasNext())
    {
      ChannelModelSite site = modelSiteIterator.next();
      if(site != null)
        site.clearDataStoresForNewIteration();
    }
  }
  
  public void setPacketModel(boolean reliableChannelQEP)
  {
    Iterator<ChannelModelSite> modelSiteIterator = channelModel.iterator();
    while(modelSiteIterator.hasNext())
    {
      ChannelModelSite site = modelSiteIterator.next();
      if(site != null)
        ChannelModelSite.setReliableChannelQEP(reliableChannelQEP);
    }
  }
}
