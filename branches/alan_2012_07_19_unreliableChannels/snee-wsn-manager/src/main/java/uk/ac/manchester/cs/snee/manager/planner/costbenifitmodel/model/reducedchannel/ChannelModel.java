package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.reducedchannel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel.NoiseModel;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.UnreliableChannelAgenda;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class ChannelModel implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = 7449374992445910226L;
  private ArrayList<ChannelModelSite> channelModel = new ArrayList<ChannelModelSite>();
  private LogicalOverlayNetwork logicaloverlayNetwork;
  private UnreliableChannelAgenda agenda;
  private ArrayList<String> failedNodes;
  private NoiseModel noiseModel; 
  
  /**
   * constructor for channel model
   * @param logicaloverlayNetwork
   * @param agenda
   * @param networkSize
   * @param failedNodes
   * @throws SNEEConfigurationException 
   */
  public ChannelModel (LogicalOverlayNetwork logicaloverlayNetwork,  
                       UnreliableChannelAgenda agenda, int networkSize,
                       ArrayList<String> failedNodes) 
  throws SNEEConfigurationException
  {
    this.agenda = agenda;
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    this.failedNodes = failedNodes;
    setupEmptyArray(networkSize + 1);
    createChannelSites();
    noiseModel = new NoiseModel();
    runModel();
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
   */
  private void runModel()
  {
    Iterator<Task> taskIterator =  agenda.taskIteratorOrderedByTime();
    while(taskIterator.hasNext())
    {
      Task task = taskIterator.next();
      if(task instanceof CommunicationTask )
      {
        CommunicationTask cTask = (CommunicationTask) task;
        if(cTask.getMode() == CommunicationTask.RECEIVE ||
           cTask.getMode() == CommunicationTask.TRANSMIT)
        {
          int packetsToTransmit = cTask.getMaxPacektsTransmitted();
          
          if(!failedNodes.contains(cTask.getSourceID()) && 
             !failedNodes.contains(cTask.getDestID()) &&
              tranmissionTaskNeeded(cTask.getSourceID()))
          {
            for(int packetID = 0; packetID < packetsToTransmit; packetID++)
            {
              tryTransmission(cTask.getDestID(), Integer.parseInt(cTask.getSourceID()), packetID);
            }
            ChannelModelSite site = channelModel.get(Integer.parseInt(cTask.getSourceID()));
            site.setNeedTransmit();
            verifyAck(cTask.getDestID(), cTask.getSourceID());
          }
        }
      }
    }
  }

  /**
   * checks if a node needs to transmit (within the model)
   * @param sourceID
   * @return
   */
  private boolean tranmissionTaskNeeded(String sourceID)
  {
    ChannelModelSite site = channelModel.get(Integer.parseInt(sourceID)); 
    if(site == null)
      return false;
    return site.channelModelNeedToTransmit();
  }

  /**
   * sorts out what acks have been recieved
   * @param destID
   * @param siteID
   */
  private void verifyAck(String destID, String siteID)
  {    
    //get all nodes to recieve acks
    ArrayList<ChannelModelSite> inputSites = getListOfChannelSitesFromLogicaloverlay(siteID.toString(), false);
    ChannelModelSite childClusterHead = channelModel.get(Integer.parseInt(logicaloverlayNetwork.getClusterHeadFor(siteID.toString())));
    
    //collect all output sites
    ArrayList<ChannelModelSite> outputSites = getListOfChannelSitesFromLogicaloverlay(destID, true);
        
    //sort out acks
    Iterator<ChannelModelSite> siteiterator = outputSites.iterator();
    while(siteiterator.hasNext())
    {
      ChannelModelSite outputSite = siteiterator.next();
      if(outputSite.needToTransmitAckTo(childClusterHead.toString()))
      {
        outputSite.channelModelSetToTransmitACK(true);
        outputSite.incrementTransmittedAcks();
        if(didPacketGetRecived(childClusterHead.toString(), Integer.parseInt(outputSite.toString())))
        {
          childClusterHead.receivedACK(outputSite);
        }
        Iterator<ChannelModelSite> childSiteIterator = inputSites.iterator();
        while(childSiteIterator.hasNext())
        {
          ChannelModelSite childSite = childSiteIterator.next();
          if(didPacketGetRecived(childSite.toString(), Integer.parseInt(outputSite.toString())))
            childSite.receivedACK(outputSite);
        }
      }
    }
  }

  
  /**
   * gets the models sites for a given logical node
   * @param siteID
   * @param withClusterHead
   * @return
   */
  private ArrayList<ChannelModelSite> getListOfChannelSitesFromLogicaloverlay(String siteID, boolean withClusterHead)
  {
    ArrayList<ChannelModelSite> sites = new  ArrayList<ChannelModelSite>();
    String clusterHeadID = logicaloverlayNetwork.getClusterHeadFor(siteID.toString());
    if(withClusterHead)
      sites.add(channelModel.get(Integer.parseInt(clusterHeadID)));
    ArrayList<String> childEqNodes = logicaloverlayNetwork.getEquivilentNodes(clusterHeadID);
    Iterator<String> inputEquivNodesStringID = childEqNodes.iterator();
    while(inputEquivNodesStringID.hasNext())
    {
      String inputEquivNodeStringID = inputEquivNodesStringID.next();
      sites.add(channelModel.get(Integer.parseInt(inputEquivNodeStringID)));
    }
    return sites;
  }

  /**
   * executes a transmission task for the model
   * @param destID
   * @param siteID
   * @param packetID
   */
  private void tryTransmission(String destID, Integer siteID, int packetID)
  {
    //collect all output sites
    ArrayList<ChannelModelSite> outputSites = getListOfChannelSitesFromLogicaloverlay(destID, true);
    Iterator<ChannelModelSite> siteiterator = outputSites.iterator();
    while(siteiterator.hasNext())
    {
      ChannelModelSite outputSite = siteiterator.next();
      if(didPacketGetRecived(destID, siteID))
      {
        outputSite.recivedInputPacket(siteID.toString(), packetID);
      }
    }
  }

  /**
   * checks with the noise model to see if the packet was recieved at the destimation.
   * @param destID
   * @param siteID
   * @return
   */
  private boolean didPacketGetRecived(String destID, Integer sourceID)
  {
    return noiseModel.packetRecieved(sourceID.toString(), destID);
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
      String clusterHeadID = logicaloverlayNetwork.getClusterHeadFor(siteID);
      Iterator<Node> inputNodes = logicaloverlayNetwork.getQep().getRT().getSite(clusterHeadID).getInputsList().iterator();
      HashMap<String, Integer> expectedPackets = new  HashMap<String, Integer>();
      while(inputNodes.hasNext())
      {
        Node input = inputNodes.next();
        Node agendaInput = agenda.getSiteByID((Site) input);
        CommunicationTask task = agenda.getTransmissionTask(agendaInput);
        int packetsToRecieve = task.getMaxPacektsTransmitted();
        expectedPackets.put(input.getID(), packetsToRecieve);
        Iterator<String> equivNodes = logicaloverlayNetwork.getEquivilentNodes(input.getID()).iterator();
        while(equivNodes.hasNext())
        {
          String equivNode = equivNodes.next();
          Node agendaEquivInput = agenda.getSiteByID(equivNode);
          task = agenda.getTransmissionTask(agendaEquivInput);
          packetsToRecieve = task.getMaxPacektsTransmitted();
          expectedPackets.put(equivNode, packetsToRecieve);
        } 
      }
      if(logicaloverlayNetwork.getQep().getRT().getRoot().getID().equals(clusterHeadID))
      {
        ChannelModelSite site = new ChannelModelSite(expectedPackets, 0, siteID, logicaloverlayNetwork, 0);
        channelModel.set(siteIDInt, site);
      }
      else
      {
        Node output = logicaloverlayNetwork.getQep().getRT().getSite(clusterHeadID).getOutput(0);
        int parents = logicaloverlayNetwork.getEquivilentNodes(output.getID()).size() + 1;
        ChannelModelSite site;
        if(logicaloverlayNetwork.isClusterHead(siteID))
          site = new ChannelModelSite(expectedPackets, parents, siteID, logicaloverlayNetwork, 0);
        else
        {
          ArrayList<String> nodes = logicaloverlayNetwork.getEquivilentNodes(logicaloverlayNetwork.getClusterHeadFor(siteID));
          int position = nodes.indexOf(siteID) + 1;
          site = new ChannelModelSite(expectedPackets, parents, siteID, logicaloverlayNetwork, position);
        }
          
        channelModel.set(siteIDInt, site);
      }
    }
  }
  
  /**
   * Used by the choice assessor to determine if a node needs to transmit its data
   * @param siteID
   * @return
   */
  public boolean needToTransmit(Integer siteID)
  {
    if(failedNodes.contains(siteID.toString()))
      return false;
    return channelModel.get(siteID).energyModelNeedToTransmit();
  }
  
  /**Used by the choice assessor to determine if a node needs to listen to a node
   * 
   * @param childID
   * @param siteID
   * @return
   */
  public boolean needToListenTo(String childID, Integer siteID)
  {
    if(failedNodes.contains(siteID.toString()) || 
       failedNodes.contains(childID) )
      return false;
    return channelModel.get(siteID).needToListenTo(childID);
  }
  
  /**
   * Used by the choice assessor to determine if a node needs to run its fragments
   * @param siteID
   * @return
   */
  public boolean needToRunFrags(int siteID)
  {
    return needToTransmit(siteID);
  }
  
  /**
   * Used by the choice assessor to determine if a node needs to transmit a ack
   * @param siteID
   * @param child
   * @return
   */
  public boolean needToTransmitACK(Integer siteID, String child)
  {
    if(failedNodes.contains(siteID.toString())|| 
       failedNodes.contains(child))
      return false;
    String clusterHead = this.logicaloverlayNetwork.getClusterHeadFor(child);
    return channelModel.get(siteID).needToTransmitAckTo(clusterHead);
  }

  /**
   * Used by the choice assessor to determine if a node needs to listen for a ack
   * @param source
   * @param child
   * @return
   */
  public boolean recievedACK(Integer source, String child)
  {
    if(failedNodes.contains(source.toString())|| 
       failedNodes.contains(child))
      return false;
    ChannelModelSite sourceSite = channelModel.get(source);
    ChannelModelSite destSite = channelModel.get(Integer.parseInt(child));
    if(sourceSite.getEnergyModeltransmittedAcks() <= destSite.getPosition())
      return true;
    else 
      return false;
    
      
    
  }
  
}
