package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved.LogicalOverlayNetworkHierarchy;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved.UnreliableChannelAgendaReduced;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class ChannelModelReduced implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = 7449374992445910226L;
  private ArrayList<ChannelModelReducedSite> channelModel = new ArrayList<ChannelModelReducedSite>();
  private LogicalOverlayNetworkHierarchy logicaloverlayNetwork;
  private UnreliableChannelAgendaReduced agenda;
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
    public ChannelModelReduced (LogicalOverlayNetworkHierarchy logicaloverlayNetwork,  
                       UnreliableChannelAgendaReduced agenda, int networkSize,
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
    for(int index = 0; index <= networkSize; index++)
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
            if(isSibling(cTask.getSourceID(), cTask.getDestID())) 
            {
              int packetsToTransmit = cTask.getMaxPacektsTransmitted();
              for(int packetID = 0; packetID < packetsToTransmit; packetID++)
              {
                tryRecievingSibling(cTask.getDestID(), Integer.parseInt(cTask.getSourceID()), packetID);
              }
              cTask.setRan(true);
            }
            else
            {
              int packetsToTransmit = cTask.getMaxPacektsTransmitted();
               for(int packetID = 0; packetID < packetsToTransmit; packetID++)
               {
                 tryRecieving(cTask.getDestID(), Integer.parseInt(cTask.getSourceID()), packetID);
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
          ChannelModelReducedSite source = channelModel.get(Integer.parseInt(cTask.getSourceID()));
          
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
          ChannelModelReducedSite sourceSite = channelModel.get(Integer.parseInt(cTask.getSourceID())); 
          ChannelModelReducedSite destSite = channelModel.get(Integer.parseInt(cTask.getDestNode().getID()));
          ChannelModelReducedSite newSite = channelModel.get(Integer.parseInt(cTask.getSiteID()));
          
          
          if(destSite.getTask(cTask.getStartTime(), CommunicationTask.ACKTRANSMIT).isRan())
            if(!didPacketGetRecived(destSite.toString(), Integer.parseInt(sourceSite.toString())))
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
          ChannelModelReducedSite site = channelModel.get(Integer.parseInt(cTask.getDestID())); 
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
  }
  
  private boolean tranmissionTaskRan(CommunicationTask cTask)
  {
    ChannelModelReducedSite site = channelModel.get(Integer.parseInt(cTask.getSourceID())); 
    return site.getTask(cTask.getStartTime(), CommunicationTask.TRANSMIT).isRan();
  }

  private boolean isSibling(String sourceID, String destID)
  {
    ArrayList<String> equivNodes = 
      this.logicaloverlayNetwork.getEquivilentNodes(this.logicaloverlayNetwork.getClusterHeadFor(sourceID));
    if(equivNodes.contains(destID) || this.logicaloverlayNetwork.getClusterHeadFor(sourceID).equals(destID))
      return true;
    else
      return false;
  }

  public boolean heardHigherPriorityNodeTransmit(ChannelModelReducedSite site, boolean redundant)
  {
    if(site.heardSiblings() == 0)
      return false;
    else
      return true;
  }

  private void tryRecieving(String destID, Integer sourceID, int packetID)
  {
    ChannelModelReducedSite outputSite = this.channelModel.get(Integer.parseInt(destID));
    if(didPacketGetRecived(destID, sourceID))
    {
      outputSite.recivedInputPacket(sourceID.toString(), packetID);
    }
  }
  
  private void tryRecievingSibling(String destID, Integer sourceID, int packetID)
  {
    ChannelModelReducedSite outputSite = this.channelModel.get(Integer.parseInt(destID));
    if(didPacketGetRecived(destID, sourceID))
    {
      outputSite.recivedSiblingPacket(sourceID.toString(), packetID);
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
        ChannelModelReducedSite site = 
          new ChannelModelReducedSite(expectedPackets, siteID, logicaloverlayNetwork, 0, 
                                      agenda.getTasks().get(agenda.getSiteByID(siteID)));
        channelModel.set(siteIDInt, site);
      }
      else
      {
        ChannelModelReducedSite site;
        if(logicaloverlayNetwork.isClusterHead(siteID))
          site = 
            new ChannelModelReducedSite(expectedPackets, siteID, logicaloverlayNetwork, 
                logicaloverlayNetwork.getPriority(siteID), agenda.getTasks().get(agenda.getSiteByID(siteID)));
        else
        {
          site =
            new ChannelModelReducedSite(expectedPackets, siteID, logicaloverlayNetwork, 
                logicaloverlayNetwork.getPriority(siteID), agenda.getTasks().get(agenda.getSiteByID(siteID)));
        }
          
        channelModel.set(siteIDInt, site);
      }
    }
  }
}
