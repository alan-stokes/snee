package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
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
  
  public ChannelModel (LogicalOverlayNetwork logicaloverlayNetwork,  
                       UnreliableChannelAgenda agenda, int networkSize,
                       ArrayList<String> failedNodes)
  {
    this.agenda = agenda;
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    this.failedNodes = failedNodes;
    setupEmptyArray(networkSize + 1);
    createChannelSites();
    runModel();
  }

  private void setupEmptyArray(int networkSize)
  {
    for(int index = 0; index < networkSize; index++)
    {
      channelModel.add(null);
    }
  }

  private void runModel()
  {
    Iterator<Task> taskIterator =  agenda.taskIteratorOrderedByTime();
    while(taskIterator.hasNext())
    {
      Task task = taskIterator.next();
      int siteID = Integer.parseInt(task.getSite().getID());
      if(task instanceof CommunicationTask)
      {
        CommunicationTask cTask = (CommunicationTask) task;
        System.out.println("task = " + cTask.toString() + " at " + cTask.getStartTime());
        int packetsToTransmit = cTask.getMaxPacektsTransmitted();
        
        if(!failedNodes.contains(cTask.getSourceID()) && 
           !failedNodes.contains(cTask.getDestID()) &&
            tranmissionTaskNeeded(cTask.getSourceID()))
        {
          for(int packetID = 0; packetID < packetsToTransmit; packetID++)
          {
            System.out.println("Try sending data from " + cTask.getSourceID() + " to "  +cTask.getDestID());
            tryTransmission(cTask.getDestID(), Integer.parseInt(cTask.getSourceID()), packetID);
          }
          verifyAck(cTask.getDestID(), cTask.getSourceID());
        }
      }
    }
  }

  private boolean tranmissionTaskNeeded(String sourceID)
  {
    ChannelModelSite site = channelModel.get(Integer.parseInt(sourceID)); 
    if(site == null)
      return false;
    return site.needToTransmit();
  }

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
        if(didPacketGetRecived(outputSite.toString(), Integer.parseInt(childClusterHead.toString())))
        {
          childClusterHead.receivedACK();
        }
        Iterator<ChannelModelSite> childSiteIterator = inputSites.iterator();
        while(childSiteIterator.hasNext())
        {
          ChannelModelSite childSite = childSiteIterator.next();
          if(didPacketGetRecived(outputSite.toString(), Integer.parseInt(childSite.toString())))
            childSite.receivedACK();
        }
      }
    }
  }

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
        String childClusterHeadid = logicaloverlayNetwork.getClusterHeadFor(siteID.toString());
        if(outputSite == null || childClusterHeadid == null )
          System.out.println();
        outputSite.recivedInputPacket(childClusterHeadid, packetID);
      }
    }
  }

  private boolean didPacketGetRecived(String destID, int siteID)
  {
    try
    {
      boolean cleanRadioChannels = 
        SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_CLEANRADIO);
      boolean relibaleChannels = 
        SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS);
    }
    catch (SNEEConfigurationException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return true;
  }

  private void createChannelSites()
  {
    Iterator<String> siteIDIterator = logicaloverlayNetwork.siteIdIterator();
    while(siteIDIterator.hasNext())
    {
      String siteID = siteIDIterator.next();
      System.out.println("node id " + siteID);
      Integer siteIDInt = Integer.parseInt(siteID);
      String clusterHeadID = logicaloverlayNetwork.getClusterHeadFor(siteID);
      System.out.println(siteID);
      Iterator<Node> inputNodes = logicaloverlayNetwork.getQep().getRT().getSite(clusterHeadID).getInputsList().iterator();
      HashMap<String, Integer> expectedPackets = new  HashMap<String, Integer>();
      while(inputNodes.hasNext())
      {
        Node input = inputNodes.next();
        System.out.println("finding transmission task for " + input.getID());
        Node agendaInput = agenda.getSiteByID((Site) input);
        CommunicationTask task = agenda.getTransmissionTask(agendaInput);
        if(task == null)
          System.out.println();
        int packetsToRecieve = task.getMaxPacektsTransmitted();
        expectedPackets.put(input.getID(), packetsToRecieve);
      }
      if(logicaloverlayNetwork.getQep().getRT().getRoot().getID().equals(clusterHeadID))
      {
        ChannelModelSite site = new ChannelModelSite(expectedPackets, 0, siteID);
        channelModel.set(siteIDInt, site);
      }
      else
      {
        Node output = logicaloverlayNetwork.getQep().getRT().getSite(clusterHeadID).getOutput(0);
        int parents = logicaloverlayNetwork.getEquivilentNodes(output.getID()).size() + 1;
        ChannelModelSite site = new ChannelModelSite(expectedPackets, parents, siteID);
        channelModel.set(siteIDInt, site);
      }
    }
  }
  
  public boolean needToTransmit(Integer siteID)
  {
    if(failedNodes.contains(siteID.toString()))
      return false;
    return channelModel.get(siteID).needToTransmit();
  }
  
  public boolean needToListenTo(String childID, Integer siteID)
  {
    if(failedNodes.contains(siteID.toString()) || 
       failedNodes.contains(childID) )
      return false;
    return channelModel.get(siteID).needToListenTo(childID);
  }
  
  public boolean needToRunFrags(int siteID)
  {
    return needToTransmit(siteID);
  }
  
}
