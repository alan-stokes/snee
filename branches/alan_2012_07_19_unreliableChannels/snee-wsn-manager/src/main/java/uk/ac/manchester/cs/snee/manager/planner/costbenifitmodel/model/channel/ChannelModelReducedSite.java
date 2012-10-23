package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved.LogicalOverlayNetworkHierarchy;

public class ChannelModelReducedSite implements Serializable
{
  /**
   * serilised id
   */
  private static final long serialVersionUID = 378104895960678188L;
  private HashMap<String, ArrayList<Boolean>> arrivedPackets = new HashMap<String, ArrayList<Boolean>>();
  private int recievedAcks;
  private ArrayList<String> needToListenTo = new ArrayList<String>();
  private ArrayList<String> receivedAcksFrom = new ArrayList<String>();
  private ArrayList<Task> tasks = new ArrayList<Task>();
  private ArrayList<String> transmittedAcksTo = new ArrayList<String>();
  private int  noOfTransmissions;
  private String siteID;
  private LogicalOverlayNetworkHierarchy overlayNetwork;
  private int priority;
  private int cycle = 0;
  private int siblingTransmissions = 0;
  
  /**
   * constructor for a channel model site
   * @param expectedPackets
   * @param parents
   * @param siteID
   * @param overlayNetwork
   * @param position
   */
  public ChannelModelReducedSite(HashMap<String, Integer> expectedPackets, String siteID,
                                 LogicalOverlayNetworkHierarchy overlayNetwork, int position,
                                 ArrayList<Task> tasks)
  {
    Iterator<String> keys = expectedPackets.keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      int sizeOfArray = expectedPackets.get(key);
      ArrayList<Boolean> packets = new ArrayList<Boolean>(sizeOfArray);
      for(int index = 0; index < sizeOfArray; index++)
      {
        packets.add(index, false);
      }
      arrivedPackets.put(key, packets);
    }
    this.siteID = siteID;
    this.overlayNetwork = overlayNetwork;
    this.priority = position;
    this.tasks.addAll(tasks);
  }
  
  /**
   * tracks input packets from specific nodes
   * @param source
   * @param packetID
   */
  public void recivedInputPacket(String source, int packetID)
  {
    ArrayList<Boolean> packets = arrivedPackets.get(source);
    arrivedPackets.remove(source);
    packets.set(packetID, true);
    arrivedPackets.put(source, packets);
    if(!needToListenTo.contains(source))
      needToListenTo.add(source);
  }
  
  /**
   * checks if a node needs to send a ack down to a child.
   * @param child
   * @return
   */
  public boolean needToTransmitAckTo(String child)
  {
    ArrayList<Boolean> packets = arrivedPackets.get(child);
    if(packets == null)
      System.out.println();
    Iterator<Boolean> packetIterator = packets.iterator();
    int counter = 0;
    while(packetIterator.hasNext())
    {
      Boolean packetRecieved = packetIterator.next();
      if(!packetRecieved)
      {
        ArrayList<String> equivNodes = this.overlayNetwork.getEquivilentNodes(this.overlayNetwork.getClusterHeadFor(child));
        equivNodes.add(this.overlayNetwork.getClusterHeadFor(child));
        Iterator<String> equivNodesIterator = equivNodes.iterator();
        boolean found = false;
        while(equivNodesIterator.hasNext())
        {
          String EquivNode = equivNodesIterator.next();
          ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
          if(equivPackets.get(counter))
            found = true;
        }
        if(!found)
          return false;
      }
      counter++;
    }
    if(this.transmittedAcksTo.contains(this.overlayNetwork.getClusterHeadFor(child)))
      return false;
    else
    {
      return true;
    }
  }
  
  public boolean recievedAllInputPackets()
  {
    Iterator<String> inputPacketKeys = arrivedPackets.keySet().iterator();
    while(inputPacketKeys.hasNext())
    {
      String child = inputPacketKeys.next();
      ArrayList<Boolean> packets = arrivedPackets.get(child);
      Iterator<Boolean> packetIterator = packets.iterator();
      int counter = 0;
      while(packetIterator.hasNext())
      {
        Boolean packetRecieved = packetIterator.next();
        if(!packetRecieved)
        {
          Iterator<String> EquivNodes = 
            this.overlayNetwork.getActiveNodesInRankedOrder(this.overlayNetwork.getClusterHeadFor(child)).iterator();
          boolean found = false;
          while(EquivNodes.hasNext())
          {
            String EquivNode = EquivNodes.next();
            ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
            if(equivPackets.get(counter))
              found = true;
          }
          if(!found)
          {
            return false;
          }
        }
        counter++;
      }
    }
    return true;
  }
  
  
  public boolean needToListenTo(String childID)
  {
    return needToListenTo.contains(childID);
  }
  
  public String toString()
  {
    return siteID;
  }

  public void transmittedPackets()
  {
    this.noOfTransmissions++;
  }
  
  /**
   * checks if a node has revieced a ack from a node
   * @param parentID
   * @return
   */
  public boolean receivedACK(Integer parentID)
  {
    return receivedAcksFrom.contains(parentID.toString());
  }

  public int getPosition()
  {
    return this.priority;
  }

  public void TransmitAckTo(String originalDestNode)
  {
    this.transmittedAcksTo.add(this.overlayNetwork.getClusterHeadFor(originalDestNode));
  }

  public boolean transmittedTo(boolean redundantTask)
  {
    if(redundantTask)
      if(this.noOfTransmissions == 2)
        return true;
      else
        return false;
    else
      if(this.noOfTransmissions == 1)
        return true;
      else
        return false;
  }

  public int getCycle()
  {
    return this.cycle;
  }

  public void updateCycle()
  {
    this.cycle++;
  }

  public void recivedSiblingPacket(String string, int packetID)
  {
    siblingTransmissions++;
  }
  
  public int heardSiblings()
  {
    return siblingTransmissions;
  }

  public boolean transmittedAckTo(String sourceID)
  {
    return this.transmittedAcksTo.contains(sourceID);
  }

  public void receivedAckFrom(String commID)
  {
    this.receivedAcksFrom.add(commID);
  }

  public CommunicationTask getTask(long startTime, int mode)
  {
    Iterator<Task> taskIterator = this.tasks.iterator();
    while(taskIterator.hasNext())
    {
      Task task = taskIterator.next();
      if(task instanceof CommunicationTask)
      {
        CommunicationTask commTask = (CommunicationTask) task;
        if(commTask.getMode() == mode && 
           startTime == commTask.getStartTime())
          return commTask;
      }
    }
    return null;
  } 

  public boolean receivedACK()
  {
    if(this.receivedAcksFrom.size() == 1)
      return true;
    else 
      return false;
  }
}
