package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.reducedchannel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;

public class ChannelModelSite implements Serializable
{
  /**
   * serilised id
   */
  private static final long serialVersionUID = 378104895960678188L;
  private HashMap<String, ArrayList<Boolean>> arrivedPackets = new HashMap<String, ArrayList<Boolean>>();
  private int recievedAcks;
  private int parents;
  private ArrayList<String> needToListenTo = new ArrayList<String>();
  private ArrayList<String> receivedAcksFrom = new ArrayList<String>();
  private boolean needToTransmit = false;
  private String siteID;
  private boolean needsToSendACK = true;
  private int transmittedAcks;
  private int energyModeltransmittedAcks;
  private int energyModelRecievedAcks;
  private LogicalOverlayNetwork overlayNetwork;
  private int position;
  
  /**
   * constructor for a channel model site
   * @param expectedPackets
   * @param parents
   * @param siteID
   * @param overlayNetwork
   * @param position
   */
  public ChannelModelSite(HashMap<String, Integer> expectedPackets, int parents, String siteID,
                          LogicalOverlayNetwork overlayNetwork, int position)
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
    this.parents = parents;
    this.siteID = siteID;
    this.overlayNetwork = overlayNetwork;
    this.position = position;
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
   * tracks what nodes this node has recieved acks from
   * @param outputSite
   */
  public void receivedACK(ChannelModelSite outputSite)
  {
    recievedAcks++;
    this.receivedAcksFrom.add(outputSite.toString());
  }
  
  /**
   * checks if the node needs to transmit packets for the running of the model.
   * @return
   */
  public boolean channelModelNeedToTransmit()
  {
    if(recievedAcks == parents)
    {
      return false;
    }
    else
    {
      return true;
    }
  }
  
  /**
   * for the chocie assessor, to check if it needs to transmit packets
   * @return
   */
  public boolean energyModelNeedToTransmit()
  {
    return needToTransmit;
  }
  
  /**
   * checks if a node needs to send a ack down to a child.
   * @param child
   * @return
   */
  public boolean needToTransmitAckTo(String child)
  {
    ArrayList<Boolean> packets = arrivedPackets.get(child);
    Iterator<Boolean> packetIterator = packets.iterator();
    int counter = 0;
    while(packetIterator.hasNext())
    {
      Boolean packetRecieved = packetIterator.next();
      if(!packetRecieved)
      {
        Iterator<String> EquivNodes = this.overlayNetwork.getEquivilentNodes(child).iterator();
        boolean found = false;
        while(EquivNodes.hasNext())
        {
          String EquivNode = EquivNodes.next();
          ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
          if(equivPackets.get(counter))
            found = true;
        }
        if(!found)
          return false;
      }
      counter++;
    }
    if(energyModeltransmittedAcks <= transmittedAcks)
    {
      this.incrementEnergyModeltransmittedAcks();
      return true;
    }
    else 
      return false;
  }
  
  
  public boolean needToListenTo(String childID)
  {
    return needToListenTo.contains(childID);
  }
  
  public String toString()
  {
    return siteID;
  }

  public void setNeedTransmit()
  {
    needToTransmit = true;
  }
  
  public boolean energyModelNeedsToTransmitACK()
  {
    return this.needsToSendACK;
  }
  
  public void channelModelSetToTransmitACK(boolean need)
  {
    this.needsToSendACK = need;
  }

  public void incrementTransmittedAcks()
  {
    this.transmittedAcks++;
  }

  public int getTransmittedAcks()
  {
    return transmittedAcks;
  }

  public void incrementEnergyModeltransmittedAcks()
  {
    this.energyModeltransmittedAcks++;
  }

  public int getEnergyModeltransmittedAcks()
  {
    return energyModeltransmittedAcks;
  }

  /**
   * checks if a node has revieced a ack from a parent node
   * @param parentID
   * @return
   */
  public boolean receivedACK(Integer parentID)
  {
    if(receivedAcksFrom.contains(parentID.toString()) &&
       this.energyModelRecievedAcks < this.recievedAcks)
    {
      this.energyModelRecievedAcks++;
      return true;
    }
    return false;
  }

  public int getPosition()
  {
    return this.position;
  }
}
