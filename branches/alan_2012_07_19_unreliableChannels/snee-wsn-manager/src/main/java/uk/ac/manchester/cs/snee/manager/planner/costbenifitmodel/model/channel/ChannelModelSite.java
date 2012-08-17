package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;

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
  private String siteID;
  
  
  public ChannelModelSite(HashMap<String, Integer> expectedPackets, int parents, String siteID)
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
  }
  
  public void recivedInputPacket(String source, int packetID)
  {
    ArrayList<Boolean> packets = arrivedPackets.get(source);
    arrivedPackets.remove(source);
    packets.set(packetID, true);
    arrivedPackets.put(source, packets);
    needToListenTo.add(source);
  }
  
  public void receivedACK()
  {
    recievedAcks++;
  }
  
  public boolean needToTransmit()
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
  
  public boolean needToTransmitAckTo(String child)
  {
    ArrayList<Boolean> packets = arrivedPackets.get(child);
    Iterator<Boolean> packetIterator = packets.iterator();
    while(packetIterator.hasNext())
    {
      Boolean packetRecieved = packetIterator.next();
      if(!packetRecieved)
        return false;
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
}
