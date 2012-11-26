package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.CostModelDataStructure;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityDataStructure;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.LogicalOverlayNetworkHierarchy;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;

public class ChannelModelSite implements Serializable
{
  /**
   * serilised id
   */
  private static final long serialVersionUID = 378104895960678188L;
  private HashMap<String, ArrayList<Boolean>> arrivedPackets = new HashMap<String, ArrayList<Boolean>>();
  private HashMap<String, Integer> expectedPackets = null;
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
  private long beta;
  private static CostParameters costs;
  private static boolean reliableChannelQEP = false;
  private static CardinalityEstimatedCostModel cardModel = null;
  
  /**
   * constructor for a channel model site
   * @param expectedPackets
   * @param parents
   * @param siteID
   * @param overlayNetwork
   * @param position
   */
  public ChannelModelSite(HashMap<String, Integer> expectedPackets, String siteID,
                                 LogicalOverlayNetworkHierarchy overlayNetwork, int position,
                                 ArrayList<Task> tasks, Long beta, CostParameters costs)
  {
    this.expectedPackets = expectedPackets;
    setupExpectedPackets();
    this.siteID = siteID;
    this.overlayNetwork = overlayNetwork;
    cardModel = new CardinalityEstimatedCostModel(overlayNetwork.getQep());
    this.priority = position;
    this.beta = beta;
    ChannelModelSite.costs = costs;
    this.tasks.addAll(tasks);
  }
  
  private void setupExpectedPackets()
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

  /**
   * determines how many packets are being transmitted from this site, given 
   * recievedPacekt count and operators on site.
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  public Integer transmittablePackets(IOT IOT) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Site site = IOT.getSiteFromID(siteID);
    ArrayList<InstanceOperator> operators = 
      IOT.getOpInstances(site, TraversalOrder.POST_ORDER, true);
    Iterator<InstanceOperator> operatorIterator = operators.iterator();
    int transmittablePacketsCount = 0;
    int currentPacketCount = 0;
    int previousOpOutput = 0;
    String preivousOutputExtent = null;
    InstanceOperator previousOp = null;
    HashMap<String, Integer> packets = new HashMap<String, Integer>();
    while(operatorIterator.hasNext())
    {
      InstanceOperator op = operatorIterator.next();
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        if(exOp.getComponentType().equals(ExchangePartType.PRODUCER) &&
           !exOp.getNext().getSite().getID().equals(exOp.getSite().getID()))
        {
          transmittablePacketsCount += tupleToPacketConversion(previousOpOutput, previousOp);
          exOp.setExtent(preivousOutputExtent);
          exOp.setTupleValueForExtent(previousOpOutput);
        }
        if(exOp.getComponentType().equals(ExchangePartType.RELAY))
        {
          InstanceExchangePart preExOp = exOp.getPrevious();
          Integer tupleCount = preExOp.getExtentTupleValue();
          Integer packetCount = this.tupleToPacketConversion(tupleCount, preExOp);
          transmittablePacketsCount += recievedPacketCount(IOT, currentPacketCount, packetCount);
          currentPacketCount += exOp.getmaxPackets(IOT.getDAF(), beta, costs);
          exOp.setTupleValueForExtent(ChannelModelSite.packetToTupleConversion(transmittablePacketsCount, exOp, preExOp));
        }
        if(exOp.getComponentType().equals(ExchangePartType.CONSUMER) &&
           previousOp == null )
        {
          Integer packetsOfSameExtent = packets.get(exOp.getExtent());
          if(packetsOfSameExtent == null)
            packetsOfSameExtent = 0;
          InstanceExchangePart preExOp = exOp.getPrevious();
          Integer tupleCount = preExOp.getExtentTupleValue();
          Integer packetCount = this.tupleToPacketConversion(tupleCount, preExOp);
          this.tupleToPacketConversion(tupleCount, exOp);
          packetsOfSameExtent += recievedPacketCount(IOT, currentPacketCount, packetCount);
          packets.remove(exOp.getExtent());
          packets.put(exOp.getExtent(), packetsOfSameExtent );
          currentPacketCount += exOp.getmaxPackets(IOT.getDAF(), beta, costs);
        }
        if(exOp.getComponentType().equals(ExchangePartType.CONSUMER) &&
            previousOp != null)
         {
          packets.put(preivousOutputExtent, previousOpOutput);
          this.tupleToPacketConversion(previousOpOutput, exOp);
         }
      }
      else
      {
        //if acquire, then need to determine tuples
        if(op.getSensornetOperator() instanceof SensornetAcquireOperator)
        {
          previousOpOutput = (int) (1* beta);
          List<Attribute> attributes = op.getSensornetOperator().getLogicalOperator().getAttributes();
          preivousOutputExtent = attributes.get(1).toString();
          previousOp = op;
        }
        else
        {
          ArrayList<Integer> opTuples = new ArrayList<Integer>();
          if(previousOp == null)
          {
            HashMap<String, Integer> extentTuples = packetToTupleConversion(packets, op, op.getInstanceInput(0));
            Iterator<String> packetIterator = extentTuples.keySet().iterator();
            while(packetIterator.hasNext())
            {
              String key = packetIterator.next();
              opTuples.add(extentTuples.get(key));
            }
          }
          else
          {
            packets.put(preivousOutputExtent, previousOpOutput);
          }
          CostModelDataStructure outputs = cardModel.model(op, opTuples);
          CardinalityDataStructure outputCard = (CardinalityDataStructure) outputs;
          previousOpOutput = (int) outputCard.getCard();
          previousOp = op;
          preivousOutputExtent = packets.toString();
          packets.clear();
          int noPackets = this.tupleToPacketConversion(previousOpOutput, op);
          transmittablePacketsCount += noPackets;
        } 
      }
    }
    return transmittablePacketsCount;
  }

  private int tupleToPacketConversion(int noTuples, InstanceOperator op)
  throws SchemaMetadataException, TypeMappingException
  {
    if(ChannelModelSite.reliableChannelQEP)
    {
      int tupleSize = 0;
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
      }
      else
        tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
      int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
      int payloadOverhead = costs.getPayloadOverhead() + 8;
      int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
      int pacekts = noTuples / numTuplesPerMessage;
      
      if(noTuples %  numTuplesPerMessage == 0)
        op.setLastPacketTupleCount(numTuplesPerMessage);
      else
        op.setLastPacketTupleCount(noTuples %  numTuplesPerMessage);
      return pacekts;
    }
    else
    {
      int tupleSize = 0;
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
      }
      else
        tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
      int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
      int payloadOverhead = costs.getPayloadOverhead();
      int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
      Double frac = new Double(noTuples) / new Double(numTuplesPerMessage);
      Double packetsD = Math.ceil(frac);
      int pacekts = packetsD.intValue();
      if(noTuples %  numTuplesPerMessage == 0)
        op.setLastPacketTupleCount(numTuplesPerMessage);
      else
        op.setLastPacketTupleCount(noTuples %  numTuplesPerMessage);
      return pacekts;
    }
  }

  public static HashMap<String, Integer> packetToTupleConversion(
                                             HashMap<String, Integer> packets, 
                                             InstanceOperator op, InstanceOperator preOp) 
  throws SchemaMetadataException, TypeMappingException
  {
    
    HashMap<String, Integer> tuples = new HashMap<String, Integer>();
    Iterator<String> packetCountInterator = packets.keySet().iterator();
    while(packetCountInterator.hasNext())
    {
      String key = packetCountInterator.next();
      Integer noPackets = packets.get(key);
      if(ChannelModelSite.reliableChannelQEP)
      {
        int tupleSize = 0;
        if(op instanceof InstanceExchangePart)
        {
          InstanceExchangePart exOp = (InstanceExchangePart) op;
          tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
        }
        else
          tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
        int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
        int payloadOverhead = costs.getPayloadOverhead() + 8;
        int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
        int tuplesForExchange = (noPackets -1) * numTuplesPerMessage;
        int lastPacketTupleCount = preOp.getLastPacketTupleCount();
        tuplesForExchange += lastPacketTupleCount;
        tuples.put(key, tuplesForExchange);
      }
      else
      {
        int tupleSize = 0;
        if(op instanceof InstanceExchangePart)
        {
          InstanceExchangePart exOp = (InstanceExchangePart) op;
          tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
        }
        else
          tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
        int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
        int payloadOverhead = costs.getPayloadOverhead();
        int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
        int tuplesForExchange = (noPackets -1) * numTuplesPerMessage;
        int lastPacketTupleCount = preOp.getLastPacketTupleCount();
        tuplesForExchange += lastPacketTupleCount;
        tuples.put(key, tuplesForExchange);
      }
    }
    return tuples;
  }
  
  /**
   * 
   * @param packets
   * @param op
   * @return
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   */
  public static Integer packetToTupleConversion( Integer noPackets, 
                                                 InstanceOperator op,
                                                 InstanceOperator preOp) 
  throws SchemaMetadataException, TypeMappingException
  {
    if(ChannelModelSite.reliableChannelQEP)
    {
      int tupleSize = 0;
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
      }
      else
        tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
      int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
      int payloadOverhead = costs.getPayloadOverhead() + 8;
      int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
      int tuplesForExchange = (noPackets -1) * numTuplesPerMessage;
      int lastPacketTupleCount = preOp.getLastPacketTupleCount();
      tuplesForExchange += lastPacketTupleCount;
      return tuplesForExchange;
    }
    else
    {
      int tupleSize = 0;
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
      }
      else
        tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
      int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
      int payloadOverhead = costs.getPayloadOverhead();
      int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
      int tuplesForExchange = (noPackets -1) * numTuplesPerMessage;
      int lastPacketTupleCount = preOp.getLastPacketTupleCount();
      tuplesForExchange += lastPacketTupleCount;
      return tuplesForExchange;
    }
  } 

  /**
   * counts how many packets were actually recieved (given packet ids. No duplicates)
   * @param iOT 
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private int recievedPacketCount(IOT IOT, int startingPoint, int maxPackets) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    //locate the child cluster heads
    Set<String> clusterHeadIds = new HashSet<String>();
    Iterator<String> inputPacketKeys = arrivedPackets.keySet().iterator();
    while(inputPacketKeys.hasNext())
    {
      String child = inputPacketKeys.next();
      clusterHeadIds.add(this.overlayNetwork.getClusterHeadFor(child));
    }
  
    int packetsRecieved = 0;
    inputPacketKeys = clusterHeadIds.iterator();
    while(inputPacketKeys.hasNext())
    {
      String child = inputPacketKeys.next();
      ArrayList<Boolean> packets = arrivedPackets.get(child);
      int index = 0;
      int firstPoint = startingPoint;
      int packetsSent = 0;
      Iterator<Boolean> packetIterator = packets.iterator();
      int counter = 0;
      while(packetIterator.hasNext())
      {
        Boolean packetRecieved = packetIterator.next();
        if(firstPoint >= index)
        {
          if(!packetRecieved)
          {
            Iterator<String> EquivNodes = 
              this.overlayNetwork.getActiveNodesInRankedOrder(this.overlayNetwork.getClusterHeadFor(child)).iterator();
            while(EquivNodes.hasNext())
            {
              String EquivNode = EquivNodes.next();
              ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
              if(equivPackets.get(counter))
                packetRecieved = true;
            }
          }
          if(packetRecieved)
            packetsRecieved++;
          packetsSent++;
          if(packetsSent == maxPackets)
          {
            return packetsRecieved;
          }
        }
        else
          index++;
        counter++;
        packetRecieved = false;
      }
    }
    return packetsRecieved;
  }

  /**
   * allows the model to determine if which model to use for packet to tuple conversion
   * @param reliableChannelQEP
   */
  public static void setReliableChannelQEP(boolean reliableChannelQEP)
  {
    ChannelModelSite.reliableChannelQEP = reliableChannelQEP;
  }

  /**
   * clears stores so ready for new iteration
   */
  public void clearDataStoresForNewIteration()
  {
    this.arrivedPackets.clear();
    this.noOfTransmissions = 0;
    this.receivedAcksFrom.clear();
    this.transmittedAcksTo.clear();
    this.siblingTransmissions = 0;
    setupExpectedPackets();
  }
}
