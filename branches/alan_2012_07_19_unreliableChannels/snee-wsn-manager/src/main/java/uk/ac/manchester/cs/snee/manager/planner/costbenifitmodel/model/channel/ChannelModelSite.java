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
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;

public class ChannelModelSite implements Serializable
{
  /**
   * serilised id
   */
  private static final long serialVersionUID = 378104895960678188L;
  private HashMap<String, ArrayList<Boolean>> arrivedPackets = 
    new HashMap<String, ArrayList<Boolean>>();
  private HashMap<String, Integer> expectedPackets = null;
  private HashMapList<String, NoiseDataStore> noiseValuesExped = new HashMapList<String, NoiseDataStore>();
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
  
  /**
   * helper method for packet setup
   */
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
    if(packetID == 0)
      System.out.println();
    packets.set(packetID -1, true);
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
        ArrayList<String> equivNodes = 
          this.overlayNetwork.getActiveEquivilentNodes(
              this.overlayNetwork.getClusterHeadFor(child));
        equivNodes.add(this.overlayNetwork.getClusterHeadFor(child));
        Iterator<String> equivNodesIterator = equivNodes.iterator();
        boolean found = false;
        while(equivNodesIterator.hasNext())
        {
          String EquivNode = equivNodesIterator.next();
          ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
          if(equivPackets == null)
            System.out.println();
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
            this.overlayNetwork.getActiveNodesInRankedOrder(
                this.overlayNetwork.getClusterHeadFor(child)).iterator();
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
    ArrayList<Boolean> packetsFromSibling =  arrivedPackets.get(string);
    if(packetID == 0)
      System.out.println();
    packetsFromSibling.set(packetID -1, true);
    arrivedPackets.remove(string);
    arrivedPackets.put(string, packetsFromSibling);
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

  /**
   * gets the comm task scheduled at the start time otherwise returns null
   * @param startTime
   * @param mode
   * @return
   */
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

  /**
   * checks if a node has received a ACK at all
   * @return
   */
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
  public ArrayList<Integer> transmittablePackets(IOT IOT) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Site site = IOT.getSiteFromID(siteID);
    ArrayList<InstanceOperator> operators = 
      IOT.getOpInstances(site, TraversalOrder.POST_ORDER, true);
    Iterator<InstanceOperator> operatorIterator = operators.iterator();
    ArrayList<Integer> packetIds = new ArrayList<Integer>();
    HashMap<String, Integer> currentPacketCount = new HashMap<String, Integer>();
    int previousOpOutput = 0;
    HashMap<String, Integer> currentUsedRecievedPacketCount = new HashMap<String, Integer>();
    String preivousOutputExtent = null;
    InstanceOperator previousOp = null;
    HashMap<String, Integer> tuples = new HashMap<String, Integer>();
    while(operatorIterator.hasNext())
    {
      InstanceOperator op = operatorIterator.next();
      if(op instanceof InstanceExchangePart)
      {
        /*if a producer exchange operator convert tuples to packets and 
         * determine how many have been lost.
         */
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        if(exOp.getComponentType().equals(ExchangePartType.PRODUCER) &&
           !exOp.getNext().getSite().getID().equals(exOp.getSite().getID()))
        {
          int maxTransmittablePacketCount = exOp.getmaxPackets(IOT.getDAF(), beta, costs);
          int transmittablePacketsCount = tupleToPacketConversion(previousOpOutput, previousOp);
          Integer cPacketCount = currentPacketCount.get(exOp.getSite().getID());
          if(cPacketCount == null)
          {
            cPacketCount = 0; 
          }
          if(!exOp.getSourceFrag().containsOperatorType(SensornetAcquireOperator.class))
          {
            Integer rPacketCount = currentUsedRecievedPacketCount.get(exOp.getSite().getID());
            if(rPacketCount == null)
            {
              rPacketCount = 0; 
            }
            addToPacketCounts(maxTransmittablePacketCount, transmittablePacketsCount, 
                              packetIds, cPacketCount, rPacketCount);
          }
          else
          {
            Integer rPacketCount = currentUsedRecievedPacketCount.get(exOp.getSite().getID());
            if(rPacketCount == null)
            {
              rPacketCount = 0; 
            }
            addToPacketCountsLeaf(maxTransmittablePacketCount, transmittablePacketsCount, 
                                  packetIds, cPacketCount, rPacketCount);
          }
          
          currentPacketCount.remove(exOp.getSite().getID());
          currentPacketCount.put(this.overlayNetwork.getClusterHeadFor(exOp.getSite().getID()), cPacketCount + maxTransmittablePacketCount);
          exOp.setExtent(preivousOutputExtent);
          exOp.setTupleValueForExtent(previousOpOutput);
          tuples.clear();
        }
        /*if the exchange is a relay, then the packets have been recieved by this site already, 
        and need to be assessed*/
        if(exOp.getComponentType().equals(ExchangePartType.RELAY))
        {
          InstanceExchangePart preExOp = exOp.getPrevious();
          //got max packets 
          int maxTransmittablePacketCount = exOp.getmaxPackets(IOT.getDAF(), beta, costs);
          Integer rPacketCount = currentUsedRecievedPacketCount.get(preExOp.getSite().getID());
          if(rPacketCount == null)
          {
            rPacketCount = 0; 
          }
          int transmittablePacketsCount = recievedPacketCount(IOT,rPacketCount, 
                                                              maxTransmittablePacketCount,
                                                              exOp.getPrevious().getSite().getID());
          Integer cPacketCount = currentPacketCount.get(exOp.getSite().getID());
          if(cPacketCount == null)
            cPacketCount = 0;
          addToPacketCounts(maxTransmittablePacketCount, transmittablePacketsCount, 
                            packetIds, cPacketCount, rPacketCount);
          currentUsedRecievedPacketCount.remove(preExOp.getSite().getID());
          currentUsedRecievedPacketCount.put(this.overlayNetwork.getClusterHeadFor(preExOp.getSite().getID()), rPacketCount + maxTransmittablePacketCount);
          currentPacketCount.remove(exOp.getSite().getID());
          currentPacketCount.put(this.overlayNetwork.getClusterHeadFor(exOp.getSite().getID()), cPacketCount + maxTransmittablePacketCount);
          exOp.setTupleValueForExtent(
              ChannelModelSite.packetToTupleConversion(transmittablePacketsCount, exOp, preExOp));
        }
        if(exOp.getComponentType().equals(ExchangePartType.CONSUMER) &&
           previousOp == null )
        {
          Integer packetsOfSameExtent = tuples.get(exOp.getExtent());
          if(packetsOfSameExtent == null)
            packetsOfSameExtent = 0;
          int maxTransmittablePacketCount = exOp.getmaxPackets(IOT.getDAF(), beta, costs);
          Integer cPacketCount = currentPacketCount.get(exOp.getPrevious().getSite().getID());
          if(cPacketCount == null)
            cPacketCount = 0;
          InstanceExchangePart preExOp =exOp.getPrevious();
          Integer rPacketCount = currentUsedRecievedPacketCount.get(preExOp.getSite().getID());
          if(rPacketCount == null)
          {
            rPacketCount = 0; 
          }
          int transmittablePacketsCount = recievedPacketCount(IOT, rPacketCount, 
                                                              maxTransmittablePacketCount, 
                                                              exOp.getPrevious().getSite().getID());
          int tuplesRecieved = ChannelModelSite.packetToTupleConversion(transmittablePacketsCount, exOp, exOp.getPrevious());
          tuples.remove(exOp.getExtent());
          tuples.put(exOp.getExtent(), tuplesRecieved + packetsOfSameExtent);
          currentUsedRecievedPacketCount.remove(preExOp.getSite().getID());
          currentUsedRecievedPacketCount.put(this.overlayNetwork.getClusterHeadFor(preExOp.getSite().getID()), rPacketCount + maxTransmittablePacketCount);
        }
        if(exOp.getComponentType().equals(ExchangePartType.CONSUMER) &&
           previousOp != null )
       {
          previousOp = null;
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
          Integer packetsOfSameExtent = tuples.get(preivousOutputExtent);
          if(packetsOfSameExtent == null)
            packetsOfSameExtent = 0;
          tuples.remove(preivousOutputExtent);
          tuples.put(preivousOutputExtent, previousOpOutput + packetsOfSameExtent);
          
        }
        // if some operator then place though cardinality model
        if(!(op.getSensornetOperator() instanceof SensornetAcquireOperator) &&
           !(op.getSensornetOperator() instanceof SensornetDeliverOperator))
        {
          ArrayList<Integer> opTuples = new ArrayList<Integer>();
          if(previousOp == null)
          {
            HashMap<String, Integer> extentTuples = packetToTupleConversion(tuples, op, op.getInstanceInput(0));
            Iterator<String> packetIterator = extentTuples.keySet().iterator();
            while(packetIterator.hasNext())
            {
              String key = packetIterator.next();
              opTuples.add(extentTuples.get(key));
            }
          }
          else
          {
            opTuples.add(previousOpOutput);
          }
          CostModelDataStructure outputs = cardModel.model(op, opTuples);
          CardinalityDataStructure outputCard = (CardinalityDataStructure) outputs;
          previousOpOutput = (int) outputCard.getCard();
          previousOp = op;
          tuples.put(preivousOutputExtent, previousOpOutput);
          preivousOutputExtent = tuples.toString();
        }
        //if delviery oeprator calc pacvkets transmitted for system to determine tuples.
        if(op.getSensornetOperator() instanceof SensornetDeliverOperator)
        {
          ArrayList<Integer> opTuples = new ArrayList<Integer>();
          Iterator<String> packetIterator = tuples.keySet().iterator();
          while(packetIterator.hasNext())
          {
            String key = packetIterator.next();
            opTuples.add(tuples.get(key));
          }
          CostModelDataStructure outputs = cardModel.model(op, opTuples);
          CardinalityDataStructure outputCard = (CardinalityDataStructure) outputs;
          previousOpOutput = (int) outputCard.getCard();
          previousOpOutput = this.tupleToPacketConversion(previousOpOutput, op);
          packetIds.clear();
          for(int index = 0; index < previousOpOutput; index++)
            packetIds.add(0);
        }
      }
    }
    return packetIds;
  }

  private void addToPacketCountsLeaf(int maxTransmittablePacketCount,
      int transmittablePacketsCount, ArrayList<Integer> packetIds,
      int currentPacketCount, int currentUsedRecievedPacketCount)
  {
    int checkedPackets = 0;
    for(int index = 1; index <= maxTransmittablePacketCount; index ++)
    {
      if(checkedPackets <= transmittablePacketsCount)
      {
        packetIds.add(index + currentPacketCount);
        checkedPackets++;
      }
    }
    
  }

  /**
   * updates packet Trackers for channel model
   * @param maxTransmittablePacketCount
   * @param transmittablePacketsCount
   * @param packetIds
   * @param currentPacketCount 
   * @param currentUsedRecievedPacketCount 
   */
  private void addToPacketCounts(int maxTransmittablePacketCount,
                                 int transmittablePacketsCount, 
                                 ArrayList<Integer> packetIds, 
                                 int currentPacketCount, int currentUsedRecievedPacketCount)
  {
    ArrayList<Boolean> packetRecievedBoolFormat = packetRecievedBoolFormat();
    int checkedPackets = 0;
    for(int index = 1; index <= maxTransmittablePacketCount; index ++)
    {
      if(checkedPackets <= transmittablePacketsCount &&
         packetRecievedBoolFormat.get(index -1 + currentUsedRecievedPacketCount))
      {
        packetIds.add(index + currentPacketCount);
        checkedPackets++;
      }
    }
  }

  /**
   * converts a number of tuples into packets
   * @param noTuples
   * @param op
   * @return
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   */
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

  /**
   * converts a number of packets into a collection of tuples
   * @param packets
   * @param op
   * @param preOp
   * @return
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   */
  public static HashMap<String, Integer> packetToTupleConversion(
                                             HashMap<String, Integer> packets, 
                                             InstanceOperator op, 
                                             InstanceOperator preOp) 
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
          tupleSize = 
            exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
        }
        else
          tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
        int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
        int payloadOverhead = costs.getPayloadOverhead() + 8;
        int numTuplesPerMessage = 
          (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
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
          tupleSize = 
            exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
        }
        else
          tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
        int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
        int payloadOverhead = costs.getPayloadOverhead();
        int numTuplesPerMessage = 
          (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
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
        tupleSize = 
          exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
      }
      else
        tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
      int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
      int payloadOverhead = costs.getPayloadOverhead() + 8;
      int numTuplesPerMessage = 
        (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
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
        tupleSize =
          exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
      }
      else
        tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
      int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
      int payloadOverhead = costs.getPayloadOverhead();
      int numTuplesPerMessage = 
        (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
      int tuplesForExchange = (noPackets) * numTuplesPerMessage;
      int lastPacketTupleCount = preOp.getLastPacketTupleCount();
        tuplesForExchange -= lastPacketTupleCount;
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
  private int recievedPacketCount(IOT IOT, int startingPoint, int maxPackets, String child) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
  
    int packetsRecieved = 0;
    ArrayList<Boolean> packets = arrivedPackets.get(child);
    int index = 0;
    int firstPoint = startingPoint;
    int packetsSent = 0;
    Iterator<Boolean> packetIterator = packets.iterator();
    int counter = 0;
    while(packetIterator.hasNext())
    {
      Boolean packetRecieved = packetIterator.next();
      if(firstPoint <= index)
      {
        if(!packetRecieved)
        {
          Iterator<String> EquivNodes = 
            this.overlayNetwork.getActiveNodesInRankedOrder(
                this.overlayNetwork.getClusterHeadFor(child)).iterator();
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
   * allows the utils system to ask if a specfic packet was recieved
   */
  
  public String packetRecievedStringBoolFormat()
  {
    String output = "";
    //locate the child cluster heads
    Set<String> clusterHeadIds = new HashSet<String>();
    Iterator<String> inputPacketKeys = arrivedPackets.keySet().iterator();
    while(inputPacketKeys.hasNext())
    {
      String child = inputPacketKeys.next();
      clusterHeadIds.add(this.overlayNetwork.getClusterHeadFor(child));
    }
  
    inputPacketKeys = clusterHeadIds.iterator();
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
            this.overlayNetwork.getActiveNodesInRankedOrder(
                this.overlayNetwork.getClusterHeadFor(child)).iterator();
          while(EquivNodes.hasNext())
          {
            String EquivNode = EquivNodes.next();
            ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
            if(equivPackets.get(counter))
              packetRecieved = true;
          }
        }
        if(packetRecieved)
          output = output.concat("T,");
        else
          output = output.concat("F,");
        counter++;
      }
    }
    return output;
  }
  
  public ArrayList<Boolean> packetRecievedBoolFormat()
  {
    ArrayList<Boolean> output = new ArrayList<Boolean>();
    //locate the child cluster heads
    Set<String> clusterHeadIds = new HashSet<String>();
    Iterator<String> inputPacketKeys = arrivedPackets.keySet().iterator();
    while(inputPacketKeys.hasNext())
    {
      String child = inputPacketKeys.next();
      clusterHeadIds.add(this.overlayNetwork.getClusterHeadFor(child));
    }
  
    inputPacketKeys = clusterHeadIds.iterator();
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
            this.overlayNetwork.getActiveNodesInRankedOrder(
                this.overlayNetwork.getClusterHeadFor(child)).iterator();
          while(EquivNodes.hasNext())
          {
            String EquivNode = EquivNodes.next();
            ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
            if(equivPackets.get(counter))
              packetRecieved = true;
          }
        }
        output.add(packetRecieved);
        counter++;
      }
    }
    return output;
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
    this.noiseValuesExped = new HashMapList<String, NoiseDataStore>();
  }

  /**
   * finds the packets from the clusterhead and other siblings and transmits them
   * @return
   */
  public ArrayList<Integer> transmittablePackets()
  {
    ArrayList<Integer> packetIDs = new ArrayList<Integer>();
    String key = this.overlayNetwork.getClusterHeadFor(siteID);
    ArrayList<Boolean> packets = arrivedPackets.get(key);
    Iterator<Boolean> packetIterator = packets.iterator();
    int counter = 0;
    while(packetIterator.hasNext())
    {
      Boolean packetRecieved = packetIterator.next();
      if(!packetRecieved)
      {
        Iterator<String> EquivNodes = 
          this.overlayNetwork.getActiveNodesInRankedOrder(
              this.overlayNetwork.getClusterHeadFor(key)).iterator();
        while(EquivNodes.hasNext())
        {
          String EquivNode = EquivNodes.next();
          ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
          if(equivPackets.get(counter))
            packetRecieved = true;
        }
      }
      if(packetRecieved)
        packetIDs.add(counter + 1);
      counter++;
    }
    return packetIDs;
  }

  public void addNoiseTrace(String sourceID, Integer packetID, NoiseDataStore noise)
  {
    String cluster = this.overlayNetwork.getClusterHeadFor(sourceID);
    this.noiseValuesExped.addWithDuplicates(new String(cluster + "-" + packetID.toString()), noise);  
  }
  
  public HashMapList<String, NoiseDataStore> getNoiseExpValues()
  {
    return this.noiseValuesExped;
  }
}
