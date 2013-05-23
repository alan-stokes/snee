package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityDataStructureChannel;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CollectionOfPackets;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.Window;
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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetNestedLoopJoinOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class ChannelModelSite implements Serializable
{
  /**
   * serilised id
   */
  private static final long serialVersionUID = 378104895960678188L;
  private HashMap<String, ArrayList<Boolean>> arrivedPackets = 
    new HashMap<String, ArrayList<Boolean>>();
  private static HashMap<String, HashMapList<Integer, Boolean>> aggregationTupleTracker = 
    new HashMap<String, HashMapList<Integer, Boolean>>();
  private HashMap<String, Integer> expectedPackets = null;
  private HashMapList<String, NoiseDataStore> noiseValuesExped = new HashMapList<String, NoiseDataStore>();
  private ArrayList<String> needToListenTo = new ArrayList<String>();
  private ArrayList<String> receivedAcksFrom = new ArrayList<String>();
  private ArrayList<Task> tasks = new ArrayList<Task>();
  private ArrayList<String> transmittedAcksTo = new ArrayList<String>();
  private CollectionOfPackets transmitableWindows = new CollectionOfPackets(); 
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
  private static int tuplesParticipatingInAggregation = 0;
  private ChannelModel model = null;
  private ArrayList<Integer> packetIds = new ArrayList<Integer>();
  private boolean usePacketIDs = false;
  private boolean ranAlready = false;

  /**
   * constructor for a channel model site
   * @param expectedPackets
   * @param parents
   * @param siteID
   * @param overlayNetwork
   * @param position
   * @param packetID 
   */
  public ChannelModelSite(HashMap<String, Integer> expectedPackets, String siteID,
                          LogicalOverlayNetworkHierarchy overlayNetwork, int position,
                          ArrayList<Task> tasks, Long beta, CostParameters costs,
                          ChannelModel model, boolean packetID)
  {
    this.expectedPackets = expectedPackets;
    setupExpectedPackets();
    this.siteID = siteID;
    this.overlayNetwork = overlayNetwork;
    cardModel = new CardinalityEstimatedCostModel(overlayNetwork.getQep());
    this.priority = position;
    this.beta = beta;
    ChannelModelSite.costs = costs;
    if(tasks != null)
    this.tasks.addAll(tasks);
    this.model = model;
    usePacketIDs = packetID;
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
    if(packets == null)
      System.out.println();
    if(packetID -1 < packets.size())
    {
      arrivedPackets.remove(source);
      packets.set(packetID -1, true);
      arrivedPackets.put(source, packets);
    }
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
      return false;
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
          if(equivPackets.size() <= counter)
            found = false;
          else if(equivPackets.get(counter))
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
    if(packetID < packetsFromSibling.size())
    {
      packetsFromSibling.set(packetID -1, true);
      arrivedPackets.remove(string);
      arrivedPackets.put(string, packetsFromSibling);
      siblingTransmissions++;
    }
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
      this.packetIds.clear();
      this.transmitableWindows.clear();
      
      if(siteID.equals("1"))
        System.out.println();
      ArrayList<InstanceOperator> operators = IOT.getOpInstancesInSpecialOrder(site);
      Iterator<InstanceOperator> operatorIterator = operators.iterator();
      HashMap<String, Integer> currentPacketCount = new HashMap<String, Integer>();
      int previousOpOutput = 0;
      HashMap<String, Integer> currentUsedRecievedPacketCount = new HashMap<String, Integer>();
      String preivousOutputExtent = null;
      InstanceOperator previousOp = null;
      CollectionOfPackets tuples = new CollectionOfPackets();
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
            Integer cPacketCount = currentPacketCount.get(exOp.getSite().getID());
            if(cPacketCount == null)
            {
              cPacketCount = 0; 
            }
            Integer rPacketCount = currentUsedRecievedPacketCount.get(exOp.getSite().getID());
            if(rPacketCount == null)
            {
              rPacketCount = 0; 
            }
            boolean isleaf = exOp.getSourceFrag().isLeaf();
            int outputPackets = this.tupleToPacketConversion(tuples, previousOp, exOp, cPacketCount);
            addToPacketCounts(outputPackets, cPacketCount, isleaf, packetIds);
            
            currentPacketCount.remove(exOp.getSite().getID());
            currentPacketCount.put(this.overlayNetwork.getClusterHeadFor(exOp.getSite().getID()), cPacketCount + outputPackets);
            exOp.setExtent(preivousOutputExtent);
            exOp.setTupleValueForExtent(previousOpOutput);
            this.transmitableWindows.updateCollection(exOp, tuples.getWindowsOfExtent(exOp, exOp.getExtent()));
            
          }
          if(exOp.getComponentType().equals(ExchangePartType.PRODUCER) &&
             exOp.getNext().getSite().getID().equals(exOp.getSite().getID()))
          {
            exOp.setExtent(exOp.getSourceFrag().getRootOperator().getExtent());
            ArrayList<Window> preOpWindows = tuples.getWindowsOfExtent(previousOp);
            tuples.updateCollection(exOp, preOpWindows);
          }
          /*if the exchange is a relay, then the packets have been recieved by this site already, 
          and need to be assessed*/
          if(exOp.getComponentType().equals(ExchangePartType.RELAY))
          {
            InstanceExchangePart preExOp = exOp.getPrevious();
            int maxTransmittablePacketCount = exOp.getmaxPackets(IOT.getDAF(), beta, costs, !reliableChannelQEP);
            exOp.setExtent(preExOp.getExtent());
            //got max packets 
            Integer rPacketCount = currentUsedRecievedPacketCount.get(preExOp.getSite().getID());
            if(rPacketCount == null)
            {
              rPacketCount = 0; 
            }
            Integer cPacketCount = currentPacketCount.get(exOp.getSite().getID());
            if(cPacketCount == null)
              cPacketCount = 0;
            boolean isleaf = exOp.getSourceFrag().containsOperatorType(SensornetAcquireOperator.class);
            int transmittablePacketsCount = recievedPacketCount(IOT, rPacketCount, 
                                                                maxTransmittablePacketCount, 
                                                                exOp.getPrevious().getSite().getID());
            if(exOp.getExtent() == null)
              exOp.setExtent(exOp.getPrevious().getExtent());
            ArrayList<Window> windows = 
                            packetToTupleConversion(transmittablePacketsCount, exOp, exOp.getPrevious(),
                                                    currentUsedRecievedPacketCount);
            tuples.updateCollection(preExOp, windows);
            int outputPackets = this.tupleToPacketConversion(tuples, preExOp, exOp, cPacketCount);
            addToPacketCounts(outputPackets, cPacketCount, isleaf, packetIds);
            currentPacketCount.remove(exOp.getSite().getID());
            currentPacketCount.put(this.overlayNetwork.getClusterHeadFor(exOp.getSite().getID()), cPacketCount + outputPackets);
            ArrayList<Window> outputWindows = packetToTupleConversion(outputPackets, exOp, preExOp, currentUsedRecievedPacketCount);
            currentUsedRecievedPacketCount.remove(preExOp.getSite().getID());
            currentUsedRecievedPacketCount.put(this.overlayNetwork.getClusterHeadFor(preExOp.getSite().getID()), rPacketCount + outputPackets);
            int noOfTuples = CollectionOfPackets.determineNoTuplesFromWindows(outputWindows);
            exOp.setTupleValueForExtent(noOfTuples);
            this.tupleToPacketConversion(tuples, preExOp, exOp, cPacketCount);
            tuples.updateCollection(exOp, outputWindows);
            this.transmitableWindows.updateCollection(exOp, tuples.getWindowsOfExtent(exOp, exOp.getExtent()));
            
          }
          if(exOp.getComponentType().equals(ExchangePartType.CONSUMER) &&
             previousOp == null )
          {
            int maxTransmittablePacketCount = exOp.getmaxPackets(IOT.getDAF(), beta, costs, !reliableChannelQEP);
            if(currentPacketCount == null || exOp == null || exOp.getPrevious() == null ||
               exOp.getPrevious().getSite() == null || exOp.getPrevious().getSite().getID() == null)
              System.out.println();
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
            if(exOp.getExtent() == null)
              exOp.setExtent(exOp.getPrevious().getExtent());
            ArrayList<Window> windows = 
              packetToTupleConversion(transmittablePacketsCount, exOp, exOp.getPrevious(),
                                      currentUsedRecievedPacketCount);
            tuples.updateCollection(exOp, windows);
            tuples.updateCollection(exOp.getPrevious(), windows);
            this.tupleToPacketConversion(tuples, exOp.getPrevious(), exOp, cPacketCount);
            currentUsedRecievedPacketCount.remove(preExOp.getSite().getID());
            currentUsedRecievedPacketCount.put(
                this.overlayNetwork.getClusterHeadFor(
                    preExOp.getSite().getID()), rPacketCount + maxTransmittablePacketCount);
          }
          if(exOp.getComponentType().equals(ExchangePartType.CONSUMER) &&
             previousOp != null )
         {
            ArrayList<Window> preOpWindows = tuples.getWindowsOfExtent(previousOp);
            previousOp = null;
            exOp.setExtent(exOp.getPrevious().getExtent());
            tuples.updateCollection(exOp, preOpWindows);
         }
        }
        else // is some sort of in-network query operator
        {
          //if acquire, then need to determine tuples
          if(op.getSensornetOperator() instanceof SensornetAcquireOperator)
          {
            List<Attribute> attributes = op.getSensornetOperator().getLogicalOperator().getAttributes();
            preivousOutputExtent = attributes.get(1).toString();
            previousOp = op;
            if(preivousOutputExtent == null)
              System.out.println();
            op.setExtent(preivousOutputExtent);
            tuples.updateCollection(op, tuples.createAcquirePacket(beta));
          }
          // if some operator then place though cardinality model
          if(!(op.getSensornetOperator() instanceof SensornetAcquireOperator) &&
             !(op.getSensornetOperator() instanceof SensornetDeliverOperator))
          {
            previousOp = op;
            InstanceOperator preOp = (InstanceOperator) op.getInput(0);
            preivousOutputExtent = recordExtent(preOp);
            String extent = "";
            if(op.getSensornetOperator() instanceof SensornetNestedLoopJoinOperator)
            {
              ArrayList<String> doneExtents = locateInputExtents(op, tuples);
              extent = calculateExtent(doneExtents);
              op.setExtent(extent);
            }
            else
            {
              op.setExtent(preOp.getExtent());
              extent = preOp.getExtent();
            }
            CardinalityDataStructureChannel outputs = cardModel.model(op, tuples, beta);
            tuples.updateCollection(op, outputs.getWindows());
            this.tupleToPacketConversion(tuples, preOp, op, 0);
          }
          //if delivery operator calculate packets transmitted for system to determine tuples.
          if(op.getSensornetOperator() instanceof SensornetDeliverOperator)
          {
            InstanceOperator preOp = op.getInstanceInput(0);
            if(preOp instanceof InstanceExchangePart)
            {
              InstanceExchangePart exPreOp = (InstanceExchangePart) preOp;
              op.setExtent(exPreOp.getExtent());
            }
            else
            {
              op.setExtent(preOp.getExtent());
            }
            CardinalityDataStructureChannel outputs = cardModel.model(op, tuples, beta);
            ArrayList<Window> outputWindows = outputs.getWindows();
            tuples.updateCollection(op, outputWindows);
            previousOpOutput = this.tupleToPacketConversion(tuples, op, op, 0);
            for(int index = 0; index < previousOpOutput; index++)
              packetIds.add(index);
            this.transmitableWindows.updateCollection(op, tuples.getWindowsOfExtent(op, op.getExtent()));
          }
        }
      }
    return packetIds;
  }

  private String calculateExtent(ArrayList<String> doneExtents)
  {
    String extent = "";
    Iterator<String> extents = doneExtents.iterator();
    while(extents.hasNext())
    {
      extent = extent.concat(extents.next());
    }
    return extent;
  }

  private ArrayList<String> locateInputExtents(InstanceOperator op,
                                               CollectionOfPackets tuples)
  {
    ArrayList<String> doneExtents = new ArrayList<String>();
    Iterator<Node> inputIterator = op.getInputsList().iterator();
    while(inputIterator.hasNext())
    {
      InstanceOperator cOp = (InstanceOperator) inputIterator.next();
      String currentExtent = null;
      if(cOp instanceof InstanceExchangePart)
      {
        InstanceExchangePart cOpex = (InstanceExchangePart) cOp;
        currentExtent = cOpex.getExtent();
      }
      else
        currentExtent = cOp.getExtent();
      if(!doneExtents.contains(currentExtent))
      {
        doneExtents.add(currentExtent);
      }
    }
    return doneExtents;
  }

  private String recordExtent(InstanceOperator preOp)
  {
    if(preOp.getSensornetOperator() instanceof SensornetExchangeOperator)
    {
      InstanceExchangePart exOp = (InstanceExchangePart) preOp;
      return exOp.getExtent();
    }
    else
    {
      return preOp.getExtent();
    }
  }

  /**
   * takes the aggregation operator, and its inputs and determines just how many tuples 
   * have been lost to create said aggregation result. 
   * If the agg operator is a eval op, it prints it to a file within the output folder for future stores.
   * @param op
   * @param iot 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private void checkAggregationTupleTracker(InstanceOperator op, IOT iot) 
  throws SchemaMetadataException, TypeMappingException
  {
    HashMapList<Integer, Boolean> currentSitesTracker = 
      ChannelModelSite.aggregationTupleTracker.get(op.getSite().getID());
    if(currentSitesTracker == null)
      currentSitesTracker = new HashMapList<Integer, Boolean>();
    HashMap<String, ArrayList<Boolean>> reducedArrivedpackets = reduceArrivedPackets(iot.getSiteFromID(op.getSite().getID()), this.arrivedPackets);
    InstanceOperator firstInput = (InstanceOperator) op.getInput(0);
    if(op.getSensornetOperator() instanceof SensornetAggrEvalOperator &&
       firstInput.getSensornetOperator() instanceof SensornetAggrMergeOperator)
    {
      boolean acqOperatorFound = false;
      Iterator<InstanceOperator> siteOps = 
        this.overlayNetwork.getQep().getIOT().getOpInstances(op.getSite()).iterator();
      while(siteOps.hasNext())
      {
        SensornetOperator senOp =  siteOps.next().getSensornetOperator();
        if(senOp instanceof SensornetAcquireOperator)
          acqOperatorFound = true;
      }
      if(acqOperatorFound)
      {
        Iterator<Integer> keys = currentSitesTracker.keySet().iterator();
        while(keys.hasNext())
        {
          Integer key = keys.next();
          currentSitesTracker.addWithDuplicates(key, true);
        }
      }
    }
    else
    {
      if(!op.getSite().isLeaf())
      {
        Iterator<Node> inputSites = op.getSite().getInputsList().iterator();
        int tupleRatio = findPacketTotupleRatio(op);
        
        while(inputSites.hasNext())
        {
          tupleRatio = findPacketTotupleRatio(op);
          Site inputSite = (Site) inputSites.next();
          boolean found = false;
          boolean acqOperatorFound = false;
          Iterator<InstanceOperator> siteOps = 
            this.overlayNetwork.getQep().getIOT().getOpInstances(inputSite).iterator();
          while(siteOps.hasNext() && !found)
          {
            SensornetOperator senOp =  siteOps.next().getSensornetOperator();
            if(senOp instanceof SensornetAggrMergeOperator ||
               senOp instanceof SensornetAggrInitOperator)
              found = true;
            if(senOp instanceof SensornetAcquireOperator)
              acqOperatorFound = true;
          }
          Site trueInputSite = inputSite;
          if(!found)
          {
            while(trueInputSite.getInputsList().size() < 2 && !found && trueInputSite.getInputsList().size() > 0)
            {
              if(trueInputSite.getInputsList().size() == 0)
                System.out.println();
              trueInputSite = (Site) trueInputSite.getInputsList().get(0);
              siteOps = this.overlayNetwork.getQep().getIOT().getOpInstances(trueInputSite).iterator();
              while(siteOps.hasNext() && !found)
              {
                SensornetOperator senOp =  siteOps.next().getSensornetOperator();
                if(senOp instanceof SensornetAggrMergeOperator)
                  found = true;
                if(senOp instanceof SensornetAcquireOperator)
                  acqOperatorFound = true;
              }
            }
          }
          
          String inputNodeID = inputSite.getID();
          //if input site had an acquire, this will be the first time its counted.
          if(acqOperatorFound)
          {
            //add received packets
            ArrayList<Boolean> receivedPackets = reducedArrivedpackets.get(inputNodeID);
            Iterator<Boolean> receivedPacketIterator = receivedPackets.iterator();
            int counter = 0;
            while(receivedPacketIterator.hasNext())
            {
              Boolean recieved = receivedPacketIterator.next();
              int oldtupleRatio = tupleRatio;
               
              if(counter == receivedPackets.size() -1)
              {
                if(op.getLastPacketTupleCount() == null)
                  tupleRatio = 0;
                else
               	  tupleRatio = op.getLastPacketTupleCount();
              }
              for(int index = 1; index <= tupleRatio; index++)
              {
                currentSitesTracker.addWithDuplicates(((counter * oldtupleRatio ) + index), recieved);
              }
              counter++;
            }
          }
          
          //add banked tuples
          if(!inputSite.isLeaf() && !trueInputSite.isLeaf())
          {
            if(inputSite == trueInputSite)
            {
              HashMapList<Integer, Boolean> inputSitesTracker = 
                ChannelModelSite.aggregationTupleTracker.get(inputSite.getID());
              if(inputSitesTracker != null)
              {
                Iterator<Integer> keys = inputSitesTracker.keySet().iterator();
                while(keys.hasNext())
                {
                  Integer key = keys.next();
                  ArrayList<Boolean> bools = inputSitesTracker.get(key);
                  Iterator<Boolean> boolIterator = bools.iterator();
                  ArrayList<Boolean> receivedPackets = reducedArrivedpackets.get(inputSite.getID());
                  Iterator<Boolean> receivedPacketIterator = receivedPackets.iterator();
                  while(receivedPacketIterator.hasNext())
                  {
                    Boolean recieved = receivedPacketIterator.next();
                    if(recieved)
                    {
                      while(boolIterator.hasNext())
                      {
                        currentSitesTracker.addWithDuplicates(key, boolIterator.next());
                      }
                    }
                  }
                }
              }
            }
            else
            {
              HashMapList<Integer, Boolean> inputSitesTracker = 
              ChannelModelSite.aggregationTupleTracker.get(trueInputSite.getID());
              if(inputSitesTracker != null)
              {
                Iterator<Integer> keys = inputSitesTracker.keySet().iterator();
                while(keys.hasNext())
                {
                  Integer key = keys.next();
                  ArrayList<Boolean> bools = inputSitesTracker.get(key);
                  Iterator<Boolean> boolIterator = bools.iterator();
                  ArrayList<Boolean> receivedPackets = reducedArrivedpackets.get(inputSite.getID());
                  if(receivedPackets == null)
                    System.out.println();
                  Iterator<Boolean> receivedPacketIterator = receivedPackets.iterator();
                  while(receivedPacketIterator.hasNext())
                  {
                    Boolean recieved = receivedPacketIterator.next();
                    if(recieved)
                    {
                      while(boolIterator.hasNext())
                      {
                        currentSitesTracker.addWithDuplicates(key, boolIterator.next());
                      }
                    }
                  }
                }
              }
            }
          }
        }
        ChannelModelSite.aggregationTupleTracker.put(op.getSite().getID(), currentSitesTracker);
      }
    }
  }
  
  private HashMap<String, ArrayList<Boolean>> getArrivedPackets()
  {
    return this.arrivedPackets;
  }

  private HashMap<String, ArrayList<Boolean>> reduceArrivedPackets(Site site, HashMap<String, ArrayList<Boolean>> arrivedPackets)
  {
    HashMap<String, ArrayList<Boolean>> reducedArrivePackets = new HashMap<String, ArrayList<Boolean>>();
    ArrayList<Node> inputs = new ArrayList<Node>(site.getInputsList());
    
    Iterator<Node> inputPacketKeys = inputs.iterator();
    while(inputPacketKeys.hasNext())
    {
      String child = inputPacketKeys.next().getID();
      ArrayList<Boolean> packets = arrivedPackets.get(child);
      ArrayList<Boolean> reducedPackets = new ArrayList<Boolean>();
      if(packets != null)
      {
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
            boolean updated = false;
            while(EquivNodes.hasNext() && !updated)
            {
              String EquivNode = EquivNodes.next();
              ArrayList<Boolean> equivPackets = arrivedPackets.get(EquivNode);
              if(equivPackets.size() > counter && equivPackets.get(counter))
              {
                reducedPackets.add(true);
                updated = true;
              }
            }
            if(!found)
            {
              reducedPackets.add(false);
            }
          }
          else
          {
            reducedPackets.add(true);
          }
          counter++;
        }
        reducedArrivePackets.put(child, reducedPackets);
      }
    }
    return reducedArrivePackets;
  }

  private int findPacketTotupleRatio(InstanceOperator op) 
  throws SchemaMetadataException, TypeMappingException
  {
    int tupleSize = 0;
    boolean foundAcquire = false;
    while(!(op instanceof InstanceExchangePart) && !foundAcquire)
    {
      if(op.getSensornetOperator() instanceof SensornetAcquireOperator)
        foundAcquire = true;
      else
        op = (InstanceOperator) op.getInput(0);
    }
    
    
    if(foundAcquire)
      tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
    else
    {
      InstanceExchangePart exOp = (InstanceExchangePart) op;
      tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
    }
    int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
    int payloadOverhead =0;
    if(this.usePacketIDs)
      payloadOverhead = costs.getPayloadOverhead() + costs.getPacketIDOverhead();
    else
      payloadOverhead = costs.getPayloadOverhead();
    return (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
  }

  /**
   * updates packet Trackers for channel model
   * @param maxTransmittablePacketCount
   * @param transmittablePacketsCount
   * @param currentPacketCount 
   * @param currentUsedRecievedPacketCount 
   * @param packetIds 
   */
  private void addToPacketCounts(int noPackets, int currentPacketCount,
                                 boolean leaf, ArrayList<Integer> packetIds)
  {
    for(int index = 1; index <= noPackets; index ++)
    {
        packetIds.add(index + currentPacketCount);
    }
  }

  /**
   * converts a number of tuples into packets
   * @param tuples
   * @param op
   * @param cPacketCount 
 * @param exOp2 
   * @return
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   */
  private int tupleToPacketConversion(CollectionOfPackets tuples, 
                                      InstanceOperator op, InstanceOperator mainOp, Integer cPacketCount)
  throws SchemaMetadataException, TypeMappingException
  {
    int tupleSize = 0;
    if(ChannelModelSite.reliableChannelQEP)
    {
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
      }
      else
      {
        tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
      }
      int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
      int payloadOverhead = costs.getPayloadOverhead();
      int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
      if(numTuplesPerMessage == 0)
    	  numTuplesPerMessage++;
      int totalTuples = tuples.determineNoTuplesFromWindows(op);
      int pacekts = (totalTuples / numTuplesPerMessage);
      
      if(totalTuples %  numTuplesPerMessage == 0)
      {
        op.setLastPacketTupleCount(numTuplesPerMessage);
        mainOp.setLastPacketTupleCount(numTuplesPerMessage);
      }
      else
      {
        op.setLastPacketTupleCount(totalTuples %  numTuplesPerMessage);
        mainOp.setLastPacketTupleCount(totalTuples %  numTuplesPerMessage);
      }
      return pacekts;
    }
    else
    {
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        tupleSize = exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
      }
      else
      {
        if(op ==null || op.getSensornetOperator() == null)
          System.out.println();
        tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
      }
      int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
      int payloadOverhead = 0;
      if(this.usePacketIDs)
        payloadOverhead = costs.getPayloadOverhead() + costs.getPacketIDOverhead();
      else
        payloadOverhead = costs.getPayloadOverhead();
      int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
      if(numTuplesPerMessage == 0)
    	  numTuplesPerMessage++;
      int totalTuples = tuples.determineNoTuplesFromWindows(op);
      Double frac = new Double(totalTuples) / new Double(numTuplesPerMessage);
      Double packetsD = Math.ceil(frac);
      int pacekts = packetsD.intValue();
      if(totalTuples %  numTuplesPerMessage == 0)
      {
        op.setLastPacketTupleCount(numTuplesPerMessage);
        mainOp.setLastPacketTupleCount(numTuplesPerMessage);
      }
      else
      {
        op.setLastPacketTupleCount(totalTuples %  numTuplesPerMessage);
        mainOp.setLastPacketTupleCount(totalTuples %  numTuplesPerMessage);
      }
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
        int payloadOverhead = costs.getPayloadOverhead();
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
        int payloadOverhead = costs.getPayloadOverhead() + costs.getPacketIDOverhead();
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
  public ArrayList<Window> packetToTupleConversion( Integer noPackets, 
                                                    InstanceOperator op,
                                                    InstanceOperator preOp,
                                                    HashMap<String, Integer> usedPackets) 
  throws SchemaMetadataException, TypeMappingException
  {
    CollectionOfPackets completeRecievedWindows = new CollectionOfPackets();
    if(noPackets == 0)
    {
      ArrayList<Window> windows = new ArrayList<Window>();
      for(int index =1; index <=beta; index++)
      {
        Window newWindow = new Window(0, index);
        windows.add(newWindow);
      }
      return windows;
    }
    else
    {
      int tupleSize = 0;
      if(ChannelModelSite.reliableChannelQEP)
      {
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
       
        CollectionOfPackets inputWindows = 
          this.model.getChannelModel().get(Integer.parseInt(preOp.getSite().getID())).getTransmitableWindows();
        ArrayList<Boolean> receivedPacketBooleanForm = this.packetRecievedBoolFormat().get(preOp.getSite().getID());
        Iterator<Boolean> receivedPacketBoolIterator = receivedPacketBooleanForm.iterator();
        int usedUpPackets;
        if(usedPackets == null)
          usedUpPackets = 0;
        else
         usedUpPackets = usedPackets.get(preOp.getSite().getID());
        int removeCoutner = 0;
        while(removeCoutner < usedUpPackets)
          receivedPacketBoolIterator.next();
        int countedPacket = 0;
        int tupleCount = 0;
        while(receivedPacketBoolIterator.hasNext() && countedPacket < noPackets)
        {
          if(receivedPacketBoolIterator.next())
          {
            Iterator<Window> receivedWindows = 
              inputWindows.returnWindowsForTuples(tupleCount, numTuplesPerMessage,op).iterator();
            while(receivedWindows.hasNext())
            {
              ArrayList<Window> update = new ArrayList<Window>();
              update.add(receivedWindows.next());
              completeRecievedWindows.updateCollection(op, update);
            }
          }
          else
          {
            tupleCount += numTuplesPerMessage;
          }
          countedPacket++;
        }
        return completeRecievedWindows.getWindowsOfExtent(op, op.getExtent());
      }
      else
      {     
        if(op instanceof InstanceExchangePart)
        {
          InstanceExchangePart exOp = (InstanceExchangePart) op;
          tupleSize = 
            exOp.getSourceFrag().getRootOperator().getSensornetOperator().getPhysicalTupleSize();
        }
        else
        {
          tupleSize = op.getSensornetOperator().getPhysicalTupleSize();
        }
      
        int maxMessagePayloadSize = costs.getMaxMessagePayloadSize();
        int payloadOverhead =0;
        if(this.usePacketIDs)
          payloadOverhead = costs.getPayloadOverhead() + costs.getPacketIDOverhead();
        else
          payloadOverhead = costs.getPayloadOverhead();
        int numTuplesPerMessage = 
        (int) Math.floor(maxMessagePayloadSize - payloadOverhead) / (tupleSize);
       
        CollectionOfPackets inputWindows = 
          this.model.getChannelModel().get(Integer.parseInt(preOp.getSite().getID())).getTransmitableWindows();
        ArrayList<Boolean> receivedPacketBooleanForm = this.packetRecievedBoolFormat().get(preOp.getSite().getID());
        Iterator<Boolean> receivedPacketBoolIterator = receivedPacketBooleanForm.iterator();
        int usedUpPackets;
        if(usedPackets == null || !usedPackets.keySet().contains(preOp.getSite().getID()))
          usedUpPackets = 0;
        else
         usedUpPackets = usedPackets.get(preOp.getSite().getID());
        int removeCoutner = 0;
        while(removeCoutner < usedUpPackets &&  receivedPacketBoolIterator.hasNext())
        {
          receivedPacketBoolIterator.next();
          removeCoutner++;
        }
        int countedPacket = 0;
        int tupleCount = 0;
        while(receivedPacketBoolIterator.hasNext() && countedPacket < noPackets)
        {
          if(receivedPacketBoolIterator.next())
          {
            Iterator<Window> receivedWindows = 
              inputWindows.returnWindowsForTuples(tupleCount, numTuplesPerMessage, preOp).iterator();
            while(receivedWindows.hasNext())
            {
              ArrayList<Window> update = new ArrayList<Window>();
              update.add(receivedWindows.next());
              completeRecievedWindows.updateCollection(op, update);
            }
            countedPacket++;
          }
          tupleCount += numTuplesPerMessage;
        }
        ArrayList<Window> output =  completeRecievedWindows.getWindowsOfExtent(op); 
        if(output.size() == 0)
        {
          ArrayList<Window> windows = new ArrayList<Window>();
          for(int index =1; index <=beta; index++)
          {
            Window newWindow = new Window(0, index);
            windows.add(newWindow);
          }
          return windows;
        }
        return output; 
      }
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
    if(packets == null)
      return 0;
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
            if(equivPackets.size() <= counter)
              packetRecieved = false;
            else if(equivPackets.get(counter))
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
  
  public HashMapList<String, Boolean> packetRecievedBoolFormat()
  {
    HashMapList<String, Boolean> output = new HashMapList<String, Boolean>();
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
            if(equivPackets.size() <= counter)
              packetRecieved = false;
            else if(equivPackets.get(counter))
              packetRecieved = true;
          }
        }
        output.addWithDuplicates(child, packetRecieved);
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
    this.transmitableWindows.clear();
    this.ranAlready = false;
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
    if(packets == null)
      System.out.println();
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

  public static int getTuplesParticipatingInAggregation()
  {
    return tuplesParticipatingInAggregation;
  }

  public void determineAggregationContribution(String siteId)
  throws SchemaMetadataException, TypeMappingException
  {
    Site site = overlayNetwork.getQep().getIOT().getSiteFromID(siteId);
    if(site == null)
      System.out.println();
    ArrayList<InstanceOperator> operators = 
      overlayNetwork.getQep().getIOT().getOpInstances(site, TraversalOrder.POST_ORDER, true);
    Iterator<InstanceOperator> operatorIterator = operators.iterator();
    while(operatorIterator.hasNext())
    {
      InstanceOperator op = operatorIterator.next();
      if(op.getSensornetOperator() instanceof SensornetAggrMergeOperator ||
         op.getSensornetOperator()  instanceof SensornetAggrEvalOperator)
      {
        checkAggregationTupleTracker(op, overlayNetwork.getQep().getIOT());
      }
      if(op.getSensornetOperator() instanceof SensornetDeliverOperator)
         finishTracking(op);
    }
  }

  private void finishTracking(InstanceOperator op)
  {
    HashMapList<Integer, Boolean> currentSitesTracker = 
      ChannelModelSite.aggregationTupleTracker.get(op.getSite().getID());
    if(currentSitesTracker == null)
      currentSitesTracker = new HashMapList<Integer, Boolean>();
    ArrayList<InstanceOperator> operators = 
      overlayNetwork.getQep().getIOT().getOpInstances(op.getSite(), TraversalOrder.POST_ORDER, true);
    boolean evalLocatedOnsameSite = false;
    Iterator<InstanceOperator> operatorIterator = operators.iterator();
    while(operatorIterator.hasNext())
    {
      if(operatorIterator.next().getSensornetOperator() instanceof SensornetAggrEvalOperator)
        evalLocatedOnsameSite = true;
    }
    int count = 0;
    if(evalLocatedOnsameSite)
    {
      Iterator<Integer> keys = currentSitesTracker.keySet().iterator();
      while(keys.hasNext())
      {
        Integer key = keys.next();
        Iterator<Boolean> values = currentSitesTracker.get(key).iterator();
        while(values.hasNext())
        {
          if(values.next())
            count++;
        }
      }
    }
    else
    {
      HashMap<String, ArrayList<Boolean>> reducedArrivedpackets = reduceArrivedPackets(op.getSite(), this.arrivedPackets);
      Iterator<Node> inputSites = op.getSite().getInputsList().iterator();
      while(inputSites.hasNext())
      {
        Site inputSite = (Site) inputSites.next();
        HashMapList<Integer, Boolean> inputSitesTracker = 
          ChannelModelSite.aggregationTupleTracker.get(inputSite.getID());
        if(inputSitesTracker == null)
          ChannelModelSite.tuplesParticipatingInAggregation = 0;
        else
        {
          Iterator<Integer> keys = inputSitesTracker.keySet().iterator();
          while(keys.hasNext())
          {
            Integer key = keys.next();
            ArrayList<Boolean> bools = inputSitesTracker.get(key);
            Iterator<Boolean> boolIterator = bools.iterator();
            ArrayList<Boolean> receivedPackets = reducedArrivedpackets.get(inputSite.getID());
            Iterator<Boolean> receivedPacketIterator = receivedPackets.iterator();
            while(receivedPacketIterator.hasNext())
            {
              Boolean recieved = receivedPacketIterator.next();
              if(recieved)
              {
                while(boolIterator.hasNext())
                {
                  currentSitesTracker.addWithDuplicates(key, boolIterator.next());
                }
              }
            }
          }
        }
        Iterator<Integer> keys = currentSitesTracker.keySet().iterator();
        while(keys.hasNext())
        {
          Integer key = keys.next();
          Iterator<Boolean> values = currentSitesTracker.get(key).iterator();
          while(values.hasNext())
          {
            if(values.next())
              count++;
          }
        }
      }
    }
    ChannelModelSite.tuplesParticipatingInAggregation = count;
  }

  public static void resetAggreData()
  {
    ChannelModelSite.aggregationTupleTracker.clear();
    tuplesParticipatingInAggregation = 0;
  }
  
  public CollectionOfPackets getTransmitableWindows()
  {
    return transmitableWindows;
  }
  public ArrayList<Integer> getPacketIds()
  {
    return packetIds;
  }
}
