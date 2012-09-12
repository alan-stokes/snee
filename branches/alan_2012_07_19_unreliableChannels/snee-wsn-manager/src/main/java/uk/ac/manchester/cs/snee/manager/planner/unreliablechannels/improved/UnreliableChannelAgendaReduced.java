package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragment;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragmentTask;
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SleepTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.UnreliableChannelAgenda;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

import org.apache.log4j.Logger;
/**
 * Class responsible for recording the schedules of logical nodes in a sensor network 
 * given a logicalOverlayNetwork and unreliable channels.
 * @author  Alan Stokes
 *
 */



public class UnreliableChannelAgendaReduced extends UnreliableChannelAgenda
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 8020437449548315506L;
  /**
   * Logger for this class.
   */
  private final static Logger logger =
    Logger.getLogger(UnreliableChannelAgendaReduced.class.getName());
  
  private LogicalOverlayNetworkHierarchy activeOverlay;
  private int numberOfRundundantCycles =0;
  
  public UnreliableChannelAgendaReduced(LogicalOverlayNetwork logicaloverlayNetwork,
                                 SensorNetworkQueryPlan qep, Topology network,
                                 boolean allowDiscontinuousSensing)
  throws AgendaException, AgendaLengthException, OptimizationException, 
         SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
         SNEEException
  {
    super(logicaloverlayNetwork, qep, network, allowDiscontinuousSensing, false);
    
    numberOfRundundantCycles = Integer.parseInt(SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_REDUNDANTCYCLES));
    
    activeOverlay = new LogicalOverlayNetworkHierarchy(logicaloverlayNetwork.getClusters(), network, qep);
    this.logicaloverlayNetwork = null;
    logger.trace("Scheduling leaf fragments alpha=" + this.alpha + " bms beta=" + this.beta);
    scheduleLeafFragments(activeOverlay);
    logger.trace("Scheduling the non-leaf fragments");
    connectLogicalNodes(activeOverlay);
    scheduleNonLeafFragments(activeOverlay);
    logger.trace("Scheduled final sleep task");
    scheduleFinalSleepTask();
    
    long length = this.getLength_bms(UnreliableChannelAgendaReduced.INCLUDE_SLEEP);
    logger.trace("Agenda alpha=" + this.alpha + " beta=" + this.beta + 
                 " alpha*beta = " + this.alpha * this.beta + " length="+length);
    
    if (length > (this.alpha * this.beta) && (!allowDiscontinuousSensing)) 
    {
      //display the invalid agenda, for debugging purposes
      //this.display(QueryCompiler.queryPlanOutputDir,
      //this.getName()+"-invalid");
      String msg = "Invalid agenda: alpha*beta = " + 
        bmsToMs(this.alpha * this.beta) + "ms, length = " + 
        bmsToMs(length) + "ms, alpha = "+bmsToMs(alpha) + "ms, beta = "
        + bmsToMs(beta);
      logger.warn(msg);
      throw new AgendaLengthException(msg);
    }
  }
  
  public UnreliableChannelAgendaReduced(LogicalOverlayNetworkHierarchy logicaloverlayNetwork,
                                        SensorNetworkQueryPlan qep, Topology network,
                                        boolean allowDiscontinuousSensing)
  throws AgendaException, AgendaLengthException, OptimizationException, 
         SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
         SNEEException
  {
    super(logicaloverlayNetwork, qep, network, allowDiscontinuousSensing, false);

    numberOfRundundantCycles = Integer.parseInt(SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_REDUNDANTCYCLES));

    activeOverlay = new LogicalOverlayNetworkHierarchy(logicaloverlayNetwork, network, qep);
    this.logicaloverlayNetwork = null;
    logger.trace("Scheduling leaf fragments alpha=" + this.alpha + " bms beta=" + this.beta);
    scheduleLeafFragments(activeOverlay);
    logger.trace("Scheduling the non-leaf fragments");
    connectLogicalNodes(activeOverlay);
    scheduleNonLeafFragments(activeOverlay);
    logger.trace("Scheduled final sleep task");
    scheduleFinalSleepTask();
    
    long length = this.getLength_bms(UnreliableChannelAgendaReduced.INCLUDE_SLEEP);
    logger.trace("Agenda alpha=" + this.alpha + " beta=" + this.beta + 
    " alpha*beta = " + this.alpha * this.beta + " length="+length);
    
    if (length > (this.alpha * this.beta) && (!allowDiscontinuousSensing)) 
    {
    //display the invalid agenda, for debugging purposes
    //this.display(QueryCompiler.queryPlanOutputDir,
    //this.getName()+"-invalid");
    String msg = "Invalid agenda: alpha*beta = " + 
    bmsToMs(this.alpha * this.beta) + "ms, length = " + 
    bmsToMs(length) + "ms, alpha = "+bmsToMs(alpha) + "ms, beta = "
    + bmsToMs(beta);
    logger.warn(msg);
    throw new AgendaLengthException(msg);
  }
}
    
  /**
   * takes the logical overlay network and connects the output's of nodes to the correct parents
   * @param activeOverlay 
   */
  private void connectLogicalNodes(LogicalOverlayNetworkHierarchy activeOverlay)
  {
    Iterator<Site> siteIterator = this.iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
    Site root = this.iot.getRT().getRoot();
    while(siteIterator.hasNext())
    {
      Site rtSite = siteIterator.next();
      if(!rtSite.equals(root))
      {
        //deal with children links
        Site outputSite = (Site) rtSite.getOutput(0);
        Iterator<String> eqivNodesIdIterator = 
          activeOverlay.getActiveEquivilentNodes(rtSite.getID()).iterator();
        while(eqivNodesIdIterator.hasNext())
        {
          String equivSiteID = eqivNodesIdIterator.next();
          Site equivSite = this.iot.getSiteFromID(equivSiteID);
          equivSite.addOutput(outputSite);
        }
        //deal with equiv links
        eqivNodesIdIterator = 
          activeOverlay.getActiveEquivilentNodes(outputSite.getID()).iterator();
        while(eqivNodesIdIterator.hasNext())
        {
          String equivSiteID = eqivNodesIdIterator.next();
          Site equivSite = this.iot.getSiteFromID(equivSiteID);
          rtSite.addOutput(equivSite);
          Iterator<String> childEqivNodesIdIterator = 
            activeOverlay.getActiveEquivilentNodes(rtSite.getID()).iterator();
          while(childEqivNodesIdIterator.hasNext())
          {
            String childEquivSiteID = childEqivNodesIdIterator.next();
            Site childEquivSite = this.iot.getSiteFromID(childEquivSiteID);
            childEquivSite.addOutput(equivSite);
          }
        }
      }
    }
    
  }

  /**
   * Schedule the leaf fragments in a query plan.  These are executed bFactor times at the
   * acquisition frequency specified by the user  
   * @param activeOverlay 
   * @param plan
   * @param bFactor
   * @param agenda
   * @throws AgendaException
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   */
  private void scheduleLeafFragments(LogicalOverlayNetworkHierarchy activeOverlay)
  throws AgendaException, OptimizationException,
  SNEEException, SchemaMetadataException, SNEEConfigurationException 
  {
    //First schedule the leaf fragments, according to the buffering factor specified 
    //Note: a separate task needs to be scheduled for each execution of a leaf fragment
    //Note: a separate task need to be scheduled for each node within the logical node
    for (long bufferingIndex = 0; bufferingIndex < this.beta; bufferingIndex++) 
    {
      final long startTime = this.alpha * bufferingIndex;

      //For each leaf fragment
      HashSet<InstanceFragment> leafFrags = iot.getLeafInstanceFragments();
      final Iterator<InstanceFragment> fragIter = leafFrags.iterator();
      while (fragIter.hasNext()) 
      {
        final InstanceFragment frag = fragIter.next();
        //For each site the fragment is executing on 
        final Site node = frag.getSite();
        if(activeOverlay.isActive(node))
        {
          //schedule head site
          trySchedulingTask(node, startTime, frag, bufferingIndex);
        }
      }
      if ((bufferingIndex + 1) != this.beta) 
      {
        final long sleepStart = this.getLength_bms(AgendaIOT.INCLUDE_SLEEP);
        final long sleepEnd = (this.alpha * (bufferingIndex + 1));
        this.addSleepTask(sleepStart, sleepEnd, false);
      }
    }
  }
  
  /**
   * tries to schedule a task given a node, start time, the fragment, and 
   * the iteration of the buffering
   * @param node
   * @param startTime
   * @param frag
   * @param n
   * @throws OptimizationException
   * @throws SNEEException
   * @throws SchemaMetadataException
   * @throws SNEEConfigurationException
   * @throws AgendaException
   */
  private void trySchedulingTask(Site node, long startTime,
                                 InstanceFragment frag, long bufferingIndex)
  throws OptimizationException, SNEEException, 
         SchemaMetadataException, SNEEConfigurationException, AgendaException
  {
    try 
    {
      this.addFragmentTask(startTime, frag, node, (bufferingIndex + 1));
    } 
    catch (final AgendaException e) 
    {
      final long taskDuration = new InstanceFragmentTask(startTime,
      frag, node, (bufferingIndex + 1), this.alpha, this.beta, daf, costParams)
      .getTimeCost(daf);

      //If time to run task before the next acquisition time:
      if (this.getNextAvailableTime(node, AgendaIOT.INCLUDE_SLEEP)
          + taskDuration <= startTime + this.alpha) 
      {
        //TODO: change this to time synchronisation QoS
        this.addFragmentTask(frag, node, (bufferingIndex + 1));
      } 
      else 
      {
        throw new AgendaException(
          "Aquisition interval is smaller than duration of acquisition " +
          "fragments on node " + node.getID());
      }
    } 
  }

  /**
   * Schedule the non-leaf fragments.  Then are executed as soon as 
   * possible after the leaf fragments have finished executing.
   * @param activeOverlay 
   * @param plan
   * @param factor
   * @param agenda
   * @throws AgendaException
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  protected void scheduleNonLeafFragments(LogicalOverlayNetworkHierarchy activeOverlay)
  throws AgendaException, OptimizationException, SNEEConfigurationException,
         SchemaMetadataException, TypeMappingException, SNEEException
  {
    final Iterator<Site> siteIter = iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      //start with the head site
      final Site currentNode = siteIter.next();
      /*get the logical overlay to which this site is head of and time for each 
      node within the logical node*/
      ArrayList<String> physicalNodesForlogicalNode = 
        activeOverlay.getActiveEquivilentNodes(currentNode.getID());
      //schedule the sibling site
      scheduleSite(currentNode, physicalNodesForlogicalNode);
    }
  }
  
  /**
   * schedules all tasks related to a non leaf site
   * @param currentNode
   * @param physicalNodesForlogicalNode 
   */
  private void scheduleSite(Site currentNode, ArrayList<String> physicalNodesForlogicalNode)
  throws AgendaException, OptimizationException, SNEEConfigurationException,
         SchemaMetadataException, TypeMappingException, SNEEException
  {
    long nonLeafStart = Long.MAX_VALUE;
    final long startTime = this.getNextAvailableTime(currentNode, AgendaIOT.IGNORE_SLEEP);
    if (startTime < nonLeafStart) 
    {
      nonLeafStart = startTime;
    }
    scheduleSiteFragments(currentNode);
    scheduleOnwardTransmissions(currentNode, physicalNodesForlogicalNode);
  }

  /**
   * Schedules any onward transmissions given a site
   * @param currentNode
   * @param physicalNodesForlogicalNode 
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws AgendaException 
   */
  private void scheduleOnwardTransmissions(Site currentNode,
                                           ArrayList<String> physicalNodesForlogicalNode)
  throws AgendaException, OptimizationException, SchemaMetadataException, 
         TypeMappingException, SNEEException, SNEEConfigurationException
  {
  //Then Schedule any onward transmissions
    if (currentNode.getOutputs().length > 0) 
    {
      final HashSet<InstanceExchangePart> tuplesToSend = 
        new HashSet<InstanceExchangePart>();
      final Iterator<InstanceExchangePart> exchCompIter = 
        iot.getExchangeOperatorsThoughSiteMapping(currentNode).iterator();
      //  iot.getExchangeOperatorsThoughInputs(currentNode).iterator();
      /*locates any exchanges and creates a communication task between current node and
      parent node*/
      while (exchCompIter.hasNext()) 
      {
        final InstanceExchangePart exchComp = exchCompIter.next();
        if ((exchComp.getComponentType() == ExchangePartType.PRODUCER &&
             !exchComp.getNext().getSite().getID().equals(exchComp.getSite().getID()))
        || (exchComp.getComponentType() == ExchangePartType.RELAY && 
             !exchComp.getNext().getSite().getID().equals(exchComp.getSite().getID())))
        {
          tuplesToSend.add(exchComp);
        }
      }
      if (tuplesToSend.size() > 0) 
      {
        ArrayList<Site> destSites = new ArrayList<Site>();
        destSites.addAll(currentNode.getOutputsListInSiteForm());
          this.appendCommunicationTask(currentNode, destSites, tuplesToSend, physicalNodesForlogicalNode);
      }
    }
  }
  
  /**
   * Appends a communication task between a child node and its logical node parent
   *  in the sensor network logical overlay network
   * @param sourceNode        the node transmitting data
   * @param destNode          the node receiving data
   * @param tuplesToSend    the data being sent
   * @param physicalNodesForlogicalNode 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   */ 
  
  public void appendCommunicationTask(final Site sourceNode, ArrayList<Site> destNodes,
                                      final HashSet<InstanceExchangePart> tuplesToSend,
                                      ArrayList<String> physicalNodesForlogicalNode)
  throws AgendaException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, SNEEException, SNEEConfigurationException 
  {
    
    Long startTime = handleTranmissions(sourceNode, tuplesToSend, false);
    handleRecieves(sourceNode, tuplesToSend, startTime, false);
    handelAcks(destNodes, sourceNode, physicalNodesForlogicalNode, false);
    for(int cycle = 1; cycle <= numberOfRundundantCycles; cycle++)
    {
      handelRedundantTransmissions(destNodes,  sourceNode,  physicalNodesForlogicalNode, tuplesToSend, cycle);
      this.activeOverlay.updatePriority(sourceNode.getID());
    }
    logger.trace("Scheduled Communication task from node "
    + sourceNode.getID() + " to nodes " + destNodes.toString()
    + " at time " + startTime + "(size: "
    + tuplesToSend.size() + " exchange components )");
  }

  
  /**
   * handles extra transmissions which may 
   * @param destNodes
   * @param trueClusterHead
   * @param physicalNodesForlogicalNode
   * @param cycle 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   * @throws AgendaException 
   */
  private void handelRedundantTransmissions(ArrayList<Site> destNodes, Site trueClusterHead, 
                                            ArrayList<String> physicalNodesForlogicalNode,
                                            HashSet<InstanceExchangePart> tuplesToSend, int cycle)
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  AgendaException, SNEEException, SNEEConfigurationException
  {
    
    Long startTimeForRedundantTransmission = this.getLength_bms(true);
    Long timeToListenToAPacket = (long) Math.ceil(costParams.getSendPacket() * 1);
    //sorts out the transmission of the cluster head
    Long overhead = new Long(0);
    Long recieveTime = new Long(0);
    Site currentClusterHead = null;
    int currentPriority = 1;
    while(currentPriority <= this.activeOverlay.getActiveNodesInRankedOrder(trueClusterHead.getID()).size())
    {
      Iterator<String> priorityOrderedNodes = 
        this.activeOverlay.getActiveNodesInRankedOrder(trueClusterHead.getID()).iterator();
      while(priorityOrderedNodes.hasNext())
      {
        String SourceNodeID = priorityOrderedNodes.next();
        Site currentSourceNode = iot.getSiteFromID(SourceNodeID);
        //if current cluster head store for the recives
        if(this.activeOverlay.getPriority(SourceNodeID) == currentPriority)
        {
          currentClusterHead = currentSourceNode;
          //sort out transmission from this node
         Long fragmentOverhead = this.scheduleSiteFragments(currentSourceNode);
         //fix for the recieve 
         if(currentPriority == 1)
         {
           recieveTime += fragmentOverhead;
           overhead = overhead + fragmentOverhead;
         }
         else
           overhead = overhead + fragmentOverhead + timeToListenToAPacket;
         
         Site furthestDestNode = iot.getSiteFromID(this.locateMostExpensiveSite(destNodes, currentClusterHead).getID());
         CommunicationTask commTaskTx = 
           new CommunicationTask(startTimeForRedundantTransmission + overhead, currentClusterHead, furthestDestNode,
                                 CommunicationTask.TRANSMIT, tuplesToSend, this.alpha, this.beta, 
                                 daf, costParams, true, iot.getSiteFromID(trueClusterHead.getOutput(0).getID()));
         this.addTask(commTaskTx, currentClusterHead);
          
          //sort out receiving of siblings transmission
          Iterator<String> higherPriorityNodes = 
            this.activeOverlay.getNodesWithHigherPriority(
                this.activeOverlay.getClusterHeadFor(currentClusterHead.getID()), 
            this.activeOverlay.getPriority(SourceNodeID) + 1).iterator();
          
          while(higherPriorityNodes.hasNext())
          {
            Site node = this.iot.getSiteFromID(higherPriorityNodes.next());
            CommunicationTask commTaskRx =  
              new CommunicationTask(startTimeForRedundantTransmission + overhead, currentClusterHead, 
                                    node, CommunicationTask.RECEIVE, new Long(1), 
                                    costParams, true, currentSourceNode);
            this.addTask(commTaskRx, node);
          }
        }
      }
      currentPriority++;
    }
    Iterator<Site> destNodesIterator = destNodes.iterator();
    while(destNodesIterator.hasNext())
    {
      Site destNode = destNodesIterator.next();
      destNode = iot.getSiteFromID(destNode.getID());
      CommunicationTask commTaskRx = 
        new CommunicationTask(startTimeForRedundantTransmission+ recieveTime, trueClusterHead, destNode,
                              CommunicationTask.RECEIVE, tuplesToSend, this.alpha, this.beta, 
                              daf, costParams, overhead, true, trueClusterHead);
      this.addTask(commTaskRx, destNode);
    }
    if(cycle != this.numberOfRundundantCycles)
    {  //deal with acks for this set of tranmissions
      handelAcks(destNodes, trueClusterHead, physicalNodesForlogicalNode, true); 
    }
  }

  
  /**
   * handles acks transmissions and recieving
   * @param destNodes
   * @param sourceNode
   * @param physicalNodesForlogicalNode
   * @return
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   */
  private Long handelAcks(ArrayList<Site> destNodes, Site sourceNode,
                          ArrayList<String> physicalNodesForlogicalNode,
                          boolean redundant) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    //get source node (actually the dest ndoe of the original transmission)
    Site sourceSite = this.iot.getSiteFromID(sourceNode.getOutput(0).getID());
    //convert array string to array site
    //turns the child logical node into a list of destinations ites
    ArrayList<Site> destSites = new ArrayList<Site>();
    Iterator<String> physicalNodesForlogicalNodeIterator = physicalNodesForlogicalNode.iterator();
    while(physicalNodesForlogicalNodeIterator.hasNext())
    {
      destSites.add(iot.getSiteFromID(physicalNodesForlogicalNodeIterator.next()));
    }
      Site destSite = 
        this.iot.getSiteFromID(this.locateMostExpensiveSite(destSites, sourceSite).getID());
      Long startTimeForAck = this.getLength_bms(true);
      ///destSite = source site, and sourceNode the dest Site. 
      CommunicationTask commTaskAckT = 
        new CommunicationTask(startTimeForAck, destSite , sourceSite, CommunicationTask.ACKTRANSMIT,
                              this.alpha, this.beta, daf, 1, costParams, redundant, sourceNode);
      this.addTask(commTaskAckT, sourceSite);
      
      
      //sorts out the acks recieves for transmission
      CommunicationTask commTaskAckR = null;
      ArrayList<Site> logicalNodeSites = new ArrayList<Site>();
      Iterator<String> siblings = physicalNodesForlogicalNode.iterator();
      while(siblings.hasNext())
      {
        String inputID = siblings.next();
        Site inputSite = iot.getSiteFromID(inputID);
        logicalNodeSites.add(inputSite);
      }
      //handle child logical node
      Iterator<Site> logicalNodeIterator = logicalNodeSites.iterator();
      int lowestPrioirty = 1;
      while(logicalNodeIterator.hasNext())
      {
        Site node = logicalNodeIterator.next();
        if(this.activeOverlay.getPriority(node.getID()) >= lowestPrioirty)
        {
          commTaskAckR = new CommunicationTask(startTimeForAck, node,  sourceSite,
              CommunicationTask.ACKRECEIVE, this.alpha, this.beta, daf, 1, costParams, redundant,
              sourceNode);
          this.addTask(commTaskAckR, node);
        }
      }
      //handle current logical overlay
      Iterator<Site> currentLogicalNodeEquivNodesIterator = destNodes.iterator();
      while(currentLogicalNodeEquivNodesIterator.hasNext())
      {
        Site currentNode = currentLogicalNodeEquivNodesIterator.next();
        if(this.activeOverlay.isActive(currentNode) && !this.activeOverlay.isClusterHead(currentNode.getID()))
        {
          Site cNode = iot.getSiteFromID(currentNode.getID());
          commTaskAckR = new CommunicationTask(startTimeForAck, cNode, sourceSite,
              CommunicationTask.ACKRECEIVE, this.alpha, this.beta, daf, 1, costParams, redundant, sourceNode);
          this.addTask(commTaskAckR, cNode);
        }
      }
    return startTimeForAck;
  }

  /**
   * handles a recieve comm task
   * @param destNodes
   * @param sourceNode
   * @param tuplesToSend
   * @param startTime
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   */
  private void handleRecieves(Site sourceNode, HashSet<InstanceExchangePart> tuplesToSend,
                              Long startTime, boolean redundant)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    //sort out direct comms. 
    Site destNode = (Site) sourceNode.getOutput(0);
    destNode = iot.getSiteFromID(destNode.getID());
      final CommunicationTask commTaskRx = 
        new CommunicationTask(startTime, sourceNode, destNode,CommunicationTask.RECEIVE,
                              tuplesToSend, this.alpha, this.beta, daf, costParams, redundant, destNode);
      this.addTask(commTaskRx, destNode);
      
  }

  /**
   * handels a tranmission comm task
   * @param tuplesToSend 
   * @param sourceNode 
   * @param destNodes 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private long handleTranmissions(Site sourceNode, HashSet<InstanceExchangePart> tuplesToSend,
                                  boolean redundant)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    //sorts out the tranmission of the cluster head
    Site destNode = (Site) sourceNode.getOutput(0);
    destNode = iot.getSiteFromID(destNode.getID());
    final long startTime = this.getLength_bms(true);
    final CommunicationTask commTaskTx = 
      new CommunicationTask(startTime, sourceNode, destNode,CommunicationTask.TRANSMIT,
                            tuplesToSend, this.alpha, this.beta, daf, costParams, redundant, destNode);
    this.addTask(commTaskTx, sourceNode);
    if(redundant)
    {
      int priority = this.activeOverlay.getPriority(sourceNode.getID()) + 1;
      Iterator<String> extraNodes = this.activeOverlay.getNodesWithHigherPriority(sourceNode.getID(), priority).iterator();
      while(extraNodes.hasNext())
      {
        String nodeID = extraNodes.next();
        Site node = this.iot.getSiteFromID(nodeID);
        final CommunicationTask commTaskRx = 
          new CommunicationTask(startTime, sourceNode, node, CommunicationTask.RECEIVE,
                                this.alpha, this.beta, daf, 1, costParams, true, sourceNode);
        this.addTask(commTaskRx, node);
      }
    }
    return startTime;
  }

  /**
   * given a set of destination sites, and a source site, find the one with the most expensive radio
   * cost
   * @param destNodes
   * @param sourceNode
   * @return
   */
  private Site locateMostExpensiveSite(ArrayList<Site> destNodes, Site sourceNode)
  {
    Iterator<Site> destSites = destNodes.iterator();
    Site mostExpensiveSite = destSites.next();
    while(destSites.hasNext())
    {
      Site destSite = destSites.next();
      if(network.getLinkEnergyCost(sourceNode, destSite) >
         network.getLinkEnergyCost(sourceNode, mostExpensiveSite))
        mostExpensiveSite = destSite;
    }
    return mostExpensiveSite;
  }

  /**
   * Schedule all fragment which have been allocated to execute on this node,
   * ensuring the precedence conditions are met
   * @param currentNode
   * @return 
   * @throws SNEEConfigurationException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   * @throws OptimizationException 
   * @throws AgendaException 
   */
  private Long scheduleSiteFragments(Site currentNode) 
  throws AgendaException, OptimizationException, SNEEException, 
         SchemaMetadataException, SNEEConfigurationException
  {
    final Iterator<InstanceFragment> fragIter = 
      iot.instanceFragmentIterator(TraversalOrder.POST_ORDER);
    Long overhead = new Long(0);
    while (fragIter.hasNext()) 
    {
      final InstanceFragment frag = fragIter.next();
      if (iot.hasSiteGotInstFrag(currentNode, frag) && (!frag.isLeaf())) 
      {
        overhead = overhead + this.addFragmentTask(frag, currentNode);
      }
    }
    return overhead;
  }
  
  public LogicalOverlayNetwork getLogicalOverlayNetwork()
  {
    return this.activeOverlay;
  }
  
  /**
   * adds a sleep task for every node within the Logical Overlay network
   */
  public void addSleepTask(final long sleepStart, final long sleepEnd,
                           final boolean lastInAgenda) 
  throws AgendaException 
  {
    if (sleepStart < 0) 
    {
      throw new AgendaException("Start time < 0");
    }
    final Iterator<Site> siteIter = this.iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      final Site site = siteIter.next();
      this.assertConsistentStartTime(sleepStart, site);
      SleepTask t = new SleepTask(sleepStart, sleepEnd, site,
        lastInAgenda, costParams);
      this.addTask(t, site);
      /*get the logical overlay to which this site is head of and time for each 
      node within the logical node*/
      ArrayList<String> physicalNodesForlogicalNode = 
        this.activeOverlay.getEquivilentNodes(site.getID());
      Iterator<String> physicalNodesForlogicalNodeIterator = 
        physicalNodesForlogicalNode.iterator();
      while(physicalNodesForlogicalNodeIterator.hasNext())
      {
        String physicalNodeID = physicalNodesForlogicalNodeIterator.next();
        Site physicalSite = (Site) iot.getSiteFromID(physicalNodeID);
        this.assertConsistentStartTime(sleepStart, physicalSite);
        t = new SleepTask(sleepStart, sleepEnd, physicalSite,
          lastInAgenda, costParams);
        this.addTask(t, physicalSite);
      }
      
    }
  }
  
  public LogicalOverlayNetworkHierarchy getActiveLogicalOverlay()
  {
    return this.activeOverlay;
  }
}
