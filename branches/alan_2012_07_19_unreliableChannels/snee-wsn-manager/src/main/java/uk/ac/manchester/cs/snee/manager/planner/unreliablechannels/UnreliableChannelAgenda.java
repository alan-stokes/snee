package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragment;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragmentTask;
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SleepTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
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



public class UnreliableChannelAgenda extends AgendaIOT
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 8020437449548315506L;
  /**
   * Logger for this class.
   */
  private final static Logger logger =
    Logger.getLogger(UnreliableChannelAgenda.class.getName());
  
  private LogicalOverlayNetworkHierarchy activeOverlay;
  private int numberOfRundundantCycles =0;
  private Topology network = null;
  
  public UnreliableChannelAgenda(LogicalOverlayNetwork logicaloverlayNetwork,
                                 SensorNetworkQueryPlan qep, Topology network,
                                 boolean allowDiscontinuousSensing)
  throws AgendaException, AgendaLengthException, OptimizationException, 
         SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
         SNEEException
  {
    super(qep.getAcquisitionInterval_bms(), qep.getBufferingFactor(), qep.getIOT(),
        qep.getCostParameters(), qep.getQueryName(), allowDiscontinuousSensing, false);

    this.network = network;
    this.costParams = qep.getCostParameters();
    this.tasks.clear();

    numberOfRundundantCycles = 
      Integer.parseInt(SNEEProperties.getSetting(
          SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_REDUNDANTCYCLES));
    
    activeOverlay = new LogicalOverlayNetworkHierarchy(logicaloverlayNetwork.getClusters(), network, qep);
    logger.trace("Scheduling leaf fragments alpha=" + this.alpha + " bms beta=" + this.beta);
    scheduleLeafFragments(activeOverlay);
    logger.trace("Scheduling the non-leaf fragments");
    connectLogicalNodes(activeOverlay);
    scheduleNonLeafFragments(activeOverlay);
    logger.trace("Scheduled final sleep task");
    scheduleFinalSleepTask();
    
    long length = this.getLength_bms(UnreliableChannelAgenda.INCLUDE_SLEEP);
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
  
  public UnreliableChannelAgenda(LogicalOverlayNetworkHierarchy logicaloverlayNetwork,
                                        SensorNetworkQueryPlan qep, Topology network,
                                        boolean allowDiscontinuousSensing)
  throws AgendaException, AgendaLengthException, OptimizationException, 
         SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
         SNEEException
  {
    super(qep.getAcquisitionInterval_bms(), qep.getBufferingFactor(), qep.getIOT(),
        qep.getCostParameters(), qep.getQueryName(), allowDiscontinuousSensing, false);

    this.network = network;
    this.costParams = qep.getCostParameters();
    this.tasks.clear();

    numberOfRundundantCycles = 
      Integer.parseInt(SNEEProperties.getSetting(
          SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_REDUNDANTCYCLES));

    activeOverlay = new LogicalOverlayNetworkHierarchy(logicaloverlayNetwork, network, qep);
    activeOverlay.resetPriorities();
    logger.trace("Scheduling leaf fragments alpha=" + this.alpha + " bms beta=" + this.beta);
    scheduleLeafFragments(activeOverlay);
    logger.trace("Scheduling the non-leaf fragments");
    connectLogicalNodes(activeOverlay);
    scheduleNonLeafFragments(activeOverlay);
    logger.trace("Scheduled final sleep task");
    scheduleFinalSleepTask();
    
    long length = this.getLength_bms(UnreliableChannelAgenda.INCLUDE_SLEEP);
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
      rtSite = this.iot.getSiteFromID(rtSite.getID());
      if(!rtSite.getID().equals(root.getID()))
      {
        if(rtSite.getOutputsList().size() == 0)
        {
          Iterator<Node> routingTreeIterator = this.iot.getRT().getSiteTree().getNodes().iterator();
          while(routingTreeIterator.hasNext())
          {
            Node rtNode = routingTreeIterator.next();
            if(rtNode.getInputsList().contains(rtSite))
              rtSite.addOutput(rtNode);
          }
          System.out.println();
        }
        //deal with children links
        Site outputSite = (Site) rtSite.getOutput(0);
        Iterator<String> eqivNodesIdIterator = 
          activeOverlay.getActiveEquivilentNodes(rtSite.getID()).iterator();
        while(eqivNodesIdIterator.hasNext())
        {
          String equivSiteID = eqivNodesIdIterator.next();
          Site equivSite = this.iot.getSiteFromID(equivSiteID);
          if(!doesContainOutput(equivSite, outputSite))
             equivSite.addOutput(outputSite);
        }
        //deal with equiv links
        eqivNodesIdIterator = 
          activeOverlay.getActiveEquivilentNodes(outputSite.getID()).iterator();
        while(eqivNodesIdIterator.hasNext())
        {
          String equivSiteID = eqivNodesIdIterator.next();
          Site equivSite = this.iot.getSiteFromID(equivSiteID);
          if(!doesContainOutput(rtSite, equivSite))
            rtSite.addOutput(equivSite);
          Iterator<String> childEqivNodesIdIterator = 
            activeOverlay.getActiveEquivilentNodes(rtSite.getID()).iterator();
          while(childEqivNodesIdIterator.hasNext())
          {
            String childEquivSiteID = childEqivNodesIdIterator.next();
            Site childEquivSite = this.iot.getSiteFromID(childEquivSiteID);
            if(!doesContainOutput(childEquivSite, equivSite))
              childEquivSite.addOutput(equivSite);
          }
        }
      }
    }
    
  }

  private boolean doesContainOutput(Site rtSite, Site equivSite)
  {
    Iterator<Node> outputSites = rtSite.getOutputsList().iterator();
    while(outputSites.hasNext())
    {
      Node node = outputSites.next();
      if(node.getID().equals(equivSite.getID()))
        return true;
    }
    return false;
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
        Site node = frag.getSite();
        node = iot.getSiteFromID(node.getID());
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
   * @return if ignoreLastSleep is true, returns the time that the last task on all nodes ends.  
   * Otherwise returns the length of the agenda. 
   * 
   */
  public final long getLength_bms(final boolean ignoreLastSleep) 
  {
    long tmp = 0;

    final Iterator<Site> nodeIter = this.tasks.keySet().iterator();
    while (nodeIter.hasNext()) 
    {
      final Site n = nodeIter.next();
      if (this.getNextAvailableTime(n, ignoreLastSleep) > tmp) 
      {
        tmp = this.getNextAvailableTime(n, ignoreLastSleep);
      }
    }
    return tmp; 
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
      Site currentNode = siteIter.next();
      currentNode = iot.getSiteFromID(currentNode.getID());
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
    handelAcks(sourceNode, physicalNodesForlogicalNode, false);
    Site destNode = (Site) sourceNode.getOutput(0);
    destNode = iot.getSiteFromID(destNode.getID());
    for(int cycle = 1; cycle <= numberOfRundundantCycles; cycle++)
    {
      handelRedundantTransmissions(destNode,  sourceNode,  physicalNodesForlogicalNode, tuplesToSend, cycle);
      this.activeOverlay.updatePriority(sourceNode.getID());
    }
    logger.trace("Scheduled Communication task from node "
    + sourceNode.getID() + " to nodes " + destNodes.toString()
    + " at time " + startTime + "(size: "
    + tuplesToSend.size() + " exchange components )");
  }

  
  /**
   * handles redundant transmissions which may occur if edge failure happens
   * @param destNode
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
  private void handelRedundantTransmissions(Site destNode, Site trueClusterHead, 
                                            ArrayList<String> physicalNodesForlogicalNode,
                                            HashSet<InstanceExchangePart> tuplesToSend, int cycle)
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  AgendaException, SNEEException, SNEEConfigurationException
  {
    Long startTimeForRedundantTransmission = this.getLength_bms(true);
    
    //sorts out the redundant transmission of the cluster priorityNode
    Long overhead = new Long(0);
    
    Iterator<String> nodesInPriorityOrder = 
      this.activeOverlay.getActiveNodesInRankedOrder(trueClusterHead.getID()).iterator();
    ArrayList<String> nodesInActiveCluster = new ArrayList<String>();
    nodesInActiveCluster.addAll(this.activeOverlay.getActiveEquivilentNodes(trueClusterHead.getID()));
    
    while(nodesInPriorityOrder.hasNext())
    {
      String nodeID = nodesInPriorityOrder.next();
      Site priorityNode = iot.getSiteFromID(nodeID);
      nodesInActiveCluster.remove(nodeID);
      //sort out transmission **********************
      CommunicationTask commTaskTx = 
        new CommunicationTask(startTimeForRedundantTransmission, priorityNode, destNode,
                              CommunicationTask.TRANSMIT, tuplesToSend, this.alpha, this.beta, 
                              daf, costParams, true, iot.getSiteFromID(trueClusterHead.getOutput(0).getID()),
                              iot.getSiteFromID(trueClusterHead.getOutput(0).getID()), true);
      this.addTask(commTaskTx, priorityNode);
      //sort out recieves for current cluster
      Iterator<String> otherActiveNodes = nodesInActiveCluster.iterator();
      while(otherActiveNodes.hasNext())
      {
        String activeNodeID = otherActiveNodes.next();
        Site activeSite = iot.getSiteFromID(activeNodeID);
        CommunicationTask commTaskRx = 
          new CommunicationTask(startTimeForRedundantTransmission + overhead, priorityNode, destNode,
                                CommunicationTask.RECEIVE,tuplesToSend, this.alpha, this.beta, daf,  
                                costParams, true, iot.getSiteFromID(trueClusterHead.getOutput(0).getID()),
                                activeSite, true);
        this.addTask(commTaskRx, activeSite);
      }
      CommunicationTask commTaskRx = 
        new CommunicationTask(startTimeForRedundantTransmission + overhead, priorityNode, destNode,
                              CommunicationTask.RECEIVE,tuplesToSend, this.alpha, this.beta, daf,  
                              costParams, true, iot.getSiteFromID(trueClusterHead.getOutput(0).getID()),
                              iot.getSiteFromID(trueClusterHead.getOutput(0).getID()), true);
      this.addTask(commTaskRx, destNode);
      
      
      
      if(nodesInActiveCluster.size() != 0)
      {
        handelAcks(priorityNode, nodesInActiveCluster, true); 
        startTimeForRedundantTransmission = this.getLength_bms(true);
      }
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
  private Long handelAcks(Site sourceNode,
                          ArrayList<String> physicalNodesForlogicalNode,
                          boolean redundant) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    //get source node (actually the dest ndoe of the original transmission)
    Site sourceSite = this.iot.getSiteFromID(sourceNode.getOutput(0).getID());
    Long startTimeForAck = this.getLength_bms(true);
    
    //sort out child logical ndoe collection
    ArrayList<Site> logicalNodeSites = new ArrayList<Site>();
    Iterator<String> siblings = physicalNodesForlogicalNode.iterator();
    while(siblings.hasNext())
    {
      String inputID = siblings.next();
      Site inputSite = iot.getSiteFromID(inputID);
      logicalNodeSites.add(inputSite);
    }
    
    sourceNode = this.locateMostExpensiveSite(logicalNodeSites, sourceSite);
    
    ///destSite = source site, and sourceNode the dest Site. 
    CommunicationTask commTaskAckT = 
      new CommunicationTask(startTimeForAck,  sourceNode, sourceSite, CommunicationTask.ACKTRANSMIT,
                            this.alpha, this.beta, daf, 1, costParams, redundant, sourceSite,
                            sourceSite, false, true, true);
    this.addTask(commTaskAckT, sourceSite);
    
    
    //sorts out the acks recieves for transmission
    CommunicationTask commTaskAckR = null;
    //handle child logical node
    Iterator<Site> logicalNodeIterator = logicalNodeSites.iterator();
    int lowestPrioirty = 1;
    while(logicalNodeIterator.hasNext())
    {
      Site node = logicalNodeIterator.next();
      if(this.activeOverlay.getPriority(node.getID()) >= lowestPrioirty)
      {
        commTaskAckR = new CommunicationTask(startTimeForAck, node, sourceSite, 
            CommunicationTask.ACKRECEIVE, this.alpha, this.beta, daf, 1, costParams, redundant,
            sourceNode, sourceNode, false, true, true);
        this.addTask(commTaskAckR, node);
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
                              tuplesToSend, this.alpha, this.beta, daf, costParams, redundant, 
                              destNode, destNode, true);
      this.addTask(commTaskRx, destNode);
      
  }

  /**
   * handles a transmission task
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
                            tuplesToSend, this.alpha, this.beta, daf, costParams, 
                            redundant, destNode, destNode, false, false, true);
    this.addTask(commTaskTx, sourceNode);
    if(redundant)
    {
      int priority = this.activeOverlay.getPriority(sourceNode.getID()) + 1;
      Iterator<String> extraNodes = 
        this.activeOverlay.getNodesWithHigherPriority(sourceNode.getID(), priority).iterator();
      while(extraNodes.hasNext())
      {
        String nodeID = extraNodes.next();
        Site node = this.iot.getSiteFromID(nodeID);
        final CommunicationTask commTaskRx = 
          new CommunicationTask(startTime, sourceNode, node, CommunicationTask.RECEIVE,
                                this.alpha, this.beta, daf, 1, costParams, true, sourceNode,
                                node, false, false, true);
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
      Site site = siteIter.next();
      site = this.iot.getSiteFromID(site.getID());
      this.assertConsistentStartTime(sleepStart, site);
      SleepTask t = new SleepTask(sleepStart, sleepEnd, site,
        lastInAgenda, costParams);
      this.addTask(t, site);
      /*get the logical overlay to which this site is head of and time for each 
      node within the logical node*/
      ArrayList<String> physicalNodesForlogicalNode = 
        this.activeOverlay.getEquivilentNodes(site.getID());
      physicalNodesForlogicalNode.remove(site.getID());
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
  
  protected final long getMaxTuplesTranmitted(DAF daf, HashSet<InstanceExchangePart> exchangeComponents) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException 
  {
    long result = 0;
    final Iterator<InstanceExchangePart> exchCompIter = exchangeComponents.iterator();
    while (exchCompIter.hasNext()) 
    {
      final InstanceExchangePart exchComp = exchCompIter.next();
      if ((   exchComp.getComponentType() == ExchangePartType.PRODUCER)
          || (exchComp.getComponentType() == ExchangePartType.RELAY)) 
      {
        result += exchComp.getmaxPackets(daf, beta, costParams, true);
      }
    }
    return result;
  }
  
  public Iterator<Task> taskIteratorOrderedByTime()
  {
    ArrayList<Task> orderedTasks = new  ArrayList<Task>();
    ArrayList<Long> startTimes = this.getStartTimes();
    Collections.sort(startTimes);
    Iterator<Long> siteTimeIterator = startTimes.iterator();
    while(siteTimeIterator.hasNext())
    {
      Long startTime = siteTimeIterator.next();
      Iterator<Task> taskIterator = this.taskIterator(startTime);
      boolean comms = false;
      //determine if the tasks in this start time are communication tasks
      while(taskIterator.hasNext())
      {
        Task task = taskIterator.next();
        if(task instanceof CommunicationTask)
          comms = true;
      }
      //if tasks are comm tasks, then locate transmission task first.
      if(comms)
      {
        //locate transmission
        taskIterator = this.taskIterator(startTime);
        while(taskIterator.hasNext())
        {
          Task task = taskIterator.next();
          if(task instanceof InstanceFragmentTask)
            System.out.println();
          CommunicationTask cTask = (CommunicationTask) task;
          if(cTask.getMode() == CommunicationTask.TRANSMIT ||
             cTask.getMode() == CommunicationTask.ACKTRANSMIT )
          {
            orderedTasks.add(task);
          }
        }
        //store all receives (no particular order)
        taskIterator = this.taskIterator(startTime);
        while(taskIterator.hasNext())
        {
          Task task = taskIterator.next();
          CommunicationTask cTask = (CommunicationTask) task;
          if(cTask.getMode() == CommunicationTask.ACKRECEIVE ||
             cTask.getMode() == CommunicationTask.RECEIVE )
          {
            orderedTasks.add(task);
          }
        }
      }
      else //not comms, just add them in any order
      {
        taskIterator = this.taskIterator(startTime);
        while(taskIterator.hasNext())
        {
          Task task = taskIterator.next();
          orderedTasks.add(task);
        }
      }
    }
    return orderedTasks.iterator();
  }

}
