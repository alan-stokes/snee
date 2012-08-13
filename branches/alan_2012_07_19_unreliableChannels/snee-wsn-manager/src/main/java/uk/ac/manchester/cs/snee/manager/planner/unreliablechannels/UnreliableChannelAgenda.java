package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
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
  private static final long serialVersionUID = -4391275568035871005L;

  /**
   * Logger for this class.
   */
  private final static Logger logger =
    Logger.getLogger(UnreliableChannelAgenda.class.getName());
  /**
   *  contains the logical overlay network used to abstract over the phsyical ndoes.
   */
  private LogicalOverlayNetwork logicaloverlayNetwork;
  private Topology network;
  
  public UnreliableChannelAgenda(LogicalOverlayNetwork logicaloverlayNetwork,
                                 SensorNetworkQueryPlan qep, Topology network,
                                 boolean allowDiscontinuousSensing)
  throws AgendaException, AgendaLengthException, OptimizationException, 
         SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
         SNEEException
  {
    super(qep.getAcquisitionInterval_bms(), qep.getBufferingFactor(), qep.getIOT(),
          qep.getQueryName(), allowDiscontinuousSensing);
    this.network = network;
    this.costParams = qep.getCostParameters();
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    this.tasks.clear();
    candidateCount = 0;
    logger.trace("Scheduling leaf fragments alpha=" + this.alpha + " bms beta=" + this.beta);
    scheduleLeafFragments();
    logger.trace("Scheduling the non-leaf fragments");
    connectLogicalNodes();
    scheduleNonLeafFragments();
    logger.trace("Scheduled final sleep task");
    scheduleFinalSleepTask();
    
    long length = this.getLength_bms(UnreliableChannelAgenda.INCLUDE_SLEEP);
    logger.trace("Agenda alpha=" + this.alpha + " beta=" + this.beta + " alpha*beta = " + this.alpha * this.beta + " length="+length);
    
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
   */
  private void connectLogicalNodes()
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
          this.logicaloverlayNetwork.getEquivilentNodes(rtSite.getID()).iterator();
        while(eqivNodesIdIterator.hasNext())
        {
          String equivSiteID = eqivNodesIdIterator.next();
          Site equivSite = this.iot.getSiteFromID(equivSiteID);
          equivSite.addOutput(outputSite);
        }
        //deal with equiv links
        eqivNodesIdIterator = 
          this.logicaloverlayNetwork.getEquivilentNodes(outputSite.getID()).iterator();
        while(eqivNodesIdIterator.hasNext())
        {
          String equivSiteID = eqivNodesIdIterator.next();
          Site equivSite = this.iot.getSiteFromID(equivSiteID);
          rtSite.addOutput(equivSite);
          Iterator<String> childEqivNodesIdIterator = 
            this.logicaloverlayNetwork.getEquivilentNodes(rtSite.getID()).iterator();
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
   * @param plan
   * @param bFactor
   * @param agenda
   * @throws AgendaException
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   */
  private void scheduleLeafFragments()
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
        //schedule head site
        trySchedulingTask(node, startTime, frag, bufferingIndex);
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
   * @param plan
   * @param factor
   * @param agenda
   * @throws AgendaException
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  protected void scheduleNonLeafFragments()
  throws AgendaException, OptimizationException, SNEEConfigurationException,
         SchemaMetadataException, TypeMappingException, SNEEException
  {
    final Iterator<Site> siteIter = iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      //schedule head site
      final Site currentNode = siteIter.next();
      scheduleSite(currentNode);
      /*get the logical overlay to which this site is head of and time for each 
      node within the logical node*/
      ArrayList<String> physicalNodesForlogicalNode = 
        logicaloverlayNetwork.getEquivilentNodes(currentNode.getID());
      Iterator<String> physicalNodesForlogicalNodeIterator = 
        physicalNodesForlogicalNode.iterator();
      while(physicalNodesForlogicalNodeIterator.hasNext())
      {
        String physicalNodeID = physicalNodesForlogicalNodeIterator.next();
        Site physicalSite = (Site) iot.getSiteFromID(physicalNodeID);
        scheduleSite(physicalSite);
      }
    }
  }
  
  /**
   * schedules all tasks related to a non leaf site
   * @param currentNode
   */
  private void scheduleSite(Site currentNode)
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
    scheduleOnwardTransmissions(currentNode);
  }

  /**
   * Schedules any onward transmissions given a site
   * @param currentNode
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws AgendaException 
   */
  private void scheduleOnwardTransmissions(Site currentNode)
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
          this.appendCommunicationTask(currentNode, destSites, tuplesToSend);
      }
    }
  }
  
  /**
   * Appends a communication task between a child node and its logical node parent
   *  in the sensor network logical overlay network
   * @param sourceNode        the node transmitting data
   * @param destNode          the node receiving data
   * @param tuplesToSend    the data being sent
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   */ 
  
  public void appendCommunicationTask(final Site sourceNode, ArrayList<Site> destNodes,
                                      final HashSet<InstanceExchangePart> tuplesToSend)
  throws AgendaException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, SNEEException, SNEEConfigurationException 
  {
    Site mostExpensiveSite = locateMostExpensiveSite(destNodes, sourceNode);
    final long startTime = this.getLength_bms(true);
    final CommunicationTask commTaskTx = 
      new CommunicationTask(startTime, sourceNode, mostExpensiveSite,CommunicationTask.TRANSMIT,
                            tuplesToSend, this.alpha, this.beta, daf, costParams);
    this.addTask(commTaskTx, sourceNode);
    
    Iterator<Site> destSites = destNodes.iterator();
    while(destSites.hasNext())
    {
      Site destSite = destSites.next();
      final CommunicationTask commTaskRx = 
        new CommunicationTask(startTime, sourceNode, destSite,CommunicationTask.RECEIVE,
                              tuplesToSend, this.alpha, this.beta, daf, costParams);
      this.addTask(commTaskRx, destSite);
    }

    logger.trace("Scheduled Communication task from node "
    + sourceNode.getID() + " to nodes " + destNodes.toString()
    + " at time " + startTime + "(size: "
    + tuplesToSend.size() + " exchange components )");
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
   * @throws SNEEConfigurationException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   * @throws OptimizationException 
   * @throws AgendaException 
   */
  private void scheduleSiteFragments(Site currentNode) 
  throws AgendaException, OptimizationException, SNEEException, 
         SchemaMetadataException, SNEEConfigurationException
  {
    final Iterator<InstanceFragment> fragIter = 
      iot.instanceFragmentIterator(TraversalOrder.POST_ORDER);
    while (fragIter.hasNext()) 
    {
      final InstanceFragment frag = fragIter.next();
      if (iot.hasSiteGotInstFrag(currentNode, frag) && (!frag.isLeaf())) 
      {
        this.addFragmentTask(frag, currentNode);
      }
    }
  }
  
  public LogicalOverlayNetwork getLogicalOverlayNetwork()
  {
    return this.logicaloverlayNetwork;
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
        logicaloverlayNetwork.getEquivilentNodes(site.getID());
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
}
