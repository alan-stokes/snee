package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels;

import java.util.ArrayList;
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
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

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
  
  public UnreliableChannelAgenda(LogicalOverlayNetwork logicaloverlayNetwork,
                                 SensorNetworkQueryPlan qep, 
                                 boolean allowDiscontinuousSensing)
  throws AgendaException, AgendaLengthException, OptimizationException, 
         SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
         SNEEException
  {
    super(qep.getAcquisitionInterval_bms(), qep.getBufferingFactor(), qep.getIOT(),
          qep.getCostParameters(), qep.getQueryName(), allowDiscontinuousSensing);
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    this.tasks.clear();
    candidateCount = 0;
    logger.trace("Scheduling leaf fragments alpha=" + this.alpha + " bms beta=" + this.beta);
    scheduleLeafFragments();
    logger.trace("Scheduling the non-leaf fragments");
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
        /*get the logical overlay to which this site is head of and time for each 
        node within the logical node*/
        ArrayList<String> physicalNodesForlogicalNode = 
          logicaloverlayNetwork.getEquivilentNodes(node.getID());
        Iterator<String> physicalNodesForlogicalNodeIterator = 
          physicalNodesForlogicalNode.iterator();
        while(physicalNodesForlogicalNodeIterator.hasNext())
        {
          String physicalNodeID = physicalNodesForlogicalNodeIterator.next();
          Site physicalSite = (Site) iot.getNode(physicalNodeID);
          trySchedulingTask(physicalSite, startTime, frag, bufferingIndex);
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
        Site physicalSite = (Site) iot.getNode(physicalNodeID);
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
        iot.getExchangeOperators(currentNode).iterator();
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
          this.appendCommunicationTask(currentNode, (Site) currentNode
            .getOutput(0), tuplesToSend);
      }
    }
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
}
