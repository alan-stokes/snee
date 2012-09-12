/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/

package uk.ac.manchester.cs.snee.compiler.iot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.FragmentTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.SNEEAlgebraicForm;
import uk.ac.manchester.cs.snee.compiler.queryplan.SleepTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;


/**
 * Class responsible for recording the schedules of nodes in a sensor network.
 * @author 	Alan Stokes
 *
 */
public class AgendaIOT extends SNEEAlgebraicForm
{

    /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -271011672606150076L;

  /**
   * Logger for this class.
   */
  private static final Logger logger = Logger.getLogger(AgendaIOT.class.getName());

  /**
   * The acquisition interval, in binary ms.  The finest granularity that 
   * may be given is 1/32ms (=32 bms)
   */
  protected long alpha;

  /**
   * The buffering factor.
   */
  protected long beta;

  protected boolean allowDiscontinuousSensing = true;
  
  /**
   * The task schedule for all the sites. 
   */
  //the list of tasks for each sensor network node
  protected HashMap<Site, ArrayList<Task>> tasks = 
  	new HashMap<Site, ArrayList<Task>>();

  //the list of all start times (used to display schedule)
  protected ArrayList<Long> startTimes = new ArrayList<Long>();

  public static final boolean IGNORE_SLEEP = true;

  public static final boolean INCLUDE_SLEEP = false;

  protected String name;

  protected CostParameters costParams;
  
  /**
   * Counter to assign unique id to different candidates.
   */
  protected static int candidateCount = 0;
  
  /**
   * both structures that contain the deployment of operators on nodes (
   * due to other downstream systems requiring different structures)
   */
  protected DAF daf;
  protected IOT iot;
  /**
   * Start of nonLeaf part of the agenda.
   * This will be where the last leaf jumps to at the end of the query duration 
   */
  protected long nonLeafStart = Integer.MAX_VALUE;
  
  /**
   * constructor used by children of this class (reliable channel agenda)
   * (only executes if last boolean set to true)
   * @param acquisitionInterval
   * @param bfactor
   * @param iot
   * @param costParams
   * @param queryName
   * @param allowDiscontinuousSensing
   * @param doExecution
   * @throws AgendaException
   * @throws AgendaLengthException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws SNEEException
   * @throws SNEEConfigurationException
   */
  public AgendaIOT(final long acquisitionInterval, final long bfactor,
	                 final IOT iot, CostParameters costParams, 
	                 final String queryName, boolean allowDiscontinuousSensing,
	                 boolean doExecution) 
  throws AgendaException, AgendaLengthException, OptimizationException, 
    SchemaMetadataException, TypeMappingException, SNEEException, 
    SNEEConfigurationException 
  {
    	super(queryName);
		this.alpha = msToBms_RoundUp(acquisitionInterval);
		this.beta = bfactor;
		this.iot = iot;
		this.allowDiscontinuousSensing=allowDiscontinuousSensing;
		this.daf = iot.getDAF();
		this.tasks.clear();
		if (!queryName.equals("")) {
			this.name = generateID(queryName);
		}
		this.costParams=costParams;
		if(doExecution)
		{
  		logger.trace("Scheduling leaf fragments alpha=" + this.alpha + " bms beta=" + this.beta);
  		scheduleLeafFragments();
  		logger.trace("Scheduling the non-leaf fragments");
  		scheduleNonLeafFragments();
  		logger.trace("Scheduling network management section");
  		logger.trace("Scheduled final sleep task");
  		scheduleFinalSleepTask();
  		
  		long length = this.getLength_bms(AgendaIOT.INCLUDE_SLEEP);
  		logger.trace("Agenda alpha=" + this.alpha + " beta=" + this.beta + " alpha*beta = " + this.alpha * this.beta + " length="+length);
  		
  		if (length > (this.alpha * this.beta) && (!allowDiscontinuousSensing)) 
  		{
   			//display the invalid agenda, for debugging purposes
  // 			this.display(QueryCompiler.queryPlanOutputDir,
  //					this.getName()+"-invalid");
   			String msg = "Invalid agenda: alpha*beta = " + 
   				bmsToMs(this.alpha * this.beta) + "ms, length = " + 
   				bmsToMs(length) + "ms, alpha = "+bmsToMs(alpha) + "ms, beta = "
   				+ bmsToMs(beta);
   			logger.warn(msg);
  			throw new AgendaLengthException(msg);
  			
  		}
    }
  }
    
	/**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }
    
	/**
	 * Generates a systematic name for this query plan strucuture, of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
    protected String generateID(final String queryName) {
    	candidateCount++;
    	return queryName + "-Agenda-" + candidateCount;
    }
    
	 /** {@inheritDoc} */
	public String getDescendantsString() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getDescendantsString()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getDescendantsString()"); 
		return this.getID()+"-"+this.iot.getDescendantsString();
	}
    
    
    public IOT getIOT() {
    	return this.iot;
    }
    
    public String getName() {
    	return this.name;
    }
    
    /**
     * Returns acquisition interval, in bms
     * @return the acquisition interval, in bms
     */
    public final long getAcquisitionInterval_bms() {
	return this.alpha;
    }

    /**
     * Returns acquisition interval, in ms
     * @return the acquisition interval, in ms
     */
	public long getAcquisitionInterval_ms() {
		return AgendaIOT.bmsToMs(this.alpha);
	}

    /**
     * Returns the buffering factor for this agenda
     * @return the buffering factor
     */
    public final long getBufferingFactor() {
	return this.beta;
    }

    /**
     * Returns the processing time for this agenda, in bms
     * @return the processing time, in bms
     */
	public long getProcessingTime_bms() {
		return this.getDeliveryTime_bms() - this.alpha * (this.beta -1);
	}
	
    /**
     * Returns the processing time for this agenda, in ms
     * @return the processing time, in ms
     */
	public long getProcessingTime_ms() {
		return AgendaIOT.bmsToMs(this.getDeliveryTime_bms() - this.alpha * (this.beta -1));
	}
	
    /**
     * Returns the delivery time for this agenda, in bms
     * @return the delivery time, in bms
     */
	public long getDeliveryTime_bms() {
	
		return this.getLength_bms(true);
	}

    /**
     * Returns the delivery time for this agenda, in ms
     * @return the delivery time, in ms
     */
	public long getDeliveryTime_ms() {
		
		return AgendaIOT.bmsToMs(this.getLength_bms(true));
	}
	
    public static int bmsToMs(final long startTime) {
	return Utils.divideAndRoundUp(startTime * 1000, 1024);
    }

    public static int msToBms_RoundUp(final long ms) {
	final long temp = ms * 1024;
	return Utils.divideAndRoundUp(temp, 1000);
    }

    /**
     * Returns the list of start times, in order, for each task on every node
     * @return
     */
    public final ArrayList<Long> getStartTimes() {
	return this.startTimes;
    }

    public final long getNonLeafStart() {
	return this.nonLeafStart;
    }

    public final void setNonLeafStart(final long nonLeafStart2) {
	this.nonLeafStart = nonLeafStart2;
    }

    /**
     * Given a time and a sensor network node, raises an exception if the given time is before the node's
     * next available time.
     * @param startTime
     * @param site
     * @throws AgendaException
     */
    protected void assertConsistentStartTime(final double startTime,
	    final Site site) throws AgendaException {

	if (startTime < this.getNextAvailableTime(site, INCLUDE_SLEEP)) {
	    throw new AgendaException("Attempt to Schedule task on node "
		    + site.getID() + " at time " + startTime
		    + " which is before the nodes next available time ("
		    + this.getNextAvailableTime(site, INCLUDE_SLEEP) + ")");
	}

    }

    /**
     * Given a node, returns true if it has been assigned any tasks.
     * @param node
     * @return
     */
    public final boolean hasTasks(final Site node) {
	return (this.tasks.get(node) != null);
    }

    //adds a start time to the list of start times
    private void addStartTime(final long startTime) {

	boolean found = false;
	int i = 0;

	final Iterator<Long> startTimesIter = this.startTimes.iterator();
	while (startTimesIter.hasNext() && !found) {
	    final Long s = startTimesIter.next();
	    if (s.intValue() == startTime) {
		found = true;
	    }

	    if (s.intValue() > startTime) {
		break;
	    }

	    i++;
	}

	if (found == false) {
	    this.startTimes.add(i, new Long(startTime));
	    found = true;
	}

    }


//    //adds a task to a nodes schedule, performing the necessary checks
    protected void addTask(final Task t, final Site site) {
	
		//add node to schedule if necessary
		if (this.tasks.get(site) == null) {
		    this.tasks.put(site, new ArrayList<Task>());
		}
	
		//add task to the node schedule
		final ArrayList<Task> taskList = this.tasks.get(site);
		taskList.add(t);
	
		//add to list of start times
		this.addStartTime(t.getStartTime());
    }

    public final Task getTask(final long startTime, final Site site) {

	if (!this.tasks.keySet().contains(site)) {
	    return null;
	}
	final Iterator<Task> taskListIter = this.tasks.get(site).iterator();
	while (taskListIter.hasNext()) {
	    final Task t = taskListIter.next();
	    if (t.getStartTime() == startTime) {
		return t;
	    }
	}
	return null;
    }

    public final FragmentTask getFirstFragmentTask(final Site site,
	    final Fragment frag) {
	final Iterator<Task> taskIter = this.taskIterator(site);
	while (taskIter.hasNext()) {
	    final Task t = taskIter.next();
	    if (t instanceof FragmentTask) {
		final FragmentTask fragTask = (FragmentTask) t;
		if (fragTask.getFragment() == frag) {
		    return fragTask;
		}
	    }
	}
	return null;
    }

    public final CommunicationTask getFirstCommTask(final Site site, final int mode) {
	final Iterator<Task> taskIter = this.taskIterator(site);
	while (taskIter.hasNext()) {
	    final Task t = taskIter.next();
	    if (t instanceof CommunicationTask) {
		final CommunicationTask commTask = (CommunicationTask) t;
		if ((commTask.getSourceNode() == site)
			&& (commTask.getMode() == mode)) {
		    return commTask;
		}
	    }
	}
	return null;
    }
    
    

    /**
     * Adds a fragment task at the specified startTime on the specified node
     * @param startTime		the time the task should start
     * @param frag			the query plan fragment to be executed
     * @param node			the sensor network node
     * @param occurrence	the nth evaluation of a leaf fragment
     * @throws AgendaException
     * @throws OptimizationException 
     * @throws SNEEConfigurationException 
     * @throws SchemaMetadataException 
     * @throws SNEEException 
     */
    public final Long addFragmentTask(final long startTime, final InstanceFragment frag,
	    final Site node, final long occurrence) 
    throws AgendaException, OptimizationException, SNEEException, SchemaMetadataException, 
    SNEEConfigurationException 
    {

    	this.assertConsistentStartTime(startTime, node);
    	final InstanceFragmentTask fragTask = new InstanceFragmentTask(startTime, frag, node,
    		occurrence, this.alpha, this.beta, daf, costParams);
    	this.addTask(fragTask, node);
    
    	if(frag == null || node == null)
    	  System.out.println("");
    	
    	logger.trace("Scheduled Fragment " + frag.getID() + " on node "
    		+ node.getID() + " at time " + startTime);
    	return fragTask.getDuration();
    }

    public final void addFragmentTask(final int startTime, final InstanceFragment frag,
	    final Site node) throws AgendaException, OptimizationException, SNEEException, SchemaMetadataException, SNEEConfigurationException {

	this.addFragmentTask(startTime, frag, node, 1);
    }

    /**
     * Adds a fragment task at the next available time on the specified node
     * @param frag		the query plan fragment to be executed
     * @param node			the sensor network node 
     * @throws AgendaException	
     * @throws OptimizationException 
     * @throws SNEEConfigurationException 
     * @throws SchemaMetadataException 
     * @throws SNEEException 
     */
    public final Long addFragmentTask(final InstanceFragment frag, final Site node)
	    throws AgendaException, OptimizationException, SNEEException, SchemaMetadataException, SNEEConfigurationException {

	   final long startTime = this.getNextAvailableTime(node, INCLUDE_SLEEP);
	  logger.trace("start time =" + startTime);
	  return this.addFragmentTask(startTime, frag, node, 1);
    }

    public final void addFragmentTask(final InstanceFragment fragment, final Site node,
	    final long ocurrence) throws AgendaException, OptimizationException, SNEEException, SchemaMetadataException, SNEEConfigurationException {
	final long startTime = this.getNextAvailableTime(node, INCLUDE_SLEEP);
	this.addFragmentTask(startTime, fragment, node, ocurrence);
    }

    /**
     * Appends a communication task between two nodes in the sensor network
     * @param sourceNode				the node transmitting data
     * @param destNode					the node receiving data
     * @param tuplesToSend		the data being sent
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     * @throws SNEEConfigurationException 
     * @throws SNEEException 
     */ 
    
    public void appendCommunicationTask(final Site sourceNode,
	    final Site destNode,
	    final HashSet<InstanceExchangePart> tuplesToSend)
	    throws AgendaException, OptimizationException, SchemaMetadataException, TypeMappingException, SNEEException, SNEEConfigurationException {

    final long startTime = this.getLength_bms(true);
	final CommunicationTask commTaskTx = new CommunicationTask(startTime,
		sourceNode, destNode,CommunicationTask.TRANSMIT,
		tuplesToSend, this.alpha, this.beta, daf, costParams, false, destNode);
	final CommunicationTask commTaskRx = new CommunicationTask(startTime,
		sourceNode, destNode,CommunicationTask.RECEIVE,
		tuplesToSend, this.alpha, this.beta, daf, costParams, false, destNode);

	this.addTask(commTaskTx, sourceNode);
	this.addTask(commTaskRx, destNode);

	logger.trace("Scheduled Communication task from node "
		+ sourceNode.getID() + " to node " + destNode.getID()
		+ " at time " + startTime + "(size: "
		+ tuplesToSend.size() + " exchange components )");
    }

    public void addSleepTask(final long sleepStart, final long sleepEnd,
	    final boolean lastInAgenda) throws AgendaException {
	if (sleepStart < 0) {
	    throw new AgendaException("Start time < 0");
	}
	final Iterator<Site> siteIter = this.iot.getRT()
		.siteIterator(TraversalOrder.POST_ORDER);
	while (siteIter.hasNext()) {
	    final Site site = siteIter.next();
	    this.assertConsistentStartTime(sleepStart, site);
	    final SleepTask t = new SleepTask(sleepStart, sleepEnd, site,
		    lastInAgenda, costParams);
	    this.addTask(t, site);
	}
    }

    /**
     * @param node		node in the sensor network
     * @return 			the time the node has completed all the tasks it has been allocated so far
     */
    public final long getNextAvailableTime(final Site node, final boolean ignoreSleep) {
	if (this.tasks.get(node) == null) {
	    return 0;
	} else {
	    //find the last task and return its end time
	    final ArrayList<Task> taskList = this.tasks.get(node);
	    int taskNum = taskList.size() - 1;
	    Task last = taskList.get(taskNum);
	    if (ignoreSleep) {
		while (last.isSleepTask()) {
		    taskNum = taskNum - 1;
		    if (taskNum < 0) {
			return 0;
		    } else {
			last = taskList.get(taskNum);
		    }
		}
	    }
	    long nextAvailableTime = last.getEndTime();
	    //			if (nextAvailableTime % 10 != 0) { //TODO: unhardcode this
	    //				nextAvailableTime = (nextAvailableTime / 10 + 1)* 10;
	    //			}

	    return nextAvailableTime;
	}

    }

    /**
     * @return if ignoreLastSleep is true, returns the time that the last task on all nodes ends.  
     * Otherwise returns the length of the agenda. 
     * 
     */
    public final long getLength_bms(final boolean ignoreLastSleep) {
	long tmp = 0;

	final Iterator<Site> nodeIter = this.tasks.keySet().iterator();
	while (nodeIter.hasNext()) {
	    final Site n = nodeIter.next();
	    if (this.getNextAvailableTime(n, ignoreLastSleep) > tmp) {
		tmp = this.getNextAvailableTime(n, ignoreLastSleep);
	    }
	}

	return tmp;
    }


    /**
     * Returns an iterator which gives the all the sites in the schedule
     * @return
     */
    public final Iterator<Site> siteIterator() {
	return this.tasks.keySet().iterator();
    }

    /**
     * Returns an iterator which gives the tasks, in order, 
     * for the given node or null if no correct node is given
     * @param node
     * @return
     */
    public final Iterator<Task> taskIterator(final Site node) 
    {
	    if(this.tasks.get(node) == null)
	      return null;
	    else
        return this.tasks.get(node).iterator();
    }

    //TODO: review thie taskschedule data structure in view of this inefficient access algorithm
    /**
     * Returns an iterator which gives the tasks at a given start time
     * @param node
     * @return
     */
    public final Iterator<Task> taskIterator(final long startTime) {
	final ArrayList<Task> results = new ArrayList<Task>();

	final Iterator<Site> nodeIter = this.tasks.keySet().iterator();
	while (nodeIter.hasNext()) {
	    final Site node = nodeIter.next();

	    final Iterator<Task> taskIter = this.tasks.get(node).iterator();
	    while (taskIter.hasNext()) {
		final Task t = taskIter.next();

		if (t.getStartTime() == startTime) {
		    results.add(t);
		}
	    }
	}

	return results.iterator();
    }

    /**
     * Returns an iterator which gives all the start times
     * @return
     */
    public final Iterator<Long> startTimeIterator() {
	return this.startTimes.iterator();
    }

    public final long getFragmentTimerOffsetVal(final String fragID,
	    final String siteID) {
	final Site site = (Site) this.iot.getRT().getSite(siteID);
	final Iterator<Task> taskIter = this.taskIterator(site);
	while (taskIter.hasNext()) {
	    final Task task = taskIter.next();
	    if (task instanceof FragmentTask) {
		final FragmentTask fragTask = (FragmentTask) task;
		final Fragment frag = fragTask.getFragment();
		if (frag.getID().equals(fragID)
			&& (fragTask.getOccurrence() == 1)) {

		    return fragTask.getStartTime();
		}
	    }
	}

	return -1;
    }

    public final long getFragmentTimerRepeatVal(final String fragID) {
	final InstanceFragment frag = this.iot.getInstanceFragment(fragID);

	if (frag.isLeaf()) {
	    return this.getAcquisitionInterval_bms();
	}

	return this.getAcquisitionInterval_bms() * this.beta;
    }

    public final long getCommTaskTimerOffsetVal(final String sourceID,
	    final String destID, final int mode) {
	Site site;

	if (mode == CommunicationTask.RECEIVE) {
	    site = (Site) this.iot.getRT().getSite(destID);
	} else {
	    site = (Site) this.iot.getRT().getSite(sourceID);
	}

	final Iterator<Task> taskIter = this.taskIterator(site);
	while (taskIter.hasNext()) {
	    final Task task = taskIter.next();
	    if (task instanceof CommunicationTask) {
		final CommunicationTask commTask = (CommunicationTask) task;
		if (commTask.getSourceID().equals(sourceID)
			&& commTask.getDestID().equals(destID)) {
		    return commTask.getStartTime();
		}
	    }
	}

	return -1;
    }

    public final long getCommTaskTimerRepeatVal() {
	return this.alpha * this.beta;
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
	    throws AgendaException, OptimizationException, SNEEException, SchemaMetadataException, SNEEConfigurationException {

	//First schedule the leaf fragments, according to the buffering factor specified 
	//Note: a separate task needs to be scheduled for each execution of a leaf fragment
	for (long n = 0; n < this.beta; n++) {
	    final long startTime = this.alpha * n;

	    //For each leaf fragment
	    HashSet<InstanceFragment> leafFrags = iot.getLeafInstanceFragments();
	    final Iterator<InstanceFragment> fragIter = leafFrags.iterator();
	    while (fragIter.hasNext()) {
		final InstanceFragment frag = fragIter.next();

		//For each site the fragment is executing on 
		  final Site node = frag.getSite();

		    try {

			this.addFragmentTask(startTime, frag, node, (n + 1));

		    } catch (final AgendaException e) {

			final long taskDuration = new InstanceFragmentTask(startTime,
				frag, node, (n + 1), this.alpha, this.beta, daf, costParams)
				.getTimeCost(daf);

			//If time to run task before the next acquisition time:
			if (this.getNextAvailableTime(node,
				AgendaIOT.INCLUDE_SLEEP)
				+ taskDuration <= startTime
				+ this.alpha) {
			    //TODO: change this to time synchronisation QoS
			    this.addFragmentTask(frag, node, (n + 1));
			} else {
			    throw new AgendaException(
				    "Aquisition interval is smaller than duration of acquisition fragments on node "
					    + node.getID());
			}
		    }
	    }
	    //Go active uses the disactivated sleep to represent all nodes do nothing
	    //if ((Settings.NESC_DO_SNOOZE) && ((n+1) != bFactor)) {
	    if ((n + 1) != this.beta) {
		final long sleepStart = this
			.getLength_bms(AgendaIOT.INCLUDE_SLEEP);
		final long sleepEnd = (this.alpha * (n + 1));
		this.addSleepTask(sleepStart, sleepEnd, false);
	    }
	}
    }

    /**
     * Schedule the non-leaf fragments.  Then are executed as soon as possible after the leaf fragments
     * have finished executing.
     * @param plan
     * @param factor
     * @param agenda
     * @throws AgendaException
     * @throws OptimizationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws SNEEConfigurationException 
     * @throws SNEEException 
     */
  private void scheduleNonLeafFragments()
	throws AgendaException, OptimizationException, SchemaMetadataException, TypeMappingException, SNEEException, SNEEConfigurationException 
	{

  	long nonLeafStart = Long.MAX_VALUE;
  
  	final Iterator<Site> siteIter = iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
  	while (siteIter.hasNext()) 
  	{
  	  final Site currentNode = siteIter.next();
  
  	  final long startTime = this.getNextAvailableTime(currentNode, AgendaIOT.IGNORE_SLEEP);
  	  if (startTime < nonLeafStart) 
  	  {
  		  nonLeafStart = startTime;
  	  }
  
  	  //Schedule all fragment which have been allocated to execute on this node,
  	  //ensuring the precedence conditions are met
  	  final Iterator<InstanceFragment> fragIter = iot.instanceFragmentIterator(TraversalOrder.POST_ORDER);
  	  while (fragIter.hasNext()) 
  	  {
  		  final InstanceFragment frag = fragIter.next();
  		  if (iot.hasSiteGotInstFrag(currentNode, frag) && (!frag.isLeaf())) 
  		  {
  		    this.addFragmentTask(frag, currentNode);
  		  }
  	  }
  
      //Then Schedule any onward transmissions
      if (currentNode.getOutputs().length > 0) 
      {
    		final HashSet<InstanceExchangePart> tuplesToSend = new HashSet<InstanceExchangePart>();
    		final Iterator<InstanceExchangePart> exchCompIter = iot.getExchangeOperatorsThoughInputs(currentNode).iterator();
    		//TODO fix to use instance exchange parts.
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
  }

	protected void scheduleFinalSleepTask() throws AgendaException {
		final long sleepStart = this.getLength_bms(AgendaIOT.INCLUDE_SLEEP);
		
		//A sleep task of at least 10 ms needs to be added here, to turn the radio off
		final long sleepEnd = Math.max(this.alpha * this.beta, 
				new Float(sleepStart + costParams.getTurnOffRadio()).longValue());
		logger.trace("Sleep task scheduled from "+sleepStart+" to "+sleepEnd);
		this.addSleepTask(sleepStart, sleepEnd, true);
		this.setNonLeafStart(nonLeafStart);
	}

	public CostParameters getCostParameters() {
		return this.costParams;
	}    
	
	public void removeNodeFromAgenda(int siteID)
	{
	  Site toBeRemove = iot.getRT().getSite(siteID);
	  tasks.remove(toBeRemove);
	}

	/**
	 * takes a set of children sites, and orders them by start transmission times. 
	 * (assumes 1 transmission task per site. (tree structure)
	 * @param children
	 */
  public void orderNodesByTransmissionTasks(ArrayList<Node> children)
  {
    Iterator<Node> childIterator = children.iterator();
    ArrayList<CommunicationTask> transmissionTasks = new ArrayList<CommunicationTask>();
    while(childIterator.hasNext())
    {
      Node child = childIterator.next();
      CommunicationTask transmissionTask = getTransmissionTask(child);
      transmissionTasks.add(transmissionTask);
    }
    Collections.sort(transmissionTasks);
    children.clear();
    Iterator<CommunicationTask> transmissionTaskIterator = transmissionTasks.iterator();
    while(transmissionTaskIterator.hasNext())
    {
      CommunicationTask task = transmissionTaskIterator.next();
      Site node = task.getSourceNode();
      children.add(node);
    }
  }

  /**
   * gets the transmission task for the child node
   * @param child node to get task for.
   * @return first communication task found during task iterator which is a transmission task.
   */
  public CommunicationTask getTransmissionTask(Node child)
  {
    final Iterator<Task> taskIter = this.taskIterator((Site) child);
    if(taskIter != null)
    {
      while (taskIter.hasNext()) 
      {
        final Task t = taskIter.next();
        if (t instanceof CommunicationTask) 
        {
          final CommunicationTask commTask = (CommunicationTask) t;
          if ((commTask.getSourceNode() == child)
             && (commTask.getMode() == CommunicationTask.TRANSMIT)) 
          {
             return commTask;
          }
        }
      }
    }
    return null;
  }

  /**
   * gets last communication task temporally which is a receive.
   * @param nodePrime node to find last receive task on.
   * @return communication task
   */
  public CommunicationTask getLastCommunicationTask(Node nodePrime)
  {
    final Iterator<Site> siteIterator = this.siteIterator();
    boolean inQEP = false;
    //check site is within QEP, if not, thenr eturn null. (as no tasks).
    //needs to be done here, as task iterator will fail with a node with no tasks.
    while(siteIterator.hasNext() && !inQEP)
    {
      Site currentSite = siteIterator.next();
      if(currentSite.getID().equals(nodePrime.getID()))
      {
        inQEP = true;
      }
    }
    
    if(!inQEP)
      return null;
      
    final Iterator<Task> taskIter = this.taskIterator((Site) nodePrime);
    
    CommunicationTask currentLastReceiveTask = null;
    while (taskIter.hasNext()) 
    {
      final Task t = taskIter.next();
      if (t instanceof CommunicationTask) 
      {
        final CommunicationTask commTask = (CommunicationTask) t;
        if ((commTask.getSourceNode() == nodePrime)
           && (commTask.getMode() == CommunicationTask.RECEIVE)) 
        {
          currentLastReceiveTask = commTask;
        }
      }
    }
    return currentLastReceiveTask;
  }
  
  /**
   * returns the communication task between child and parent
   * @param child transmitter of communication task
   * @param parent receiver of communication task
   * @return communication task.
   */
  public CommunicationTask getCommunicationTaskBetween(Node child, Node parent)
  {
    final Iterator<Task> taskIter = this.taskIterator((Site)child);
    while(taskIter.hasNext())
    {
      Task task = taskIter.next();
      if (task instanceof CommunicationTask) 
      {
        final CommunicationTask commTask = (CommunicationTask) task;
        if ((commTask.getSourceNode().getID().equals(child.getID())) && 
            (commTask.getDestNode().getID().equals(parent.getID())))
        {
          return commTask;
        }
      }
    }
    return null;
  }
  
  /**
   * removes the communication task from the tasks hash map.
   * @param taskToBeRemoved 
   */
  public int removeCommunicationTask(CommunicationTask taskToBeRemoved)
  {
    //remove tx
    ArrayList<Task>  tasksOfNode = tasks.remove(taskToBeRemoved.getSourceNode());
    Iterator<Task> taskIterator = tasksOfNode.iterator();
    boolean removedtx = false;
    int index = -1;
    while(taskIterator.hasNext() && !removedtx)
    {
      Task task = taskIterator.next();
      index ++;
      if(task instanceof CommunicationTask) 
      {
        final CommunicationTask commTask = (CommunicationTask) task;
        if ((commTask.getSourceNode() == taskToBeRemoved.getSourceNode()) && 
            (commTask.getDestNode() == taskToBeRemoved.getDestNode()))
        {
          taskIterator.remove();
          removedtx = true;
        }
      }
    }
    tasks.put(taskToBeRemoved.getSourceNode(), tasksOfNode);
    
    return index;
    
  }
  
  /**
   * Appends a communication task between two nodes in the sensor network at a given time
   * @param sourceNode        the node transmitting data
   * @param destNode          the node receiving data
   * @param time              The start time of this communication task
   * @param exchangeComponents    the data being sent
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   */ 
  
  public final void appendCommunicationTask(final Site sourceNode,
    final Site destNode, final long time,
    final HashSet<InstanceExchangePart> exchangeComponents, int childIndex)
    throws AgendaException, OptimizationException, SchemaMetadataException, TypeMappingException, SNEEException, SNEEConfigurationException 
  {

    final long startTime = time;

    final CommunicationTask commTaskTx = new CommunicationTask(startTime,
    sourceNode, destNode, CommunicationTask.TRANSMIT, 
    exchangeComponents, this.alpha, this.beta, daf, costParams, false, destNode);
    final CommunicationTask commTaskRx = new CommunicationTask(startTime,
    sourceNode, destNode, CommunicationTask.RECEIVE, 
    exchangeComponents, this.alpha, this.beta, daf, costParams, false, destNode);
  
    this.addTask(commTaskTx, sourceNode, childIndex);
    this.addTask(commTaskRx, destNode);
  
    logger.trace("Scheduled Communication task from node "
    + sourceNode.getID() + " to node " + destNode.getID()
    + " at time " + startTime + "(size: "
    + exchangeComponents.size() + " exchange components )");
  }
  
  /**
   * adds a task at a specfic index in the arraylist
   * @param t the task to be added
   * @param site the site the task is being added to
   * @param index the index in the list.
   */
  private void addTask(final Task t, final Site site, final int index) 
  {
    //add node to schedule if necessary
    if (this.tasks.get(site) == null) 
    {
        this.tasks.put(site, new ArrayList<Task>());
    }
  
    //add task to the node schedule
    final ArrayList<Task> taskList = this.tasks.get(site);
    taskList.add(index, t);
  
    //add to list of start times
    this.addStartTime(t.getStartTime());
  }

  /**
   * locates all sites which have transmission tasks after startTime
   * @param startTime
   * @return
   */
  public ArrayList<Node> sitesWithTransmissionTasksAfterTime(long startTime)
  {
    ArrayList<Node> sites = new ArrayList<Node> ();
    ArrayList<Long> startTimes = this.getStartTimes();
    ArrayList<Long> interestedStartTimes = new ArrayList<Long>();
    Iterator<Long> startTimesIterator = startTimes.iterator();
    while(startTimesIterator.hasNext())
    {
      Long curremtStartTime = startTimesIterator.next();
      if(curremtStartTime > startTime && startTimesIterator.hasNext())
        interestedStartTimes.add(curremtStartTime);
    }
    Iterator<Site> siteIter = this.iot.siteIterator(TraversalOrder.POST_ORDER);
    while(siteIter.hasNext())
    {
      Site site = siteIter.next();
      Iterator<Long> interestedTimesIterator = interestedStartTimes.iterator();
      while(interestedTimesIterator.hasNext())
      {
        Task hopefulTask = this.getTask(interestedTimesIterator.next(), site);
        if(hopefulTask != null && !sites.contains(site))
          sites.add(site);
      }
    }
    return sites;
  }

  /**
   * returns tasks, for the site energy model
   * @return
   */
  public HashMap<Site, ArrayList<Task>> getTasks()
  {
    return tasks;
  }

  public Site getSiteByID(Site site)
  {
    Iterator<Site> siteIterator = this.tasks.keySet().iterator();
    while(siteIterator.hasNext())
    {
      Site agendaSite = siteIterator.next();
      if(agendaSite.getID().equals(site.getID()))
        return agendaSite;
    }
    return null;
  }
  
  public Site getSiteByID(String siteID)
  {
    Iterator<Site> siteIterator = this.tasks.keySet().iterator();
    while(siteIterator.hasNext())
    {
      Site agendaSite = siteIterator.next();
      if(agendaSite.getID().equals(siteID))
        return agendaSite;
    }
    return null;
  }
}
